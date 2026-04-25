package blurry

import (
	"context"
	"errors"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var log = logging.Logger("gossip")

const gossipTopic = "/gossip/1.0.0"

type GossipPubSuber interface {
	PublishRaw(topicName string, data []byte) error
	SubscribeRaw(topicName string, h func([]byte) error) error
}

type GossipError string

func (e GossipError) Error() string {
	return string(e)
}

type GossipBroadcaster struct {
	ctx context.Context

	gossip   GossipPubSuber
	topic    string
	dataChan chan []byte

	once sync.Once
	mx   sync.Mutex
}

// NewGossipBroadcaster creates a new Gossip-based broadcaster for CRDT
func NewGossipBroadcaster(ctx context.Context, gossip GossipPubSuber) (*GossipBroadcaster, error) {
	gb := &GossipBroadcaster{
		gossip:   gossip,
		topic:    gossipTopic,
		dataChan: make(chan []byte, 100),
		ctx:      ctx,
		once:     sync.Once{},
		mx:       sync.Mutex{},
	}
	err := gossip.SubscribeRaw(gossipTopic, func(data []byte) error {
		gb.Receive(data)
		return nil
	})
	return gb, err
}

// Broadcast sends data via Gossip
func (gb *GossipBroadcaster) Broadcast(_ context.Context, data []byte) error {
	gb.mx.Lock()
	defer gb.mx.Unlock()

	return gb.gossip.PublishRaw(gb.topic, data)
}

// Next receives broadcasted data
func (gb *GossipBroadcaster) Next(ctx context.Context) ([]byte, error) {
	select {
	case data := <-gb.dataChan:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gb.ctx.Done():
		gb.close()
		return nil, gb.ctx.Err()
	}
}

// Receive is called by Gossip subscription handler to deliver data
func (gb *GossipBroadcaster) Receive(data []byte) {
	gb.mx.Lock()
	defer gb.mx.Unlock()

	select {
	case gb.dataChan <- data:
	case <-gb.ctx.Done():
		gb.close()
		return
	default:
		<-gb.dataChan
		gb.dataChan <- data
	}
}

func (gb *GossipBroadcaster) close() {
	if gb == nil {
		return
	}
	gb.once.Do(func() {
		close(gb.dataChan)
	})
}

const (
	ErrPubsubNotInit     GossipError = "gossip: service not initialized"
	ErrAlreadyRunning    GossipError = "gossip: pubsub is already running"
	ErrListenerMalformed GossipError = "gossip: pubsub listener not initialized properly"
	ErrPubsubEmptyTopic  GossipError = "gossip: topic name is empty"
)

type topicHandler func(data []byte) error

type Gossip struct {
	ctx    context.Context
	pubsub *pubsub.PubSub
	node   host.Host

	mx               *sync.RWMutex
	subs             []*pubsub.Subscription
	relayCancelFuncs map[string]pubsub.RelayCancelFunc
	topics           map[string]*pubsub.Topic
	handlersMap      map[string]topicHandler
	isRunning        *atomic.Bool
}

type TopicHandler struct {
	TopicName string
	Handler   topicHandler
}

func NewGossip(ctx context.Context) *Gossip {
	return &Gossip{
		ctx:              ctx,
		mx:               new(sync.RWMutex),
		subs:             []*pubsub.Subscription{},
		topics:           map[string]*pubsub.Topic{},
		relayCancelFuncs: map[string]pubsub.RelayCancelFunc{},
		isRunning:        new(atomic.Bool),
	}
}

func (g *Gossip) Run(node host.Host) (err error) {
	if g.isRunning.Load() {
		return ErrAlreadyRunning
	}

	g.node = node

	if err := g.runGossip(); err != nil {
		return fmt.Errorf("gossip: failed to run: %w", err)
	}

	handlers := make([]TopicHandler, 0, len(g.handlersMap))
	for name, h := range g.handlersMap {
		handlers = append(handlers, TopicHandler{
			TopicName: name,
			Handler:   h,
		})
	}

	for _, th := range handlers {
		if err := g.SubscribeRaw(th.TopicName, th.Handler); err != nil {
			return fmt.Errorf("gossip: presubscribe: %w", err)
		}
	}

	go func() {
		if err := g.runListener(); err != nil {
			log.Errorf("gossip: listener: %v", err)
			return
		}
		log.Infoln("gossip: listener stopped")
	}()

	return nil
}

func (g *Gossip) runListener() error {
	if g == nil {
		return ErrListenerMalformed
	}
	for {
		if !g.isRunning.Load() {
			return nil
		}

		if err := g.ctx.Err(); err != nil {
			return err
		}

		g.mx.RLock()
		subs := make([]*pubsub.Subscription, len(g.subs))
		copy(subs, g.subs)
		g.mx.RUnlock()

		for _, sub := range subs { // TODO scale this!
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)

			msg, err := sub.Next(ctx)
			cancel()
			if errors.Is(err, pubsub.ErrSubscriptionCancelled) {
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}
			if err != nil {
				log.Errorf("gossip: failed to listen subscription to topic: %v", err)
				continue
			}
			if msg == nil || msg.Topic == nil {
				continue
			}

			log.Debugf("gossip: received message: %s", string(msg.Data))

			g.mx.RLock()
			handlerF, ok := g.handlersMap[strings.TrimSpace(*msg.Topic)]
			g.mx.RUnlock()
			if !ok || handlerF == nil {
				continue
			}
			if err := handlerF(msg.Data); err != nil {
				log.Errorf(
					"gossip: failed to handle peer %s message from topic %s: %v",
					msg.ReceivedFrom.String(), *msg.Topic, err,
				)
				continue
			}
		}
	}
}

func (g *Gossip) runGossip() (err error) {
	if g == nil || g.node == nil {
		return GossipError("gossip: service not initialized properly")
	}

	g.pubsub, err = pubsub.NewGossipSub(g.ctx, g.node)
	if err != nil {
		return err
	}
	g.isRunning.Store(true)

	log.Infoln("gossip: started")

	return
}

func (g *Gossip) SubscribeRaw(topicName string, h func([]byte) error) (err error) {
	if g == nil || !g.isRunning.Load() {
		return ErrPubsubNotInit
	}
	g.mx.Lock()
	defer g.mx.Unlock()

	if topicName == "" {
		return ErrPubsubEmptyTopic
	}

	topic, ok := g.topics[topicName]
	if !ok {
		topic, err = g.pubsub.Join(topicName)
		if err != nil {
			return err
		}
		g.topics[topicName] = topic
	}

	relayCancel, err := topic.Relay()
	if err != nil {
		return err
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}

	log.Infof("gossip: subscribed to topic: %s", topicName)

	g.relayCancelFuncs[topicName] = relayCancel
	g.subs = append(g.subs, sub)
	g.handlersMap[topicName] = h

	return nil
}

func (g *Gossip) PublishRaw(topicName string, data []byte) (err error) {
	if g == nil || !g.isRunning.Load() {
		return ErrPubsubNotInit
	}

	g.mx.Lock()
	defer g.mx.Unlock()

	topic, ok := g.topics[topicName]
	if !ok {
		topic, err = g.pubsub.Join(topicName)
		if err != nil {
			return err
		}
		g.topics[topicName] = topic
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	err = topic.Publish(ctx, data)
	cancel()
	if err != nil && !errors.Is(err, pubsub.ErrTopicClosed) {
		log.Errorf("gossip: failed to publish owner update message: %v", err)
		return err
	}
	return nil
}

func (g *Gossip) Close() (err error) {
	if !g.isRunning.Load() {
		return nil
	}

	g.mx.Lock()
	defer g.mx.Unlock()

	for t := range g.relayCancelFuncs {
		g.relayCancelFuncs[t]()
	}

	for _, sub := range g.subs {
		sub.Cancel()
	}

	for _, topic := range g.topics {
		_ = topic.Close()
	}

	g.isRunning.Store(false)

	g.pubsub = nil
	g.relayCancelFuncs = nil
	g.topics = nil
	g.subs = nil
	log.Infoln("gossip: closed")
	return err
}
