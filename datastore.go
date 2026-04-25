package blurry

import (
	ds "github.com/ipfs/go-datastore"
	badgerDS "github.com/ipfs/go-ds-badger4"
)

type Options = badgerDS.Options

var DefaultOptions = &badgerDS.DefaultOptions

func NewDatastore(path string, opts *Options) (ds.Datastore, error) {
	return badgerDS.NewDatastore(path, opts)
}
