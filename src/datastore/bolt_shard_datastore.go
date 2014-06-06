package datastore

import (
	"cluster"
	"configuration"
	"protocol"

	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type BoltShardDatastore struct {
	baseDir     string
	config      *configuration.Configuration
	writeBuffer *cluster.WriteBuffer
	shards      map[uint32]*BoltShard
	lock        *sync.Mutex
}

func NewBoltShardDatastore(config *configuration.Configuration) (*BoltShardDatastore, error) {
	baseDir := filepath.Join(config.DataDir, "shard_data")
	err := os.MkdirAll(baseDir, 0744)
	if err != nil {
		return nil, err
	}

	return &BoltShardDatastore{
		baseDir: baseDir,
		config:  config,
		shards:  make(map[uint32]*BoltShard),
		lock:    &sync.Mutex{},
	}, nil
}

func (d *BoltShardDatastore) BufferWrite(request *protocol.Request) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.writeBuffer.Write(request)
}

func (d *BoltShardDatastore) Close() {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, shard := range d.shards {
		shard.close()
	}
}

func (d *BoltShardDatastore) DeleteShard(shardId uint32) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	shardDir := filepath.Join(d.baseDir, fmt.Sprint(shardId))
	err := os.RemoveAll(shardDir)
	if err != nil {
		return err
	}

	delete(d.shards, shardId)
	return nil
}

func (d *BoltShardDatastore) GetOrCreateShard(id uint32) (cluster.LocalShardDb, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var (
		shard   *BoltShard
		present bool
	)
	if shard, present = d.shards[id]; !present {
		shardPath := filepath.Join(d.baseDir, fmt.Sprint(id))
		// create a shard
		if err := os.MkdirAll(shardPath, 0744); err != nil {
			return nil, err
		}

		shard = NewBoltShard(shardPath)
		d.shards[id] = shard
	}
	return shard, nil
}

func (d *BoltShardDatastore) ReturnShard(id uint32) {
	d.lock.Lock()
	defer d.lock.Unlock()
	//	d.shards[id].close()
	//	delete(d.shards, id)
}

func (d *BoltShardDatastore) SetWriteBuffer(writeBuffer *cluster.WriteBuffer) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.writeBuffer = writeBuffer
}

func (d *BoltShardDatastore) Write(request *protocol.Request) error {
	shardDb, err := d.GetOrCreateShard(*request.ShardId)
	if err != nil {
		return err
	}

	//defer d.ReturnShard(*request.ShardId)
	return shardDb.Write(*request.Database, request.MultiSeries)
}
