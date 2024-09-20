package memcached

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	gomemcache "github.com/bradfitz/gomemcache/memcache"
)

type memcached struct {
	client     *gomemcache.Client
	fieldcount int64
}

func (mc *memcached) Close() error {
	return mc.client.Close()
}

func (mc *memcached) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (mc *memcached) CleanupThread(_ context.Context) {
}

func (mc *memcached) Read(ctx context.Context, table string, key string, fields []string) (data map[string][]byte, err error) {
	data = make(map[string][]byte, len(fields))
	err = nil
	it, err2 := mc.client.Get(getKeyName(table, key))
	err = err2
	if err != nil {
		return
	}
	err = json.Unmarshal(it.Value, &data)
	return
}

func (mc *memcached) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (mc *memcached) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	fullUpdate := false
	if int64(len(values)) == mc.fieldcount {
		fullUpdate = true
	}
	err = nil
	var encodedJson = make([]byte, 0)
	if fullUpdate {
		encodedJson, err = json.Marshal(values)
		if err != nil {
			return err
		}
	} else {
		var initialEncodedJson string = ""
		it, err2 := mc.client.Get(getKeyName(table, key))
		err = err2
		if err != nil {
			return
		}
		initialEncodedJson = string(it.Value)
		err, encodedJson = mergeEncodedJsonWithMap(initialEncodedJson, values)
		if err != nil {
			return
		}
		return mc.client.Set(&gomemcache.Item{Key: getKeyName(table, key), Value: []byte(string(encodedJson))})
	}
	return
}

func mergeEncodedJsonWithMap(stringReply string, values map[string][]byte) (err error, data []byte) {
	curVal := map[string][]byte{}
	err = json.Unmarshal([]byte(stringReply), &curVal)
	if err != nil {
		return
	}
	for k, v := range values {
		curVal[k] = v
	}
	data, err = json.Marshal(curVal)
	return
}

func getKeyName(table string, key string) string {
	return table + "/" + key
}

func (mc *memcached) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	err = mc.client.Set(&gomemcache.Item{Key: getKeyName(table, key), Value: []byte(string(data))})
	return
}

func (mc *memcached) Delete(ctx context.Context, table string, key string) error {
	return nil
}

const (
	memcachedAddr        = "memcached.addr"
	memcachedAddrDefault = "localhost:11211"
)

type memcachedCreator struct{}

func (mc memcachedCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	memcached := &memcached{}

	addr := p.GetString(memcachedAddr, memcachedAddrDefault)
	mclient := gomemcache.New(addr)

	memcached.client = mclient
	memcached.fieldcount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	return memcached, nil
}

func init() {
	ycsb.RegisterDBCreator("memcached", memcachedCreator{})
}
