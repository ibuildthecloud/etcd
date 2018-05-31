// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientv3

import (
	"database/sql"
	"sync"

	"github.com/coreos/etcd/clientv3/driver"
	"github.com/coreos/etcd/clientv3/driver/sqlite"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type (
	CompactResponse pb.CompactionResponse
	PutResponse     pb.PutResponse
	GetResponse     pb.RangeResponse
	DeleteResponse  pb.DeleteRangeResponse
	TxnResponse     pb.TxnResponse
)

var (
	connection *kv
	dbOnce     sync.Once
)

type KV interface {
	// Put puts a key-value pair into etcd.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error)

	// Get retrieves keys.
	// By default, Get will return the value for "key", if any.
	// When passed WithRange(end), Get will return the keys in the range [key, end).
	// When passed WithFromKey(), Get returns keys greater than or equal to key.
	// When passed WithRev(rev) with rev > 0, Get retrieves keys at the given revision;
	// if the required revision is compacted, the request will fail with ErrCompacted .
	// When passed WithLimit(limit), the number of returned keys is bounded by limit.
	// When passed WithSort(), the keys will be sorted.
	Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error)

	// Compact compacts etcd KV history before the given rev.
	Compact(ctx context.Context, rev int64, opts ...CompactOption) (*CompactResponse, error)

	// Txn creates a transaction.
	Txn(ctx context.Context) Txn
}

type kv struct {
	sync.Mutex
	d driver.Driver
}

func newKV() *kv {
	dbOnce.Do(func() {
		var err error
		db, err := sql.Open("sqlite3", "./foo.db")
		if err != nil {
			logrus.Fatal(err)
		}

		d := sqlite.NewSQLite()
		if err := d.Start(context.TODO(), db); err != nil {
			panic(err)
		}

		connection = &kv{
			d: d,
		}
	})

	return connection
}

func (k *kv) Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error) {
	k.Lock()
	defer k.Unlock()

	op := OpPut(key, val, opts...)
	return k.opPut(ctx, op)
}

func (k *kv) opPut(ctx context.Context, op Op) (*PutResponse, error) {
	oldR, r, err := k.d.Update(ctx, op.key, op.val, op.rev, int64(op.leaseID))
	if err != nil {
		return nil, err
	}
	return getPutResponse(oldR, r), nil
}

func (k *kv) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	op := OpGet(key, opts...)
	return k.opGet(ctx, op)
}

func (k *kv) opGet(ctx context.Context, op Op) (*GetResponse, error) {
	kvs, err := k.d.List(ctx, op.rev, op.limit, op.key, op.boundingKey)
	if err != nil {
		return nil, err
	}

	return getResponse(kvs, op.limit, op.countOnly), nil
}

func getPutResponse(oldValue *driver.KeyValue, value *driver.KeyValue) *PutResponse {
	return &PutResponse{
		Header: &pb.ResponseHeader{
			Revision: value.Revision,
		},
		PrevKv: toKeyValue(oldValue),
	}
}

func toKeyValue(v *driver.KeyValue) *mvccpb.KeyValue {
	if v == nil {
		return nil
	}

	return &mvccpb.KeyValue{
		Key:            []byte(v.Key),
		CreateRevision: v.CreateRevision,
		ModRevision:    v.Revision,
		Version:        v.Version,
		Value:          v.Value,
		Lease:          v.TTL,
	}
}

func getDeleteResponse(values []*driver.KeyValue) *DeleteResponse {
	gr := getResponse(values, 0, false)
	return &DeleteResponse{
		Header: &pb.ResponseHeader{
			Revision: gr.Header.Revision,
		},
		PrevKvs: gr.Kvs,
	}
}

func getResponse(values []*driver.KeyValue, limit int64, count bool) *GetResponse {
	gr := &GetResponse{
		Header: &pb.ResponseHeader{},
	}

	for _, v := range values {
		kv := toKeyValue(v)
		if kv.ModRevision > gr.Header.Revision {
			gr.Header.Revision = kv.ModRevision
		}

		gr.Kvs = append(gr.Kvs, kv)
	}

	gr.Count = int64(len(gr.Kvs))
	if limit > 0 && gr.Count > limit {
		gr.Kvs = gr.Kvs[:limit]
		gr.More = true
	}

	if count {
		gr.Kvs = nil
	}

	return gr
}

func (k *kv) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	k.Lock()
	defer k.Unlock()

	op := OpDelete(key, opts...)
	return k.opDelete(ctx, op)
}

func (k *kv) opDelete(ctx context.Context, op Op) (*DeleteResponse, error) {
	r, err := k.d.Delete(ctx, op.key, op.rev)
	if err != nil {
		return nil, err
	}
	return getDeleteResponse(r), nil
}

func (k *kv) Compact(ctx context.Context, rev int64, opts ...CompactOption) (*CompactResponse, error) {
	return &CompactResponse{
		Header: &pb.ResponseHeader{},
	}, nil
}

func (k *kv) Txn(ctx context.Context) Txn {
	return &txn{
		kv:  k,
		ctx: ctx,
	}
}
