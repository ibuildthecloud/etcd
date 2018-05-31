package driver

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"time"

	"fmt"

	"github.com/pkg/errors"
	"github.com/rancher/norman/pkg/broadcast"
)

type Generic struct {
	db *sql.DB

	CleanupSQL      string
	ListSQL         string
	ListRevisionSQL string
	ListResumeSQL   string
	ReplaySQL       string
	InsertSQL       string
	GetRevisionSQL  string
	revision        int64

	changes     chan *KeyValue
	broadcaster broadcast.Broadcaster
	cancel      func()
}

func (g *Generic) Start(ctx context.Context, db *sql.DB) error {
	g.db = db
	g.changes = make(chan *KeyValue, 1024)

	row := db.QueryRowContext(ctx, g.GetRevisionSQL)
	rev := sql.NullInt64{}
	if err := row.Scan(&rev); err != nil {
		return errors.Wrap(err, "Failed to initialize revision")
	}
	if rev.Int64 == 0 {
		g.revision = 1
	} else {
		g.revision = rev.Int64
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				db.ExecContext(ctx, g.CleanupSQL, time.Now().Second())
			}
		}
	}()

	return nil
}

func (g *Generic) Get(ctx context.Context, key string) (*KeyValue, error) {
	kvs, err := g.List(ctx, 0, 1, key, "")
	if err != nil {
		return nil, err
	}
	if len(kvs) > 0 {
		return kvs[0], nil
	}
	return nil, nil
}

func (g *Generic) replayEvents(ctx context.Context, key string, revision int64) ([]*KeyValue, error) {
	rows, err := g.db.QueryContext(ctx, g.ReplaySQL, key, revision)
	fmt.Printf("!!!! REPLAYED for key %s\n", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp []*KeyValue
	for rows.Next() {
		value := KeyValue{}
		if err := scan(rows.Scan, &value); err != nil {
			return nil, err
		}
		fmt.Printf("!!!! REPLAYED for key %s: %v\n", key, value)
		resp = append(resp, &value)
	}

	return resp, nil
}

func (g *Generic) List(ctx context.Context, revision, limit int64, rangeKey, startKey string) ([]*KeyValue, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if limit == 0 {
		limit = 1000000
	} else {
		limit = limit + 1
	}

	if revision <= 0 {
		rows, err = g.db.QueryContext(ctx, g.ListSQL, rangeKey, limit)
	} else if len(startKey) > 0 {
		rows, err = g.db.QueryContext(ctx, g.ListResumeSQL, revision, rangeKey, startKey, limit)
	} else {
		rows, err = g.db.QueryContext(ctx, g.ListRevisionSQL, revision, rangeKey, limit)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp []*KeyValue
	for rows.Next() {
		value := KeyValue{}
		if err := scan(rows.Scan, &value); err != nil {
			return nil, err
		}
		if value.Del == 0 {
			resp = append(resp, &value)
		}
	}

	return resp, nil
}

func (g *Generic) Delete(ctx context.Context, key string, revision int64) ([]*KeyValue, error) {
	if strings.HasSuffix(key, "%") {
		panic("can not delete list revision")
	}

	_, err := g.mod(ctx, true, key, []byte{}, revision, 0)
	return nil, err
}

func (g *Generic) Update(ctx context.Context, key string, value []byte, revision, ttl int64) (*KeyValue, *KeyValue, error) {
	kv, err := g.mod(ctx, false, key, value, revision, ttl)
	if err != nil {
		return nil, nil, err
	}

	if kv.Version == 1 {
		return nil, kv, nil
	}

	oldKv := *kv
	oldKv.Revision = oldKv.OldRevision
	oldKv.Value = oldKv.OldValue
	return &oldKv, kv, nil
}

func (g *Generic) mod(ctx context.Context, delete bool, key string, value []byte, revision int64, ttl int64) (*KeyValue, error) {
	oldKv, err := g.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if revision > 0 && oldKv == nil {
		return nil, ErrNotExists
	}

	if revision > 0 && oldKv.Revision != revision {
		return nil, ErrRevisionMatch
	}

	newRevision := atomic.AddInt64(&g.revision, 1)
	result := &KeyValue{
		Key:            key,
		Value:          value,
		Revision:       newRevision,
		TTL:            int64(ttl),
		CreateRevision: newRevision,
		Version:        1,
	}
	if oldKv != nil {
		result.OldRevision = oldKv.Revision
		result.OldValue = oldKv.Value
		result.TTL = oldKv.TTL
		result.CreateRevision = oldKv.CreateRevision
		result.Version = oldKv.Version + 1
	}

	if delete {
		result.Del = 1
	}

	_, err = g.db.ExecContext(ctx, g.InsertSQL,
		result.Key,
		result.Value,
		result.OldValue,
		result.OldRevision,
		result.CreateRevision,
		result.Revision,
		result.TTL,
		result.Version,
		result.Del,
	)
	if err != nil {
		return nil, err
	}

	g.changes <- result
	return result, nil
}

type scanner func(dest ...interface{}) error

func scan(s scanner, out *KeyValue) error {
	return s(
		&out.ID,
		&out.Key,
		&out.Value,
		&out.OldValue,
		&out.OldRevision,
		&out.CreateRevision,
		&out.Revision,
		&out.TTL,
		&out.Version,
		&out.Del)
}
