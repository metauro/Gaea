// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// Copyright 2019 The Gaea Authors. All Rights Reserved.
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

package etcdclient

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/XiaoMi/Gaea/log"
	"go.etcd.io/etcd/clientv3"
)

// ErrClosedEtcdClient means etcd client closed
var ErrClosedEtcdClient = errors.New("use of closed etcd client")

const (
	defaultEtcdPrefix = "/gaea"
)

// EtcdClient etcd client
type EtcdClient struct {
	sync.Mutex
	client  *clientv3.Client
	closed  bool
	timeout time.Duration
	Prefix  string
	lease   map[string]clientv3.LeaseID
}

// New constructor of EtcdClient
func New(addr string, timeout time.Duration, username, passwd, root string) (*EtcdClient, error) {
	endpoints := strings.Split(addr, ",")
	for i, s := range endpoints {
		if s != "" && !strings.HasPrefix(s, "http://") {
			endpoints[i] = "http://" + s
		}
	}
	config := clientv3.Config{
		Endpoints:   endpoints,
		Username:    username,
		Password:    passwd,
		DialTimeout: time.Second * 10,
	}
	c, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(root) == "" {
		root = defaultEtcdPrefix
	}
	return &EtcdClient{
		client:  c,
		timeout: timeout,
		Prefix:  root,
		lease:   make(map[string]clientv3.LeaseID),
	}, nil
}

// Close close etcd client
func (c *EtcdClient) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}

func (c *EtcdClient) contextWithTimeout() (context.Context, context.CancelFunc) {
	if c.timeout == 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), c.timeout)
}

// Mkdir create directory
func (c *EtcdClient) Mkdir(dir string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	return c.mkdir(dir)
}

func (c *EtcdClient) mkdir(dir string) error {
	if dir == "" || dir == "/" {
		return nil
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()

	_, err := c.client.Put(cntx, dir, "")
	if err != nil {
		return err
	}
	return nil
}

// Create create path with data
func (c *EtcdClient) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	log.Debug("etcd create node %s", path)
	_, err := c.client.Put(cntx, path, string(data))
	if err != nil {
		log.Debug("etcd create node %s failed: %s", path, err)
		return err
	}
	log.Debug("etcd create node OK")
	return nil
}

// Update update path with data
func (c *EtcdClient) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	log.Debug("etcd update node %s", path)
	_, err := c.client.Put(cntx, path, string(data))
	if err != nil {
		log.Debug("etcd update node %s failed: %s", path, err)
		return err
	}
	log.Debug("etcd update node OK")
	return nil
}

// UpdateWithTTL update path with data and ttl
func (c *EtcdClient) UpdateWithTTL(path string, data []byte, ttl time.Duration) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	log.Debug("etcd update node %s with ttl %d", path, ttl)
	if _, ex := c.lease[path]; !ex {
		res, err := c.client.Grant(cntx, int64(ttl.Seconds()))
		if err != nil {
			return err
		}
		c.lease[path] = res.ID
	}

	_, err := c.client.Put(cntx, path, string(data), clientv3.WithLease(c.lease[path]))
	if err != nil {
		log.Debug("etcd update node %s failed: %s", path, err)
		return err
	}
	log.Debug("etcd update node OK")
	return nil
}

// Delete delete path
func (c *EtcdClient) Delete(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	log.Debug("etcd delete node %s", path)
	_, err := c.client.Delete(cntx, path)
	if err != nil {
		log.Debug("etcd delete node %s failed: %s", path, err)
		return err
	}
	log.Debug("etcd delete node OK")
	return nil
}

// Read read path data
func (c *EtcdClient) Read(path string) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	log.Debug("etcd read node %s", path)
	r, err := c.client.Get(cntx, path)
	if err != nil {
		return nil, err
	}
	if len(r.Kvs) == 0 || len(r.Kvs) > 1 {
		return nil, nil
	}
	return r.Kvs[0].Value, nil
}

// List list path, return slice of all paths
func (c *EtcdClient) List(path string) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, ErrClosedEtcdClient
	}
	cntx, canceller := c.contextWithTimeout()
	defer canceller()
	log.Debug("etcd list node %s", path)
	r, err := c.client.Get(cntx, path, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make([]string, len(r.Kvs))
	for i, v := range r.Kvs {
		result[i] = string(v.Key)
	}
	return result, nil
}

// v.Key watch path
func (c *EtcdClient) Watch(path string, ch chan string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		panic(ErrClosedEtcdClient)
	}
	watcher := c.client.Watch(context.Background(), path, clientv3.WithPrefix())
	for {
		res := <-watcher
		if err := res.Err(); err != nil {
			panic(err)
		}
		action := res.Events[0].Type
		if action == clientv3.EventTypePut {
			ch <- "set"
		} else if action == clientv3.EventTypeDelete {
			ch <- "delete"
		}
	}
}

// BasePrefix return base prefix
func (c *EtcdClient) BasePrefix() string {
	return c.Prefix
}
