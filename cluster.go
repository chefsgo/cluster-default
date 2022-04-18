package cluster_default

import (
	"errors"
	"strings"
	"sync"

	"github.com/chef-go/chef"
)

//默认logger驱动
//test

type (
	defaultClusterDriver struct {
	}
	defaultClusterConnect struct {
		config chef.ClusterConfig
		mutex  sync.RWMutex
		data   chef.ClusterData
	}
)

// Connect
func (driver *defaultClusterDriver) Connect(config chef.ClusterConfig) (chef.ClusterConnect, error) {
	return &defaultClusterConnect{
		config: config,
	}, nil
}

// Open 打开连接
func (connect *defaultClusterConnect) Open() error {
	return nil
}

// Close 关闭连接
func (connect *defaultClusterConnect) Close() error {
	return nil
}

// Read
func (connect *defaultClusterConnect) Read(key string) ([]byte, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()

	if val, ok := connect.data[key]; ok {
		return val, nil
	}

	return nil, errors.New("read error")
}

// Write
func (connect *defaultClusterConnect) Write(key string, val []byte) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.data[key] = val

	return nil
}

// Delete
func (connect *defaultClusterConnect) Delete(key string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	delete(connect.data, key)

	return nil
}

func (connect *defaultClusterConnect) Clear(prefix string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	for key, _ := range connect.data {
		if strings.HasPrefix(key, prefix) {
			delete(connect.data, key)
		}
	}

	return nil
}
func (connect *defaultClusterConnect) Batch(data chef.ClusterData) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	for key, val := range data {
		if val == nil {
			delete(connect.data, key)
		} else {
			connect.data[key] = val
		}
	}
	return nil
}

func (connect *defaultClusterConnect) Fetch(prefix string) (chef.ClusterData, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()

	data := make(chef.ClusterData, 0)

	for key, val := range connect.data {
		if strings.HasPrefix(key, prefix) {
			data[key] = val
		}
	}

	return data, nil
}

//单节点模式，肯定命中
func (connect *defaultClusterConnect) Locate(key string) bool {
	return true
}

func (connect *defaultClusterConnect) Peers() []chef.ClusterPeer {
	return []chef.ClusterPeer{}
}
