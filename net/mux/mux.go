// Copyright 2018 fatedier, fatedier@gmail.com
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

package mux

import (
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/fatedier/golib/errors"
	gnet "github.com/fatedier/golib/net"
)

const (
	// DefaultTimeout is the default length of time to wait for bytes we need.
	DefaultTimeout = 10 * time.Second
)

// Mux 目的是为了复用端口 TODO 分析具体原理
type Mux struct {
	// 用于监听frps配置的BindAddr:BindPort，其实就是frpc需要注册的地址
	ln net.Listener

	// TODO 默认Listener是谁？
	defaultLn *listener

	// sorted by priority
	// 根据数据的特征，把流量导入到不同的listener当中，不同的listener可能特征是相同的，因此必须进行优先级排序
	// 在满足某个特征的情况下，优先级低的listener会被优先选择
	// listener的优先级按照从小打大排序，priority小的的listener会优先选择
	lns []*listener
	// 最多需要取出多少个字节，根据Mux的原理，由于不同协议的流量需要取出的字节数不同，为了能够正确匹配合适的listener,
	// 这里要取出的字节数至少是所有listener中需要的最大字节数
	maxNeedBytesNum uint32
	keepAlive       time.Duration

	mu sync.RWMutex
}

func NewMux(ln net.Listener) (mux *Mux) {
	mux = &Mux{
		ln:  ln,
		lns: make([]*listener, 0),
	}
	return
}

func (mux *Mux) SetKeepAlive(keepAlive time.Duration) {
	mux.keepAlive = keepAlive
}

// priority
func (mux *Mux) Listen(priority int, needBytesNum uint32, fn MatchFunc) net.Listener {
	ln := &listener{
		c:            make(chan net.Conn),
		mux:          mux,
		priority:     priority,
		needBytesNum: needBytesNum,
		matchFn:      fn,
	}

	mux.mu.Lock()
	defer mux.mu.Unlock()
	if needBytesNum > mux.maxNeedBytesNum {
		mux.maxNeedBytesNum = needBytesNum
	}

	newlns := append(mux.copyLns(), ln)
	sort.Slice(newlns, func(i, j int) bool {
		if newlns[i].priority == newlns[j].priority {
			return newlns[i].needBytesNum < newlns[j].needBytesNum
		}
		// 优先级按照从小到大进行排序
		return newlns[i].priority < newlns[j].priority
	})
	mux.lns = newlns
	return ln
}

func (mux *Mux) ListenHttp(priority int) net.Listener {
	return mux.Listen(priority, HttpNeedBytesNum, HttpMatchFunc)
}

func (mux *Mux) ListenHttps(priority int) net.Listener {
	return mux.Listen(priority, HttpsNeedBytesNum, HttpsMatchFunc)
}

func (mux *Mux) DefaultListener() net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.defaultLn == nil {
		mux.defaultLn = &listener{
			c:   make(chan net.Conn),
			mux: mux,
		}
	}
	return mux.defaultLn
}

func (mux *Mux) release(ln *listener) bool {
	result := false
	mux.mu.Lock()
	defer mux.mu.Unlock()
	lns := mux.copyLns()

	for i, l := range lns {
		if l == ln {
			lns = append(lns[:i], lns[i+1:]...)
			result = true
			break
		}
	}
	mux.lns = lns
	return result
}

func (mux *Mux) copyLns() []*listener {
	lns := make([]*listener, 0, len(mux.lns))
	for _, l := range mux.lns {
		lns = append(lns, l)
	}
	return lns
}

// Serve handles connections from ln and multiplexes then across registered listeners.
func (mux *Mux) Serve() error {
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		// Wait for the next connection.
		// If it returns a temporary error then simply retry.
		// If it returns any other error then exit immediately.
		// 开始监听frps配置的BindAddr:BindPort地址，等待frpc注册上来
		conn, err := mux.ln.Accept()
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			if tempDelay == 0 {
				tempDelay = 5 * time.Millisecond
			} else {
				tempDelay *= 2
			}
			if max := 1 * time.Second; tempDelay > max {
				tempDelay = max
			}
			time.Sleep(tempDelay)
			continue
		}

		if err != nil {
			return err
		}
		tempDelay = 0

		// 每接收到一个请求，就启动一个协程进行处理，Mux需要根据流量特征把数据转发给合适的Listener
		go mux.handleConn(conn)
	}
}

func (mux *Mux) handleConn(conn net.Conn) {
	mux.mu.RLock()
	// 拿到需要从连接中取出的最大字节数
	maxNeedBytesNum := mux.maxNeedBytesNum
	// 拿到后端listener,一会儿需要把流量具体代理到这其中的一个listener
	lns := mux.lns
	defaultLn := mux.defaultLn
	mux.mu.RUnlock()

	if mux.keepAlive != 0 {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlivePeriod(mux.keepAlive)
		}
	}

	// TODO 什么叫做共享连接？
	sharedConn, rd := gnet.NewSharedConnSize(conn, int(maxNeedBytesNum))
	data := make([]byte, maxNeedBytesNum)

	conn.SetReadDeadline(time.Now().Add(DefaultTimeout))
	// 从连接当中取出需要的字节数量放到rd当中
	_, err := io.ReadFull(rd, data)
	if err != nil {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	for _, ln := range lns {
		// 匹配一个合适的listener，然后把连接转发过去
		if match := ln.matchFn(data); match {
			err = errors.PanicToError(func() {
				ln.c <- sharedConn
			})
			if err != nil {
				conn.Close()
			}
			return
		}
	}

	// No match listeners
	if defaultLn != nil {
		err = errors.PanicToError(func() {
			defaultLn.c <- sharedConn
		})
		if err != nil {
			conn.Close()
		}
		return
	}

	// No listeners for this connection, close it.
	conn.Close()
	return
}

type listener struct {
	mux *Mux

	priority     int
	needBytesNum uint32
	matchFn      MatchFunc

	c  chan net.Conn
	mu sync.RWMutex
}

// Accept waits for and returns the next connection to the listener.
func (ln *listener) Accept() (net.Conn, error) {
	conn, ok := <-ln.c
	if !ok {
		return nil, fmt.Errorf("network connection closed")
	}
	return conn, nil
}

// Close removes this listener from the parent mux and closes the channel.
func (ln *listener) Close() error {
	if ok := ln.mux.release(ln); ok {
		// Close done to signal to any RLock holders to release their lock.
		close(ln.c)
	}
	return nil
}

func (ln *listener) Addr() net.Addr {
	if ln.mux == nil {
		return nil
	}
	ln.mux.mu.RLock()
	defer ln.mux.mu.RUnlock()
	if ln.mux.ln == nil {
		return nil
	}
	return ln.mux.ln.Addr()
}
