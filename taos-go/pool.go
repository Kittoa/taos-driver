package taos_go

import (
	"time"
	"fmt"
)

func NewGenericPool(minOpen, maxOpen int, maxLifetime time.Duration, factory factory)(*Pool, error) {
	if maxOpen <= 0 || minOpen > maxOpen {
		return nil, ErrInvalidConfig
	}
	p := &Pool{
		maxOpen:     maxOpen,
		minOpen:     minOpen,
		maxLifetime: maxLifetime,
		factory:     factory,
		pool:        make(chan GenericlConn, maxOpen),
	}

	for i := 0; i < minOpen; i++ {
		conn, err := factory()
		if err != nil {
			fmt.Println("create conn err:", err)
			return p, err
		}
		p.numOpen++
		p.pool <- conn
	}
	return p, nil
}

func (p *Pool)Acquire()(GenericlConn, error) {
	if p.closed {
		return nil, ErrPoolClosed
	}
	for {
		conn, err := p.getOrCreate()
		if err != nil {
			return nil, err
		}
		// todo maxLifttime处理
		return conn, nil
	}
}

func (p *Pool)getOrCreate()(GenericlConn, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	default:
	}
	p.Lock()
	if p.numOpen >= p.maxOpen {
		conn := <-p.pool
		p.Unlock()
		return conn, nil
	}
	// 新建连接
	conn, err := p.factory()
	if err != nil {
		p.Unlock()
		return nil, err
	}
	p.numOpen++
	p.Unlock()
	return conn, nil
}

// 释放单个资源到连接池
func (p *Pool)Release(conn GenericlConn)error {
	if p.closed {
		return ErrPoolClosed
	}
	p.Lock()
	p.pool <- conn
	p.Unlock()
	return nil
}

// 关闭单个资源
func (p *Pool)Close(conn GenericlConn)error {
	p.Lock()
	conn.Close()
	p.numOpen--
	p.Unlock()
	return nil
}

// 关闭连接池，释放所有资源
func (p *Pool)Shutdown()error {
	if p.closed {
		return ErrPoolClosed
	}
	p.Lock()
	close(p.pool)
	for conn := range p.pool {
		conn.Close()
		p.numOpen--
	}
	p.closed = true
	p.Unlock()
	return nil
}
