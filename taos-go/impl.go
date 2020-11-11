package taos_go

type factory func()(GenericlConn, error)

type GenericPool interface {
	Acquire() (GenericlConn, error) // 获取资源
	Release(GenericlConn) error     // 释放资源
	Close(GenericlConn) error       // 关闭资源
	Shutdown() error                // 关闭池
}

type GenericlConn interface {
	Close()
}


