package taos_go

import (
	"errors"
	"sync"
	"time"
	"unsafe"
)

var (
	ErrInvalidConfig = errors.New("invalid pool config")
	ErrPoolClosed    = errors.New("pool closed")
	ErrInvalidParam = errors.New("invalid input param")

	CreateTableSql = "create table if not exists %v (%v) tags (%v)"
	CreateSubTableSql = "create table if not exists %v using %v tags (\"%v\", %v)"
	InsertSql = "insert into %s values(%s)"
	SelectSql = "select * from %v"
)

type Taos struct {
	taos           *TaosDB
	dbname         string
	lcoation       string
	groupid        int
	exist          bool
}

type fieldType byte
type fieldFlag uint16

type taos struct {
	taos         unsafe.Pointer
	result       unsafe.Pointer
	taos_sub     unsafe.Pointer
	rows         []taosSqlField
	affectedRows int
	insertId     int
}

type taosSqlField struct {
	tableName string
	name      string
	length    uint32
	flags     fieldFlag // indicate whether this field can is null
	fieldType fieldType
	decimals  byte
	charSet   uint8
}

type Pool struct{
	sync.Mutex
	pool        chan GenericlConn
	maxOpen     int  // 池中最大资源数
	numOpen     int  // 当前池中资源数
	minOpen     int  // 池中最少资源数
	closed      bool // 池是否已关闭
	maxLifetime time.Duration
	factory     factory // 创建连接的方法
}

type TaosConn struct {
	taos taos
}

type TaosDB struct {
	pool *Pool
	sub  map[string]GenericlConn
}


