package taos_go

import (
	"time"
	"unsafe"

	log "github.com/Sirupsen/logrus"
)

func (ts *TaosDB) Open(ip, user, pass, dbname, path string, port uint32) error{
	var err error
	new(TaosConn).ConfigPath(path)
	ts.sub = make(map[string]GenericlConn)
	ts.pool, err = NewGenericPool(10,20, time.Duration(time.Now().Unix()), func() (GenericlConn, error) {
		tc := &TaosConn{}
		err = tc.Open(ip, user, pass, dbname, port)
		return taosc, err
	})
	return err
}

func (ts *TaosDB) Close() error{
	return ts.pool.Shutdown()
}

func (ts *TaosDB) Subscribe(restart int, topic, sql string, fp, param unsafe.Pointer, interval int) error{
	var err error
	log.Infoln("jzp Subscribe")
	ts.sub[topic], err = ts.pool.Acquire()
	if err != nil {
		log.Infoln("jzp Subscribe failed")
		return err
	}
	log.Infoln("jzp Subscribe success")
	return ts.sub[topic].(*TaosConn).Subscribe(restart, topic, sql, fp, param , interval)
}

func (ts *TaosDB) UnSubscribe(topic string, keepProgress int) error{
	err := ts.sub[topic].(*TaosConn).UnSubscribe(keepProgress)
	ts.pool.Release(ts.sub[topic])
	delete(ts.sub, topic)
	return err
}

func (ts *TaosDB) Consume(topic string, data interface{}) error{
	return ts.sub[topic].(*TaosConn).Consume(data)
}

func (ts *TaosDB) Insert(sql string) (int, error){
	conn, err := ts.pool.Acquire()
	if err != nil {
		return 0, err
	}
	defer ts.pool.Release(conn)

	return conn.(*TaosConn).Insert(sql)
}

func (ts *TaosDB) Create(sql string) (int, error){
	conn, err := ts.pool.Acquire()
	if err != nil {
		return 0, err
	}
	defer ts.pool.Release(conn)

	return conn.(*TaosConn).Create(sql)
}

func (ts *TaosDB) Query(sql string, model interface{}) (int, error){
	conn, err := ts.pool.Acquire()
	if err != nil {
		return 0, err
	}
	defer ts.pool.Release(conn)

	return conn.(*TaosConn).Query(sql, model)
}

func (ts *TaosDB) Enable() bool{
	return ts.pool != nil
}