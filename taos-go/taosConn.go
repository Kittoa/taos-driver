package taos_go

import (
	"unsafe"
	"reflect"
)

func (tc *TaosConn) Open(ip, user, pass, db string, port uint32) error{
	return tc.taos.taosConnect(ip, user, pass, db, port)
}

func (tc *TaosConn) Close() {
	tc.taos.taosClose()
}

func (tc *TaosConn) Subscribe(restart int, topic, sql string, fp, param unsafe.Pointer, interval int) error{
	return tc.taos.taosSubscribe(restart, topic, sql, fp, param, interval)
}

func (tc *TaosConn) UnSubscribe(keepProgress int) error{
	return tc.taos.taosUnSubscribe(keepProgress)
}

func (tc *TaosConn) ConfigPath(path string) error{
	return tc.taos.taosConfigPath(path)
}

func (tc *TaosConn) Consume(data interface{}) error{
	if data == nil {
		return ErrInvalidParam
	}
	tc.taos.taosConsume()
	return tc.query(data)
}

func (tc *TaosConn) Insert(sql string) (int, error){
	return tc.taos.exec(sql)
}

func (tc *TaosConn) Create(sql string) (int, error){
	return tc.taos.exec(sql)
}

func (tc *TaosConn) query(data interface{}) error{
	t := reflect.TypeOf(data)
	rows, err := tc.taos.getRow(t.Elem().Elem().NumField())
	if err != nil {
		return err
	}

	data0 := reflect.ValueOf(data).Elem()

	m0 := make([]reflect.Value, 0)
	for _, v := range rows {
		m := reflect.New(t.Elem().Elem())
		for k, val := range v {
			if reflect.ValueOf(val).IsValid() {
				m.Elem().Field(k).Set(reflect.ValueOf(val))
			}
		}
		m0 = append(m0, m.Elem())
	}
	val_arr := reflect.Append(data0, m0...)
	data0.Set(val_arr)
	return nil
}

func (tc *TaosConn) Query(sql string, model interface{}) (int, error){
	n, err := tc.taos.exec(sql)
	if err != nil {
		return n, err
	}
	return n, tc.query(model)
}