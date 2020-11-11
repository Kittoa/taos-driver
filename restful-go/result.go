package restful_go

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
	"reflect"
	"os"
)

func (ts *Taos) CreateTable(model interface{}) error {
	sql := getTableSql(ts.dbName, model)
	_, err := exec(ts.ip, ts.port, ts.token, ExecS, sql)
	if err != nil {
		return err
	}

	return nil
}

func (ts *Taos) CreateSubTable(subTableName string, model interface{}) error {
	sql := getSubTableSql(ts.dbName, subTableName, model)
	_, err := exec(ts.ip, ts.port, ts.token, ExecS, sql)
	if err != nil {
		return err
	}

	return nil
}

func (ts *Taos) Insert(subTable string, model interface{}) error {
	sql := getInsertSql(ts.dbName + "." + subTable, model)
	_, err := exec(ts.ip, ts.port, ts.token, ExecS, sql)
	if err != nil {
		return err
	}

	return nil
}

func (ts *Taos) Query(sql string, model interface{}) error {
	result, err := exec(ts.ip, ts.port, ts.token, ExecI, sql)
	if err != nil {
		return err
	}

	taosi := TaosSelectRusult{} //Data: model
	err = json.Unmarshal(result, &taosi)
	if err != nil {
		return err
	}

	transModel(model, taosi.Data)
	return nil
}

func (ts *Taos) RegisterModel(model ...interface{}) error {
	var sql string
	for _, v := range model {
		sql = getTableSql(ts.dbName, v)
		_, err := exec(ts.ip, ts.port, ts.token, ExecS, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ts *Taos) Subscribe(topic, tablename string, restart bool) error {
	if !isDirExist("subscribe") {
		err := os.Mkdir("subscribe", 0644)
		if err != nil {
			return err
		}
	}

	subscribe := Subscribe{topic: fmt.Sprintf(Select, ts.dbName + "." + tablename)}
	if isDirExist("subscribe" + "/" + topic) {
		result, err := ioutil.ReadFile("subscribe" + "/" + topic)
		if err != nil {
			return err
		}

		sub := strings.Split(string(result), "\n")
		subscribe.time = sub[1]
	}
	
	ts.subs[topic] = subscribe
	return nil
}

func (ts *Taos) UnSubscribe(topic string) error {
	substr := ts.subs[topic].topic + "\n" + ts.subs[topic].time
	err := ioutil.WriteFile("subscribe" + "/" + topic, []byte(substr), 0644)
	if err != nil {
		return err
	}
	return nil
}

func (ts *Taos) Consume(topic string, model interface{}) error {
	var sql string
	if ts.subs[topic].time == "" {
		sql = ts.subs[topic].topic
	}else{
		sql = ts.subs[topic].topic + " where ts > " + "\"" + ts.subs[topic].time + "\""
	}
	sql += " order by " + strings.Split(strings.Split(reflect.TypeOf(model).Elem().Elem().Field(0).Tag.Get("taos"), ";")[0], ":")[1]
	result, err := exec(ts.ip, ts.port, ts.token, ExecI, sql)
	if err != nil {
		return err
	}

	taosi := TaosSelectRusult{}
	err = json.Unmarshal(result, &taosi)
	if err != nil {
		return err
	}

	last := transModel(model, taosi.Data)
	if last != 0 {
		s := ts.subs[topic]
		s.time = time.Unix(0, last * 1e6).Format("2006-01-02 15:04:05.000")
		ts.subs[topic] = s
	}
	return nil
}
