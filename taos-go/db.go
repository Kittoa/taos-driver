package taos_go

import (
	"strconv"
)

//NewTaosDb 涛思数据库初始化
func Open(ip, port, user, passwd, dbName, configPath string) (*Taos, error) {
	taos := &Taos{dbname: dbName}
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, errors.New("init taos db error: " + err.Error())
	}

	err := taos.taos.Open(ip, user, passwd, dbName, configPath, p)
	if err != nil {
		return nil, errors.New("init taos db error: " + err.Error())
	}
	return taos, nil
}

func (taos *Taos) CreateTable(sql string) (int, error) {
	return taos.taos.Create(sql)
}

func (taos *Taos) CreateSubTable(subTabName string, model interface{}) (int, error) {
	sql := getSubTableSql(subTabName, model)
	return taos.taos.Create(sql)
}

func (taos *Taos) Insert(sql string) (int, error) {
	return taos.taos.Insert(sql)
}

func (taos *Taos) InsertModel(subTabName string, model interface{}) (int, error) {
	sql := getInsertSql(subTabName, model)
	return taos.taos.Insert(sql)
}

func (taos *Taos) Query(sql string, model interface{}) (int, error) {
	return taos.taos.Query(sql, model)
}

func (taos *Taos) Subscribe(topic, sql string) error {
	return taos.taos.Subscribe(0, topic, sql, nil, nil, 1000)
}

func (taos *Taos) SubscribeModel(topic string, model interface{}) error {
	sql := getSubscribeSql(model)
	return taos.taos.Subscribe(0, topic, sql, nil, nil, 1000)
}

func (taos *Taos) UnSubscribe(topic string) error {
	return taos.taos.UnSubscribe(topic, 1)
}

func (taos *Taos) Consume(topic string, data interface{}) error {
	return taos.taos.Consume(topic, data)
}

func (taos *Taos) Enable() bool {
	return taos.taos.Enable()
}

func (taos *Taos) GetDbName() string {
	return taos.dbname
}

func (taos *Taos) Exist() bool {
	return taos.exist
}

func (taos *Taos) SetExist() {
	taos.exist = true
}

func (taos *Taos) SubExists() bool {
	return isDirExist("subconf")
}

func (taos *Taos) Destory() {
	taos.taos.Close()
}
