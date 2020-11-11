package restful_go

var (
	Login = "http://%v:%v/rest/login/%v/%v"
	ExecS = "http://%v:%v/rest/sql"
	ExecI = "http://%v:%v/rest/sqlt"

	CreateTableSql    = "create table if not exists %v (%v) tags (%v)"
	CreateSubTableSql = "create table if not exists %v using %v tags (%v)"
	InsertSql         = "insert into %v values(%v)"
	Select            = "select * from %v"
)

type Taos struct {
	dbName string
	token  string
	ip     string
	port   string
	subs   map[string]Subscribe
}

type Subscribe struct {
	topic string
	time  string
}

type TaosLoginRusult struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
	Desc   string `json:"desc"`
}

type TaosSelectRusult struct {
	Status string      `json:"status"`
	Head   []string    `json:"head"`
	Data   interface{} `json:"data"`
	Rows   int         `json:"rows"`
}

