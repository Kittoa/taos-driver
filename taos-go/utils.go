package taos_go

func getTableSql(model interface{}) string{
	var param, tag, sql string

	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Tag.Get("taos_tag") == "" {
			tagStr := t.Field(i).Tag.Get("taos")
			for _, v := range strings.Split(tagStr, ";") {
				kv := strings.Split(v, ":")
				if kv[0] == "column" {
					param += kv[1] + " "
				}
				if kv[0] == "type" {
					param += kv[1] + ", "
				}
			}

		} else {
			tagStr := t.Field(i).Tag.Get("taos_tag")
			for _, v := range strings.Split(tagStr, ";") {
				kv := strings.Split(v, ":")
				if kv[0] == "column" {
					tag += kv[1] + " "
				}
				if kv[0] == "type" {
					tag += kv[1] + ", "
				}
			}
		}
	}

	v := reflect.ValueOf(model)
	sql = fmt.Sprintf(CreateTableSql, v.MethodByName("GetTable").Call(nil)[0].Interface().(string), param[:len(param) - 2], tag[:len(tag) - 2])
	return sql
}

func getSubTableSql(dbname, subTableName string, model interface{}) string{
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	tagsStr := ""
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Tag.Get("taos_tag") != "" {
			switch t.Field(i).Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				tagsStr += strconv.Itoa(int(v.Field(i).Int())) + ", "
			case reflect.String:
				tagsStr += "\"" + v.Field(i).String() + "\"" + ", "
			case reflect.Float32, reflect.Float64:
				tagsStr += strconv.FormatFloat(v.Field(i).Float(), 'f', 6, 64) + ", "
			}
		}
	}

	sql := fmt.Sprintf(CreateSubTableSql, dbname + "." + subTableName,
		dbname + "." + v.MethodByName("GetTable").Call(nil)[0].Interface().(string),
		tagsStr[:len(tagsStr) - 2])
	return sql
}

func getSubscribeSql(model interface{}) string{
	v := reflect.ValueOf(model)
	sql := fmt.Sprintf(SelectSql, v.MethodByName("GetTable").Call(nil)[0].Interface().(string))
	return sql
}

func getInsertSql(subTableName string, model interface{}) string {
	var sql, param string
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < v.NumField() ;i++ {
		if strings.Contains(t.Field(i).Tag.Get("taos"), "timestamp") {
			param += "now, "
			continue
		}

		if t.Field(i).Tag.Get("taos_tag") != "" {
			continue
		}

		switch v.Field(i).Kind() {
		case reflect.String:
			param += "\"" + v.Field(i).String() + "\"" + ", "
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			param += strconv.Itoa(int(v.Field(i).Int())) + ", "
		case reflect.Float32, reflect.Float64:
			param += strconv.FormatFloat(v.Field(i).Float(), 'f', 6, 64) + ", "
		case reflect.Struct:
			if v.Field(i).String() == "time.Time" {
				param += v.Field(i).Interface().(time.Time).Format("2006-01-02T15:04:05.000") + ", "
			}
		}
	}
	sql = fmt.Sprintf(InsertSql, subTableName, param[:len(param) - 2])
	return sql
}

func isDirExist(dir string) bool {
	_, err := os.Stat(dir)
	return err == nil || os.IsExist(err)
}
