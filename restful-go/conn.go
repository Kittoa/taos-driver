package restful_go

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func Open(ip, port, user, passwd, dbname string) (*Taos, error) {
	taos := new(Taos)
	url := fmt.Sprintf(Login, ip, port, user, passwd)
	resp, err := http.Get(url)
	if err != nil {
		return taos, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return taos, err
	}

	taoslr := TaosLoginRusult{}
	err = json.Unmarshal(body, &taoslr)
	if err != nil {
		return taos, err
	}

	if taoslr.Code != 0 || taoslr.Status != "succ" {
		return taos, errors.New("taos connect failed," + taoslr.Desc)
	}

	taos.ip = ip
	taos.port = port
	taos.dbName = dbname
	taos.token = taoslr.Desc
	taos.subs = make(map[string]Subscribe)
	return taos, nil
}

func exec(ip, port, token, tstype, sql string) ([]byte, error) {
	url := fmt.Sprintf(tstype, ip, port)
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, strings.NewReader(sql))
	if err != nil {
		return []byte{}, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Taosd " + token)
	resp, err := client.Do(req)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}

	if resp.StatusCode != 200 {
		taoslr := TaosLoginRusult{}
		err := json.Unmarshal(body, &taoslr)
		if err != nil {
			return []byte{}, err
		}
		return []byte{}, errors.New(taoslr.Desc)
	}

	return body, nil
}
