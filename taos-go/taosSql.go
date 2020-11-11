package taos_go

/*
#cgo CFLAGS : -I./ -I./thirdInclude
#cgo LDFLAGS: -L./ -L./thirdLib -L./taos-driver -ltaos
#cgo LDFLAGS: -Wl,-rpath="./"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include  "taos.h"
int taos_options2(TSDB_OPTION option, const void * arg) {
        return taos_options(option, arg);
}
*/
import "C"
import (
	"unsafe"
	"errors"
	"io"
	"fmt"
)

func (tc *taos) taosConnect(ip, user, pass, db string, port uint32) error {
	cuser := C.CString(user)
	cpass := C.CString(pass)
	cip := C.CString(ip)
	cdb := C.CString(db)
	defer C.free(unsafe.Pointer(cip))
	defer C.free(unsafe.Pointer(cuser))
	defer C.free(unsafe.Pointer(cpass))
	defer C.free(unsafe.Pointer(cdb))
	taosObj := C.taos_connect(cip, cuser, cpass, cdb, (C.ushort)(port))
	if taosObj == nil {
		errStr := C.GoString(C.taos_errstr(taosObj))
		return errors.New("taos_connect() fail!" + errStr)
	}

	tc.taos = unsafe.Pointer(taosObj)
	return nil
}

func (tc *taos) taosClose() {
	C.taos_free_result(tc.result)
	tc.result = nil
	C.taos_close(tc.taos)
	tc.taos = nil
}

func (tc *taos) taosError() {
	// free local resouce: allocated memory/metric-meta refcnt
	//var pRes unsafe.Pointer
	C.taos_free_result(tc.result)
	tc.result = nil
}

func (tc *taos) taosSubscribe(restart int, topic, sql string, fp, param unsafe.Pointer, interval int) error{
	tc.taos_sub = unsafe.Pointer(C.taos_subscribe(tc.taos, C.int(restart),  C.CString(topic), C.CString(sql), C.TAOS_SUBSCRIBE_CALLBACK(fp), param,  C.int(interval)))
	if tc.taos_sub == nil{
		return errors.New("taos subscribe failed!")
	}
	return nil
}

func (tc *taos) taosConsume() {
	tc.result = unsafe.Pointer(C.taos_consume(tc.taos_sub))
}

func (tc *taos) taosUnSubscribe(keepProgress int) error {
	if tc.taos_sub == nil {
		return errors.New("taos unsubscribe failed, errinfo: taos_sub object is nil")
	}
	C.taos_unsubscribe(tc.taos_sub, C.int(keepProgress))
	return nil
}

func (tc *taos) taosConfigPath(path string) error {
	n := C.taos_options2(C.TSDB_OPTION_CONFIGDIR, unsafe.Pointer(C.CString(path)))
	if n < 0 {
		return errors.New("taos set config file path failed")
	}
	return nil
}

func (tc *taos) readColumns(count int) ([]taosSqlField, error) {

	tc.rows = make([]taosSqlField, count)

	if tc.result == nil {
		return nil, errors.New("invalid result")
	}

	pFields := (*C.struct_taosField)(C.taos_fetch_fields(tc.result))

	// TODO: Optimized rewriting !!!!
	fields := (*[1 << 30]C.struct_taosField)(unsafe.Pointer(pFields))

	for i := 0; i < count; i++ {
		//columns[i].tableName = ms.taos.
		//fmt.Println(reflect.TypeOf(fields[i].name))
		var charray []byte
		for j := 0; j < len(fields[i].name); j++ {
			//fmt.Println("fields[i].name[j]: ", fields[i].name[j])
			if fields[i].name[j] != 0 {
				charray = append(charray, byte(fields[i].name[j]))
			} else {
				break
			}
		}
		tc.rows[i].name = string(charray)
		tc.rows[i].length = (uint32)(fields[i].bytes)
		tc.rows[i].fieldType = fieldType(fields[i]._type)
		tc.rows[i].flags = 0
		// columns[i].decimals  = 0
		//columns[i].charSet    = 0
	}
	return tc.rows, nil
}

func (tc *taos) readRow(dest []interface{}) error {
	if tc == nil {
		return io.EOF
	}

	if tc.result == nil {
		return errors.New("result is nil!")
	}

	//var row *unsafe.Pointer
	row := C.taos_fetch_row(tc.result)
	if row == nil {
		//C.taos_free_result(tc.result)
		tc.result = nil
		return io.EOF
	}

	length := C.taos_fetch_lengths(tc.result);
	// because sizeof(void*)  == sizeof(int*) == 8
	// notes: sizeof(int) == 8 in go, but sizeof(int) == 4 in C.
	for i := range dest {
		currentRow := (unsafe.Pointer)(uintptr(*((*int)(unsafe.Pointer(uintptr(unsafe.Pointer(row)) + uintptr(i)*unsafe.Sizeof(int(0)))))))

		if currentRow == nil {
			dest[i] = nil
			continue
		}

		switch tc.rows[i].fieldType {
		case C.TSDB_DATA_TYPE_BOOL:
			if (*((*byte)(currentRow))) != 0 {
				dest[i] = true
			} else {
				dest[i] = false
			}
			break

		case C.TSDB_DATA_TYPE_TINYINT:
			dest[i] = (int)(*((*byte)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_SMALLINT:
			dest[i] = (int16)(*((*int16)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_INT:
			dest[i] = (int)(*((*int32)(currentRow))) // notes int32 of go <----> int of C
			break

		case C.TSDB_DATA_TYPE_BIGINT:
			dest[i] = (int64)(*((*int64)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_FLOAT:
			dest[i] = (*((*float32)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_DOUBLE:
			dest[i] = (*((*float64)(currentRow)))
			break

		case C.TSDB_DATA_TYPE_BINARY, C.TSDB_DATA_TYPE_NCHAR:
			//charLen := tc.rows[i].length
			charLen := *((*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(length)) + uintptr(i)*unsafe.Sizeof(int32(0)))))
			var index int32
			binaryVal := make([]byte, charLen)
			for index = 0; index < charLen; index++ {
				binaryVal[index] = *((*byte)(unsafe.Pointer(uintptr(currentRow) + uintptr(index))))
			}
			dest[i] = string(binaryVal[:])
			break

		case C.TSDB_DATA_TYPE_TIMESTAMP:
			dest[i] = (int64)(*((*int64)(currentRow)))
			break

		default:
			fmt.Println("default fieldType: set dest[] to nil")
			dest[i] = nil
			break
		}
	}

	return nil
}

func (tc *taos) getRow(num int) ([][]interface{}, error) {
	if tc.result == nil {
		return nil, nil
	}

	var err error
	tc.rows, err = tc.readColumns(num)
	if err != nil {
		fmt.Println("GetRow readColumns failed, errinfo:", err)
		return nil, err
	}

	result := make([][]interface{}, 0)
	for{
		if tc.result == nil {
			break
		}
		tmp := make([]interface{}, num)
		err = tc.readRow(tmp)
		if err == io.EOF{
			break
		}

		if err != nil {
			fmt.Println("GetRow readRow failed, errinfo:", err)
		}
		result = append(result, tmp)
	}

	return result, nil
}

func (tc *taos) taos_error() {
	// free local resouce: allocated memory/metric-meta refcnt
	//var pRes unsafe.Pointer
	C.taos_free_result(tc.result)
	tc.result = nil
}

func (tc *taos) exec(sqlstr string) (int, error) {
	//csqlstr := (*UserChar)(unsafe.Pointer(&sqlstr))

	csqlstr := C.CString(sqlstr)
	defer C.free(unsafe.Pointer(csqlstr))
	if tc.result != nil {
		C.taos_free_result(tc.result)
		tc.result = nil
	}
	tc.result = unsafe.Pointer(C.taos_query(tc.taos, csqlstr))
	//mc.result = unsafe.Pointer(C.taos_query_c(mc.taos, csqlstr.Str, C.uint32_t(csqlstr.Len)))
	code := C.taos_errno(tc.result)
	if 0 != code {
		errStr := C.GoString(C.taos_errstr(tc.result))
		tc.taos_error()
		return 0, errors.New(errStr)
	}

	// read result and save into mc struct
	num_fields := int(C.taos_field_count(tc.result))
	if 0 == num_fields { // there are no select and show kinds of commands
		tc.affectedRows = int(C.taos_affected_rows(tc.result))
		tc.insertId = 0
		return tc.affectedRows, nil
	}

	return num_fields, nil
}
