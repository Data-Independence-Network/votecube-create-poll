package main

import (
	"database/sql"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"strconv"
	"strings"
)

func ReplaceSQL(stmt, pattern string, len int) string {
	pattern += ","
	stmt = fmt.Sprintf(stmt, strings.Repeat(pattern, len))
	n := 0
	for strings.IndexByte(stmt, '?') != -1 {
		n++
		param := "$" + strconv.Itoa(n)
		stmt = strings.Replace(stmt, "?", param, 1)
	}
	return strings.TrimSuffix(stmt, ",")
}

func denyBatch(
	batch *RequestBatch,
	err error,
	db *sql.DB,
	tx *sql.Tx,
) {
	if err != nil {
		log.Fatal(err)
	}

	for _, request := range batch.Data {
		if request == nil {
			continue
		}
		request.Ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		request.Done <- true
	}

	if tx != nil {
		tx.Rollback()
	}

	if db != nil {
		db.Close()
	}
}
