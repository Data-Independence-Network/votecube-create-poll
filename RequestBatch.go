package main

import "github.com/diapco/votecube-crud/deserialize"

type RequestBatch struct {
	Data []deserialize.Request
	Add  chan deserialize.Request
}

func (batch *RequestBatch) accept() {
	for request := range batch.Add {
		batch.Data = append(batch.Data, request)
	}
}
