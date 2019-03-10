package main

import "github.com/diapco/votecube-crud/deserialize"

type RequestBatch struct {
	Data []*deserialize.CreatePollRequest
	Add  chan *deserialize.CreatePollRequest
}

func (batch *RequestBatch) accept() {
	for request := range batch.Add {
		batch.Data = append(batch.Data, request)
	}
}
