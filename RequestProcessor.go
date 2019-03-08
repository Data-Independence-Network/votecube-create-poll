package main

import (
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/deserialize/model/poll"
	"github.com/diapco/votecube-crud/models"
	"github.com/valyala/fasthttp"
	"time"
)

type RequestProcessor struct {
	batch *RequestBatch
}

func (proc *RequestProcessor) startProcessing() {

	for range time.Tick(time.Second * 20) {
		lastBatch := proc.batch

		nextBatch := &RequestBatch{}
		nextBatch.accept()
		proc.batch = nextBatch

		proc.processRequest(lastBatch)
	}

}

func (proc *RequestProcessor) processRequest(batch *RequestBatch) {
	time.Sleep(1 * time.Millisecond)
	close(batch.Add)

	if len(batch.Data) == 0 {
		return
	}

	for _, request := range batch.Data {

		var err error
		var aPoll models.Poll
		var cursor int64 = 0
		ctx := request.ctx
		data := request.ctx.PostBody()
		var dataLen = int64(len(data))

		aPoll, err = poll.DeserializePoll(&deserialize.DeserializeContext{
			&cursor, &data, dataLen, nil, nil, nil, 1,
		}, err)

		if dataLen != cursor {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.done <- true
			continue
		}

		if err != nil {
			// then override already written body
			//ctx.SetBody([]byte("this is completely new body contents"))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.done <- true
			continue
		}
	}
}
