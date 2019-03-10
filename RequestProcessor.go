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

func (proc *RequestProcessor) startProcessing(locMaps *deserialize.LocationMaps) {

	for range time.Tick(time.Second * 20) {
		lastBatch := proc.batch

		nextBatch := &RequestBatch{}
		nextBatch.accept()
		proc.batch = nextBatch

		proc.processRequest(lastBatch, locMaps)
	}

}

func (proc *RequestProcessor) processRequest(batch *RequestBatch, locMaps *deserialize.LocationMaps) {
	time.Sleep(1 * time.Millisecond)
	close(batch.Add)

	if len(batch.Data) == 0 {
		return
	}

	idRefs := deserialize.IdReferences{
		DimDirIdRefs: make(map[int64]map[int]*deserialize.Request),
		DimIdRefs:    make(map[int64]map[int]*deserialize.Request),
		DirIdRefs:    make(map[int64]map[int]*deserialize.Request),
		LabelIdRefs:  make(map[int64]map[int]*deserialize.Request),
	}

	for index, request := range batch.Data {

		var err error
		var aPoll models.Poll
		var cursor int64 = 0
		ctx := request.Ctx
		data := request.Ctx.PostBody()
		var dataLen = int64(len(data))

		request.Index = index

		zCtx := deserialize.DeserializeContext{
			Cursor:  &cursor,
			Data:    &data,
			DataLen: dataLen,
			IdRefs:  &idRefs,
			LocMaps: locMaps,
			ReqLocSets: &deserialize.LocationSets{
				ContinentSet: make(map[int64]bool),
				CountrySet:   make(map[int64]bool),
				StateSet:     make(map[int64]bool),
				TownSet:      make(map[int64]bool),
			},
			Request: &request,
		}

		aPoll, err = poll.DeserializePoll(&zCtx, err)

		if dataLen != cursor {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.Done <- true
			continue
		}

		if err != nil {
			// then override already written body
			//ctx.SetBody([]byte("this is completely new body contents"))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.Done <- true
			continue
		}
	}
}
