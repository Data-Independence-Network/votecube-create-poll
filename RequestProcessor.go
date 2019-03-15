package main

import (
	"database/sql"
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/deserialize/model/poll"
	"github.com/diapco/votecube-crud/models"
	"github.com/valyala/fasthttp"
	"time"
)

type RequestProcessor struct {
	batch *RequestBatch
}

func (proc *RequestProcessor) startProcessing(
	locMaps *deserialize.LocationMaps,
	themeMap *map[int64]models.Theme,
) {

	for range time.Tick(time.Second * 20) {
		lastBatch := proc.batch

		nextBatch := &RequestBatch{}
		nextBatch.accept()
		proc.batch = nextBatch

		proc.processRequestBatch(lastBatch, locMaps, themeMap)
	}

}

func (proc *RequestProcessor) processRequestBatch(
	batch *RequestBatch,
	locMaps *deserialize.LocationMaps,
	themeMap *map[int64]models.Theme,
) {
	time.Sleep(1 * time.Millisecond)
	close(batch.Add)

	if len(batch.Data) == 0 {
		return
	}

	db, err := sql.Open("postgres", `postgresql://root@localhost:26257/votecube?sslmode=disable`)
	if err != nil {
		denyBatch(batch, err, db, nil)
		return
	}

	idRefs, ctxMapByLabelName := deserializeCreatePollRequests(
		batch,
		locMaps,
		themeMap,
	)

	err = verifyAllIds(batch, &idRefs)
	if err != nil {
		denyBatch(batch, err, db, nil)
		return
	}

	err = VerifyLabels(db, batch, &ctxMapByLabelName)
	if err != nil {
		denyBatch(batch, err, db, nil)
		return
	}

	/**
	At this point:
	All invalid requests have been filtered out

	Next step:
	Populate New Sequences
	Bulk Create records
	Write the new sequences in response to notify the client
	*/

	recordArrays, validRequests := MoveRecordsToArrays(batch)

	err = AssignSeqValues(&recordArrays)
	if err != nil {
		denyBatch(batch, err, db, nil)
		return
	}

	PopulateResponseAndCrossReferences(&validRequests)

	err = InsertRecords(&recordArrays, db)
	if err != nil {
		denyBatch(batch, err, db, nil)
		return
	}

	db.Close()

	for _, request := range batch.Data {
		if request == nil {
			continue
		}

		request.Ctx.SetBody(*request.ResponseData)
		request.Ctx.SetStatusCode(fasthttp.StatusCreated)
		request.Done <- true
	}
}

func deserializeCreatePollRequests(
	batch *RequestBatch,
	locMaps *deserialize.LocationMaps,
	themeMap *map[int64]models.Theme,
) (deserialize.CreatePollIdReferences, map[string][]*deserialize.CreatePollDeserializeContext) {
	now := time.Now()

	tomorrow := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	idRefs := deserialize.CreatePollIdReferences{
		FactorPositionIdRefs: make(map[int64]map[int]*deserialize.CreatePollRequest),
		FactorIdRefs:         make(map[int64]map[int]*deserialize.CreatePollRequest),
		PositionIdRefs:       make(map[int64]map[int]*deserialize.CreatePollRequest),
		LabelIdRefs:          make(map[int64]map[int]*deserialize.CreatePollRequest),
	}

	var ctxMapByLabelName = make(map[string][]*deserialize.CreatePollDeserializeContext)

	for index, request := range batch.Data {
		var err error
		var aPoll models.Poll
		var cursor int64 = 0
		ctx := request.Ctx
		data := request.Ctx.PostBody()
		var dataLen = int64(len(data))

		request.Index = index

		zCtx := deserialize.CreatePollDeserializeContext{
			IdRefs:            &idRefs,
			Index:             index,
			LocMaps:           locMaps,
			CtxMapByLabelName: ctxMapByLabelName,
			ReqLocSets: &deserialize.LocationSets{
				ContinentSet: make(map[int64]bool),
				CountrySet:   make(map[int64]bool),
				StateSet:     make(map[int64]bool),
				TownSet:      make(map[int64]bool),
			},
			Request: request,
			RequestInput: deserialize.RequestInput{
				Cursor:  &cursor,
				Data:    &data,
				DataLen: dataLen,
			},
			RequestNewLabelMapByName: make(map[string]*models.PollsLabel),
			ThemeMap:                 themeMap,
			Tomorrow:                 tomorrow,
		}

		aPoll, err = poll.DeserializePoll(&zCtx, err)

		if dataLen != cursor {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.Done <- true
			batch.Data[index] = nil
			continue
		}

		if err != nil {
			// then override already written body
			//ctx.SetBody([]byte("this is completely new body contents"))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.Done <- true
			batch.Data[index] = nil
			continue
		}

		request.Poll = aPoll
	}

	return idRefs, ctxMapByLabelName
}
