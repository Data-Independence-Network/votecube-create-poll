package main

import (
	"database/sql"
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/deserialize/model/poll"
	"github.com/diapco/votecube-crud/models"
	"github.com/diapco/votecube-crud/sequence"
	"github.com/diapco/votecube-crud/serialize"
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
		denyBatch(batch, err)
		return
	}

	now := time.Now()

	tomorrow := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	idRefs := deserialize.CreatePollIdReferences{
		DimDirIdRefs: make(map[int64]map[int]*deserialize.CreatePollRequest),
		DimIdRefs:    make(map[int64]map[int]*deserialize.CreatePollRequest),
		DirIdRefs:    make(map[int64]map[int]*deserialize.CreatePollRequest),
		LabelIdRefs:  make(map[int64]map[int]*deserialize.CreatePollRequest),
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

	verifyAllIds(batch, &idRefs)

	VerifyLabels(db, batch, &ctxMapByLabelName)

	/**
	At this point:
	All invalid requests have been filtered out

	Next step:
	Populate New Sequences
	Bulk Create records
	Write the new sequences in response to notify the client
	*/

	var validRequests []*deserialize.CreatePollRequest
	//i := -1

	var polls []*models.Poll
	var pollContinents []*models.PollsContinent
	var pollCountries []*models.PollsCountry
	var pollStates []*models.PollsState
	var pollTowns []*models.PollsTown
	var pollDimDirs []*models.PollsDimensionsDirection
	var dimensionDirections []*models.DimensionDirection
	var dimensions []*models.Dimension
	var directions []*models.Direction
	var pollLabels []*models.PollsLabel
	var labels []*models.Label

	for _, request := range batch.Data {
		if request == nil {
			continue
		}
		validRequests = append(validRequests, request)
		//i = i + 1
		//request.Index = i

		poll := request.Poll
		polls = append(polls, &poll)

		for _, pollContinent := range poll.R.PollsContinents {
			pollContinents = append(pollContinents, pollContinent)
		}
		pollsCountries := poll.R.PollsCountries
		if pollsCountries != nil {
			for _, pollCountry := range pollsCountries {
				pollCountries = append(pollCountries, pollCountry)
			}
		}
		pollsStates := poll.R.PollsStates
		if pollsStates != nil {
			for _, pollState := range pollsStates {
				pollStates = append(pollStates, pollState)
			}
		}
		pollsTowns := poll.R.PollsTowns
		if pollsTowns != nil {
			for _, pollTown := range pollsTowns {
				pollTowns = append(pollTowns, pollTown)
			}
		}

		for _, pollDimDir := range poll.R.PollsDimensionsDirections {
			pollDimDirs = append(pollDimDirs, pollDimDir)

			if pollDimDir.DimensionDirectionID == 0 {
				dimensionDirection := pollDimDir.R.DimensionDirection
				dimensionDirections = append(dimensionDirections, dimensionDirection)

				if dimensionDirection.DimensionID == 0 {
					dimensions = append(dimensions, dimensionDirection.R.Dimension)
				}
				if dimensionDirection.DirectionID == 0 {
					directions = append(directions, dimensionDirection.R.Direction)
				}
			}
		}

		pollsLabels := poll.R.PollsLabels
		if pollsLabels != nil {
			for _, pollLabel := range pollsLabels {
				pollLabels = append(pollLabels, pollLabel)

				if pollLabel.LabelID == 0 {

					labels = append(labels, pollLabel.R.Label)
				}
			}
		}
	}

	numPolls := len(validRequests)

	pollSeqCursor := sequence.PollId.GetCursor(len(polls))
	pollContinentSeqCursor := sequence.PollContinentId.GetCursor(len(pollContinents))
	pollCountrySeqCursor := sequence.PollCountryId.GetCursor(len(pollCountries))
	pollStateSeqCursor := sequence.PollStateId.GetCursor(len(pollStates))
	pollTownSeqCursor := sequence.PollStateId.GetCursor(len(pollTowns))
	pollDimDirSeqCursor := sequence.PollStateId.GetCursor(len(pollDimDirs))
	dimDirSeqCursor := sequence.PollStateId.GetCursor(len(dimensionDirections))
	dimensionSeqCursor := sequence.PollStateId.GetCursor(len(dimensions))
	directionSeqCursor := sequence.PollStateId.GetCursor(len(directions))
	pollLabelSeqCursor := sequence.PollStateId.GetCursor(len(pollLabels))
	labelSeqCursor := sequence.PollStateId.GetCursor(len(labels))

	for _, poll := range polls {
		poll.PollID = pollSeqCursor.Next()
	}
	for _, pollContinent := range pollContinents {
		pollContinent.PollContinentID = pollContinentSeqCursor.Next()
	}
	for _, pollCountry := range pollCountries {
		pollCountry.PollCountryID = pollCountrySeqCursor.Next()
	}
	for _, pollState := range pollStates {
		pollState.PollStateID = pollStateSeqCursor.Next()
	}
	for _, pollTown := range pollTowns {
		pollTown.PollTownID = pollTownSeqCursor.Next()
	}
	for _, pollDimDir := range pollDimDirs {
		pollDimDir.PollDimensionDirectionID = pollDimDirSeqCursor.Next()
	}
	for _, dimensionDirection := range dimensionDirections {
		dimensionDirection.DimensionDirectionID = dimDirSeqCursor.Next()
	}
	for _, dimension := range dimensions {
		dimension.DimensionID = dimensionSeqCursor.Next()
	}
	for _, direction := range directions {
		direction.DirectionID = directionSeqCursor.Next()
	}
	for _, pollLabel := range pollLabels {
		pollLabel.PollLabelID = pollLabelSeqCursor.Next()
	}
	for _, label := range labels {
		label.LabelID = labelSeqCursor.Next()
	}

	pollValues := []interface{}{}
	pollContinentValues := []interface{}{}
	pollCountryValues := []interface{}{}
	pollStateValues := []interface{}{}
	pollTownValues := []interface{}{}
	pollDimDirValues := []interface{}{}
	dimDirValues := []interface{}{}
	dimensionValues := []interface{}{}
	directionValues := []interface{}{}
	pollLabelValues := []interface{}{}
	labelValues := []interface{}{}

	pollSqlStr := "INSERT INTO " + models.TableNames.Polls + "(" +
		models.PollColumns.PollID + ", " +
		models.PollColumns.PollDescription +
		" ) VALUES %s"

	for _, request := range batch.Data {
		if request == nil {
			continue
		}

		request.ResponseData = append(request.ResponseData, serialize.WNum(request.Poll.PollID))
		request.Poll
	}

	vals := []interface{}{}
	for _, row := range data {
		vals = append(vals, row.FirstName, row.LastName, row.Age)
	}

	sqlStr := `INSERT INTO test(column1, column2, column3) VALUES %s`
	sqlStr = ReplaceSQL(sqlStr, "(?, ?, ?)", len(data))

	//Prepare and execute the statement
	stmt, _ := db.Prepare(sqlStr)
	res, _ := stmt.Exec(vals...)
}
