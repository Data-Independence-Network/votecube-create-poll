package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/deserialize/model/poll"
	"github.com/diapco/votecube-crud/models"
	"github.com/valyala/fasthttp"
	"github.com/volatiletech/sqlboiler/queries/qm"
	"log"
	"strconv"
	"strings"
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

	var labelNames = make([]interface{}, len(ctxMapByLabelName))
	i := 0
	for labelName, _ := range ctxMapByLabelName {
		labelNames[i] = labelName
		i++
	}

	existingLabels, err := models.Labels(
		qm.Select(models.LabelColumns.LabelID, models.LabelColumns.Name),
		qm.WhereIn(models.LabelColumns.Name+" in ?", labelNames...),
	).All(context.Background(), db)

	if err != nil {
		denyBatch(batch, err)
		return
	}

	for _, existingLabel := range existingLabels {
		for _, ctx := range ctxMapByLabelName[existingLabel.Name] {
			_, labelAlreadySpecified := ctx.IdRefs.LabelIdRefs[existingLabel.LabelID]
			if labelAlreadySpecified {
				request := batch.Data[ctx.Index]
				if request == nil {
					continue
				}
				request.Ctx.SetStatusCode(fasthttp.StatusBadRequest)
				request.Done <- true
				batch.Data[ctx.Index] = nil
			}
			pollLabel, _ := ctx.RequestNewLabelMapByName[existingLabel.Name]
			pollLabel.R.Label = nil
			pollLabel.LabelID = existingLabel.LabelID
		}
	}

	verifyAllIds(batch, &idRefs)

	for _, request := range batch.Data {
		if request == nil {
			continue
		}
		pollsLabels := request.Poll.R.PollsLabels
		if pollsLabels != nil {
			continue
		}
		for _, pollLabel := range pollsLabels {
			pollLabels = append(pollLabels, pollLabel)

			if pollLabel.LabelID == 0 {

				labels = append(labels, pollLabel.R.Label)
			}
		}
	}

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

	pollSeqCursor := db.PollId.GetCursor(numPolls)

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
) {
	log.Fatal(err)
	for _, request := range batch.Data {
		if request == nil {
			continue
		}
		request.Ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		request.Done <- true
	}
}
