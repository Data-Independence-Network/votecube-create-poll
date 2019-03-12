package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/models"
	"github.com/valyala/fasthttp"
	"github.com/volatiletech/sqlboiler/queries/qm"
)

/*
At this point:

Data has been deserialized & structurally verified
Locations have been verified (including their internal relations)
Id References have been identified

Next step:
	Query for Id references (Dim, DimDir, Dir, Label)
	Check requests and verify that all ids exist
	Invalidate requests that reference invalid Ids
*/
func verifyAllIds(
	batch *RequestBatch,
	idRefs *deserialize.CreatePollIdReferences) {

	db, err := sql.Open("postgres", `postgresql://root@localhost:26257/votecube?sslmode=disable`)
	if err != nil {
		panic(err)
	}
	numIdVerificationDbRequests := 4
	idVerificationDbRequestsDone := make(chan bool, numIdVerificationDbRequests)

	go verifyDimensionIds(idRefs, db, idVerificationDbRequestsDone)
	go verifyDimensionDirectionIds(idRefs, db, idVerificationDbRequestsDone)
	go verifyDirectionIds(idRefs, db, idVerificationDbRequestsDone)
	go verifyLabelIds(idRefs, db, idVerificationDbRequestsDone)

	numCompletedInitialDbRequests := 0
	for range idVerificationDbRequestsDone {
		numCompletedInitialDbRequests++
		if numCompletedInitialDbRequests == numIdVerificationDbRequests {
			break
		}
	}

	denyRequestsWithInvalidIds(batch.Data, idRefs)
}

func verifyDimensionIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan bool) {

	dimIds := make([]interface{}, len(idRefs.DimIdRefs))
	for dimId := range idRefs.DimIdRefs {
		dimIds = append(dimIds, dimId)
	}

	dimensions, err := models.Dimensions(
		qm.Select(models.DimensionColumns.DimensionID),
		qm.WhereIn(models.DimensionColumns.DimensionID+" in ?", dimIds),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Dimensions")
		panic(err)
	}

	for _, dimension := range dimensions {
		delete(idRefs.DimIdRefs, dimension.DimensionID)
	}

	done <- true
}

func verifyDimensionDirectionIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan bool) {

	dimDirIds := make([]interface{}, len(idRefs.DimDirIdRefs))
	for dimDirId := range idRefs.DimDirIdRefs {
		dimDirIds = append(dimDirIds, dimDirId)
	}

	dimensionDirections, err := models.DimensionDirections(
		qm.Select(models.DimensionDirectionColumns.DimensionDirectionID),
		qm.WhereIn(models.DimensionDirectionColumns.DimensionDirectionID+" in ?", dimDirIds),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying DimensionDirections")
		panic(err)
	}

	for _, dimensionDirection := range dimensionDirections {
		delete(idRefs.DimDirIdRefs, dimensionDirection.DimensionDirectionID)
	}

	done <- true
}

func verifyDirectionIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan bool) {

	dirIds := make([]interface{}, len(idRefs.DirIdRefs))
	for dirId := range idRefs.DirIdRefs {
		dirIds = append(dirIds, dirId)
	}

	directions, err := models.Directions(
		qm.Select(models.DirectionColumns.DirectionID),
		qm.WhereIn(models.DirectionColumns.DirectionID+" in ?", dirIds),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Directions")
		panic(err)
	}

	for _, direction := range directions {
		delete(idRefs.DirIdRefs, direction.DirectionID)
	}

	done <- true
}

func verifyLabelIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan bool) {

	labelIds := make([]interface{}, len(idRefs.LabelIdRefs))
	for labelId := range idRefs.LabelIdRefs {
		labelIds = append(labelIds, labelId)
	}

	labels, err := models.Labels(
		qm.Select(models.LabelColumns.LabelID),
		qm.WhereIn(models.LabelColumns.LabelID+" in ?", labelIds),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Labels")
		panic(err)
	}

	for _, label := range labels {
		delete(idRefs.LabelIdRefs, label.LabelID)
	}

	done <- true
}

func denyRequestsWithInvalidIds(
	data []*deserialize.CreatePollRequest,
	idRefs *deserialize.CreatePollIdReferences) {
	denyRequestsWithInvalidIdsForIdType(data, idRefs.DimDirIdRefs)
	denyRequestsWithInvalidIdsForIdType(data, idRefs.DimIdRefs)
	denyRequestsWithInvalidIdsForIdType(data, idRefs.DirIdRefs)
	denyRequestsWithInvalidIdsForIdType(data, idRefs.LabelIdRefs)
}

func denyRequestsWithInvalidIdsForIdType(
	data []*deserialize.CreatePollRequest,
	requestMapByIndexAndId map[int64]map[int]*deserialize.CreatePollRequest) {
	for _, createPollRequestMap := range requestMapByIndexAndId {
		for index, request := range createPollRequestMap {
			if data[index] == nil {
				continue
			}

			data[index] = nil

			request.Ctx.SetStatusCode(fasthttp.StatusBadRequest)
			request.Done <- true
		}
	}
}
