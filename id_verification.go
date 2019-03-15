package main

import (
	"context"
	"database/sql"
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
	Query for Id references (Factor, FactorPosition, Position, Label)
	Check requests and verify that all ids exist
	Invalidate requests that reference invalid Ids
*/
func verifyAllIds(
	batch *RequestBatch,
	idRefs *deserialize.CreatePollIdReferences,
) error {

	db, err := sql.Open("postgres", `postgresql://root@localhost:26257/votecube?sslmode=disable`)
	if err != nil {
		return err
	}

	numIdVerificationDbRequests := 4
	idVerificationDbRequestsDone := make(chan error, numIdVerificationDbRequests)

	go verifyFactorIds(idRefs, db, idVerificationDbRequestsDone)
	go verifyFactorPositionIds(idRefs, db, idVerificationDbRequestsDone)
	go verifyPositionIds(idRefs, db, idVerificationDbRequestsDone)
	go verifyLabelIds(idRefs, db, idVerificationDbRequestsDone)

	numCompletedInitialDbRequests := 0
	for err = range idVerificationDbRequestsDone {
		if err != nil {
			return err
		}

		numCompletedInitialDbRequests++
		if numCompletedInitialDbRequests == numIdVerificationDbRequests {
			break
		}
	}

	denyRequestsWithInvalidIds(batch.Data, idRefs)

	return nil
}

func verifyFactorIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan error) {

	factorIds := make([]interface{}, len(idRefs.FactorIdRefs))
	for factorId := range idRefs.FactorIdRefs {
		factorIds = append(factorIds, factorId)
	}

	factors, err := models.Factors(
		qm.Select(models.FactorColumns.FactorID),
		qm.WhereIn(models.FactorColumns.FactorID+" in ?", factorIds),
	).All(context.Background(), db)

	if err != nil {
		done <- err
		return
	}

	for _, factor := range factors {
		delete(idRefs.FactorIdRefs, factor.FactorID)
	}

	done <- nil
}

func verifyFactorPositionIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan error) {

	factorPositionIds := make([]interface{}, len(idRefs.FactorPositionIdRefs))
	for factorPositionId := range idRefs.FactorPositionIdRefs {
		factorPositionIds = append(factorPositionIds, factorPositionId)
	}

	factorPositions, err := models.FactorPositions(
		qm.Select(models.FactorPositionColumns.FactorPositionID),
		qm.WhereIn(models.FactorPositionColumns.FactorPositionID+" in ?", factorPositionIds),
	).All(context.Background(), db)

	if err != nil {
		done <- err
		return
	}

	for _, factorPosition := range factorPositions {
		delete(idRefs.FactorPositionIdRefs, factorPosition.FactorPositionID)
	}

	done <- nil
}

func verifyPositionIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan error) {

	positionIds := make([]interface{}, len(idRefs.PositionIdRefs))
	for positionId := range idRefs.PositionIdRefs {
		positionIds = append(positionIds, positionId)
	}

	positions, err := models.Positions(
		qm.Select(models.PositionColumns.PositionID),
		qm.WhereIn(models.PositionColumns.PositionID+" in ?", positionIds),
	).All(context.Background(), db)

	if err != nil {
		done <- err
		return
	}

	for _, position := range positions {
		delete(idRefs.PositionIdRefs, position.PositionID)
	}

	done <- nil
}

func verifyLabelIds(
	idRefs *deserialize.CreatePollIdReferences,
	db *sql.DB,
	done chan error) {

	labelIds := make([]interface{}, len(idRefs.LabelIdRefs))
	for labelId := range idRefs.LabelIdRefs {
		labelIds = append(labelIds, labelId)
	}

	labels, err := models.Labels(
		qm.Select(models.LabelColumns.LabelID),
		qm.WhereIn(models.LabelColumns.LabelID+" in ?", labelIds),
	).All(context.Background(), db)

	if err != nil {
		done <- err
		return
	}

	for _, label := range labels {
		delete(idRefs.LabelIdRefs, label.LabelID)
	}

	done <- nil
}

func denyRequestsWithInvalidIds(
	data []*deserialize.CreatePollRequest,
	idRefs *deserialize.CreatePollIdReferences) {
	denyRequestsWithInvalidIdsForIdType(data, idRefs.FactorPositionIdRefs)
	denyRequestsWithInvalidIdsForIdType(data, idRefs.FactorIdRefs)
	denyRequestsWithInvalidIdsForIdType(data, idRefs.PositionIdRefs)
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
