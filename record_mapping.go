package main

import (
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/models"
	"github.com/diapco/votecube-crud/sequence"
	"github.com/diapco/votecube-crud/serialize"
	"time"
)

type RecordArrays struct {
	Polls               []*models.Poll
	PollContinents      []*models.PollsContinent
	PollCountries       []*models.PollsCountry
	PollStates          []*models.PollsState
	PollTowns           []*models.PollsTown
	PollFactorPositions []*models.PollsFactorsPosition
	FactorPositions     []*models.FactorPosition
	Factors             []*models.Factor
	Positions           []*models.Position
	PollLabels          []*models.PollsLabel
	Labels              []*models.Label
}

func MoveRecordsToArrays(
	batch *RequestBatch,
) (RecordArrays, []*deserialize.CreatePollRequest) {
	var validRequests []*deserialize.CreatePollRequest

	var polls []*models.Poll
	var pollContinents []*models.PollsContinent
	var pollCountries []*models.PollsCountry
	var pollStates []*models.PollsState
	var pollTowns []*models.PollsTown
	var pollFactorPositions []*models.PollsFactorsPosition
	var factorPositions []*models.FactorPosition
	var factors []*models.Factor
	var positions []*models.Position
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

		for _, pollFactorPosition := range poll.R.PollsFactorsPositions {
			pollFactorPositions = append(pollFactorPositions, pollFactorPosition)

			if pollFactorPosition.FactorPositionID == 0 {
				factorPosition := pollFactorPosition.R.FactorPosition
				factorPositions = append(factorPositions, factorPosition)

				if factorPosition.FactorID == 0 {
					factors = append(factors, factorPosition.R.Factor)
				}
				if factorPosition.PositionID == 0 {
					positions = append(positions, factorPosition.R.Position)
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

	return RecordArrays{
		Polls:               polls,
		PollContinents:      pollContinents,
		PollCountries:       pollCountries,
		PollStates:          pollStates,
		PollTowns:           pollTowns,
		PollFactorPositions: pollFactorPositions,
		FactorPositions:     factorPositions,
		Factors:             factors,
		Positions:           positions,
		PollLabels:          pollLabels,
		Labels:              labels,
	}, validRequests
}

func AssignSeqValues(
	recArrs *RecordArrays,
) error {
	pollSeqCursor, err := sequence.PollId.GetCursor(len(recArrs.Polls))
	if err != nil {
		return err
	}

	pollContinentSeqCursor, err := sequence.PollContinentId.GetCursor(len(recArrs.PollContinents))
	if err != nil {
		return err
	}

	pollCountrySeqCursor, err := sequence.PollCountryId.GetCursor(len(recArrs.PollCountries))
	if err != nil {
		return err
	}

	pollStateSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.PollStates))
	if err != nil {
		return err
	}

	pollTownSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.PollTowns))
	if err != nil {
		return err
	}

	pollFactorPositionSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.PollFactorPositions))
	if err != nil {
		return err
	}

	factorPositionSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.FactorPositions))
	if err != nil {
		return err
	}

	factorSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.Factors))
	if err != nil {
		return err
	}

	positionSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.Positions))
	if err != nil {
		return err
	}

	pollLabelSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.PollLabels))
	if err != nil {
		return err
	}

	labelSeqCursor, err := sequence.PollStateId.GetCursor(len(recArrs.Labels))
	if err != nil {
		return err
	}

	for _, poll := range recArrs.Polls {
		poll.PollID = pollSeqCursor.Next()
	}
	for _, pollContinent := range recArrs.PollContinents {
		pollContinent.PollContinentID = pollContinentSeqCursor.Next()
	}
	for _, pollCountry := range recArrs.PollCountries {
		pollCountry.PollCountryID = pollCountrySeqCursor.Next()
	}
	for _, pollState := range recArrs.PollStates {
		pollState.PollStateID = pollStateSeqCursor.Next()
	}
	for _, pollTown := range recArrs.PollTowns {
		pollTown.PollTownID = pollTownSeqCursor.Next()
	}
	for _, pollFactorPosition := range recArrs.PollFactorPositions {
		pollFactorPosition.PollFactorPositionID = pollFactorPositionSeqCursor.Next()
	}
	for _, factorPosition := range recArrs.FactorPositions {
		factorPosition.FactorPositionID = factorPositionSeqCursor.Next()
	}
	for _, factor := range recArrs.Factors {
		factor.FactorID = factorSeqCursor.Next()
	}
	for _, position := range recArrs.Positions {
		position.PositionID = positionSeqCursor.Next()
	}
	for _, pollLabel := range recArrs.PollLabels {
		pollLabel.PollLabelID = pollLabelSeqCursor.Next()
	}
	for _, label := range recArrs.Labels {
		label.LabelID = labelSeqCursor.Next()
	}

	return nil
}

func PopulateResponseAndCrossReferences(
	validRequests *[]*deserialize.CreatePollRequest,
) {
	now := time.Now()
	for _, request := range *validRequests {
		if request == nil {
			continue
		}

		responseData := make([]byte, 0)
		request.ResponseData = &responseData

		poll := request.Poll
		pollId := poll.PollID

		poll.CreatedAt = now

		serialize.WNum(poll.PollID, request.ResponseData)

		for _, pollContinent := range poll.R.PollsContinents {
			pollContinent.PollID = pollId

			serialize.WNum(pollContinent.PollContinentID, request.ResponseData)
		}

		for _, pollCountry := range poll.R.PollsCountries {
			pollCountry.PollID = pollId

			serialize.WNum(pollCountry.PollCountryID, request.ResponseData)
		}

		for _, pollState := range poll.R.PollsStates {
			pollState.PollID = pollId

			serialize.WNum(pollState.PollStateID, request.ResponseData)
		}

		for _, pollTown := range poll.R.PollsTowns {
			pollTown.PollID = pollId

			serialize.WNum(pollTown.PollTownID, request.ResponseData)
		}

		for _, pollFactorPosition := range poll.R.PollsFactorsPositions {
			pollFactorPosition.PollID = pollId

			serialize.WNum(pollFactorPosition.PollFactorPositionID, request.ResponseData)

			factorPosition := pollFactorPosition.R.FactorPosition
			if factorPosition != nil {
				factorPosition.CreatedAt = now
				pollFactorPosition.FactorPositionID = factorPosition.FactorPositionID

				serialize.WNum(factorPosition.FactorPositionID, request.ResponseData)

				factor := factorPosition.R.Factor
				if factor != nil {
					factor.CreatedAt = now
					factorPosition.FactorID = factor.FactorID

					serialize.WNum(factor.FactorID, request.ResponseData)
				}

				position := factorPosition.R.Position
				if position != nil {
					position.CreatedAt = now
					factorPosition.PositionID = position.PositionID

					serialize.WNum(position.PositionID, request.ResponseData)
				}
			}
		}

		for _, pollLabel := range poll.R.PollsLabels {
			pollLabel.PollID = pollId

			serialize.WNum(pollLabel.PollLabelID, request.ResponseData)

			label := pollLabel.R.Label
			if label != nil {
				label.CreatedAt = now
				pollLabel.LabelID = label.LabelID

				serialize.WNum(label.LabelID, request.ResponseData)
			}
		}
	}
}
