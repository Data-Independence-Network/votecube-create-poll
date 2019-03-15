package main

import (
	"database/sql"
	"github.com/diapco/votecube-crud/models"
)

func InsertRecords(
	recArrs *RecordArrays,
	db *sql.DB,
) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = InsertPolls(recArrs.Polls, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPollContinents(recArrs.PollContinents, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPollCountries(recArrs.PollCountries, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPollStates(recArrs.PollStates, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPollTowns(recArrs.PollTowns, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertFactors(recArrs.Factors, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPositions(recArrs.Positions, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertFactorPositions(recArrs.FactorPositions, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPollFactorPositions(recArrs.PollFactorPositions, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertLabels(recArrs.Labels, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = InsertPollLabels(recArrs.PollLabels, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func InsertPolls(
	polls []*models.Poll,
	tx *sql.Tx,
) (sql.Result, error) {
	pollValues := []interface{}{}

	pollSqlStr := "INSERT INTO " + models.TableNames.Polls + "(" +
		models.PollColumns.PollID + ", " +
		models.PollColumns.PollDescription + ", " +
		models.PollColumns.StartDate + ", " +
		models.PollColumns.EndDate + ", " +
		models.PollColumns.ThemeID +
		" ) VALUES %s"

	for _, poll := range polls {
		pollValues = append(
			pollValues,
			poll.PollID,
			poll.PollDescription,
			poll.StartDate,
			poll.EndDate,
			poll.ThemeID,
		)
	}

	pollSqlStr = ReplaceSQL(pollSqlStr, "(?, ?, ?, ?, ?)", len(pollValues))
	stmt, _ := tx.Prepare(pollSqlStr)

	return stmt.Exec(pollValues...)
}

func InsertPollContinents(
	pollContinents []*models.PollsContinent,
	tx *sql.Tx,
) (sql.Result, error) {
	pollContinentValues := []interface{}{}

	pollContinentSqlStr := "INSERT INTO " + models.TableNames.PollsContinent + "(" +
		models.PollsContinentColumns.PollContinentID + ", " +
		models.PollsContinentColumns.PollID + ", " +
		models.PollsContinentColumns.ContinentID +
		" ) VALUES %s"

	for _, pollContinent := range pollContinents {
		pollContinentValues = append(
			pollContinentValues,
			pollContinent.PollContinentID,
			pollContinent.PollID,
			pollContinent.ContinentID,
		)
	}

	pollContinentSqlStr = ReplaceSQL(pollContinentSqlStr, "(?, ?, ?)", len(pollContinentValues))
	stmt, _ := tx.Prepare(pollContinentSqlStr)

	return stmt.Exec(pollContinentValues...)
}

func InsertPollCountries(
	pollCountries []*models.PollsCountry,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(pollCountries) == 0 {
		return nil, nil
	}

	pollCountryValues := []interface{}{}

	pollCountrySqlStr := "INSERT INTO " + models.TableNames.PollsCountry + "(" +
		models.PollsCountryColumns.PollCountryID + ", " +
		models.PollsCountryColumns.PollID + ", " +
		models.PollsCountryColumns.CountryID +
		" ) VALUES %s"

	for _, pollCountry := range pollCountries {
		pollCountryValues = append(
			pollCountryValues,
			pollCountry.PollCountryID,
			pollCountry.PollID,
			pollCountry.CountryID,
		)
	}

	pollCountrySqlStr = ReplaceSQL(pollCountrySqlStr, "(?, ?, ?)", len(pollCountryValues))
	stmt, _ := tx.Prepare(pollCountrySqlStr)

	return stmt.Exec(pollCountryValues...)
}

func InsertPollStates(
	pollStates []*models.PollsState,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(pollStates) == 0 {
		return nil, nil
	}

	pollStateValues := []interface{}{}

	pollStateSqlStr := "INSERT INTO " + models.TableNames.PollsState + "(" +
		models.PollsStateColumns.PollStateID + ", " +
		models.PollsStateColumns.PollID + ", " +
		models.PollsStateColumns.StateID +
		" ) VALUES %s"

	for _, pollState := range pollStates {
		pollStateValues = append(
			pollStateValues,
			pollState.PollStateID,
			pollState.PollID,
			pollState.StateID,
		)
	}

	pollStateSqlStr = ReplaceSQL(pollStateSqlStr, "(?, ?, ?)", len(pollStateValues))
	stmt, _ := tx.Prepare(pollStateSqlStr)

	return stmt.Exec(pollStateValues...)
}

func InsertPollTowns(
	pollTowns []*models.PollsTown,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(pollTowns) == 0 {
		return nil, nil
	}

	pollTownValues := []interface{}{}

	pollTownSqlStr := "INSERT INTO " + models.TableNames.PollsTown + "(" +
		models.PollsTownColumns.PollTownID + ", " +
		models.PollsTownColumns.PollID + ", " +
		models.PollsTownColumns.TownID +
		" ) VALUES %s"

	for _, pollTown := range pollTowns {
		pollTownValues = append(
			pollTownValues,
			pollTown.PollTownID,
			pollTown.PollID,
			pollTown.TownID,
		)
	}

	pollTownSqlStr = ReplaceSQL(pollTownSqlStr, "(?, ?, ?)", len(pollTownValues))
	stmt, _ := tx.Prepare(pollTownSqlStr)

	return stmt.Exec(pollTownValues...)
}

func InsertPollFactorPositions(
	pollFactorPositions []*models.PollsFactorsPosition,
	tx *sql.Tx,
) (sql.Result, error) {
	pollFactorPositionValues := []interface{}{}

	pollFactorPositionSqlStr := "INSERT INTO " + models.TableNames.PollsFactorsPositions + "(" +
		models.PollsFactorsPositionColumns.PollFactorPositionID + ", " +
		models.PollsFactorsPositionColumns.PollID + ", " +
		models.PollsFactorsPositionColumns.FactorPositionID + ", " +
		models.PollsFactorsPositionColumns.FactorCoordinateAxis + ", " +
		models.PollsFactorsPositionColumns.PositionOrientation + ", " +
		models.PollsFactorsPositionColumns.ColorID +
		" ) VALUES %s"

	for _, pollFactorPosition := range pollFactorPositions {
		pollFactorPositionValues = append(
			pollFactorPositionValues,
			pollFactorPosition.PollFactorPositionID,
			pollFactorPosition.PollID,
			pollFactorPosition.FactorPositionID,
			pollFactorPosition.FactorCoordinateAxis,
			pollFactorPosition.PositionOrientation,
			pollFactorPosition.ColorID,
		)
	}

	pollFactorPositionSqlStr = ReplaceSQL(pollFactorPositionSqlStr, "(?, ?, ?, ?, ?, ?)", len(pollFactorPositionValues))
	stmt, _ := tx.Prepare(pollFactorPositionSqlStr)

	return stmt.Exec(pollFactorPositionValues...)
}

func InsertFactorPositions(
	factorPositions []*models.FactorPosition,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(factorPositions) == 0 {
		return nil, nil
	}

	factorPositionValues := []interface{}{}

	factorPositionSqlStr := "INSERT INTO " + models.TableNames.FactorPositions + "(" +
		models.FactorPositionColumns.FactorPositionID + ", " +
		models.FactorPositionColumns.FactorID + ", " +
		models.FactorPositionColumns.PositionID + ", " +
		models.FactorPositionColumns.UserAccountID + ", " +
		models.FactorPositionColumns.CreatedAt +
		" ) VALUES %s"

	for _, factorPosition := range factorPositions {
		factorPositionValues = append(
			factorPositionValues,
			factorPosition.FactorPositionID,
			factorPosition.FactorID,
			factorPosition.PositionID,
			factorPosition.UserAccountID,
			factorPosition.CreatedAt,
		)
	}

	factorPositionSqlStr = ReplaceSQL(factorPositionSqlStr, "(?, ?, ?, ?, ?)", len(factorPositionValues))
	stmt, _ := tx.Prepare(factorPositionSqlStr)

	return stmt.Exec(factorPositionValues...)
}

func InsertFactors(
	factors []*models.Factor,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(factors) == 0 {
		return nil, nil
	}

	factorValues := []interface{}{}

	factorSqlStr := "INSERT INTO " + models.TableNames.Factors + "(" +
		models.FactorColumns.FactorID + ", " +
		models.FactorColumns.FactorName + ", " +
		models.FactorColumns.ParentFactorID + ", " +
		models.FactorColumns.UserAccountID + ", " +
		models.FactorColumns.CreatedAt +
		" ) VALUES %s"

	for _, factorPosition := range factors {
		factorValues = append(
			factorValues,
			factorPosition.FactorID,
			factorPosition.FactorName,
			factorPosition.ParentFactorID,
			factorPosition.UserAccountID,
			factorPosition.CreatedAt,
		)
	}

	factorSqlStr = ReplaceSQL(factorSqlStr, "(?, ?, ?, ?, ?)", len(factorValues))
	stmt, _ := tx.Prepare(factorSqlStr)

	return stmt.Exec(factorValues...)
}

func InsertPositions(
	positions []*models.Position,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(positions) == 0 {
		return nil, nil
	}

	positionValues := []interface{}{}

	positionSqlStr := "INSERT INTO " + models.TableNames.Positions + "(" +
		models.PositionColumns.PositionID + ", " +
		models.PositionColumns.PositionDescription + ", " +
		models.PositionColumns.ParentPositionID + ", " +
		models.PositionColumns.UserAccountID + ", " +
		models.PositionColumns.CreatedAt +
		" ) VALUES %s"

	for _, factorPosition := range positions {
		positionValues = append(
			positionValues,
			factorPosition.PositionID,
			factorPosition.PositionDescription,
			factorPosition.ParentPositionID,
			factorPosition.UserAccountID,
			factorPosition.CreatedAt,
		)
	}

	positionSqlStr = ReplaceSQL(positionSqlStr, "(?, ?, ?, ?, ?)", len(positionValues))
	stmt, _ := tx.Prepare(positionSqlStr)

	return stmt.Exec(positionValues...)
}

func InsertPollLabels(
	pollLabels []*models.PollsLabel,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(pollLabels) == 0 {
		return nil, nil
	}

	pollLabelValues := []interface{}{}

	pollLabelSqlStr := "INSERT INTO " + models.TableNames.PollsLabels + "(" +
		models.PollsLabelColumns.PollLabelID + ", " +
		models.PollsLabelColumns.PollID + ", " +
		models.PollsLabelColumns.LabelID + ", " +
		models.PollsLabelColumns.UserAccountID + ", " +
		models.PollsLabelColumns.CreatedAt +
		" ) VALUES %s"

	for _, pollLabel := range pollLabels {
		pollLabelValues = append(
			pollLabelValues,
			pollLabel.PollLabelID,
			pollLabel.PollID,
			pollLabel.LabelID,
			pollLabel.UserAccountID,
			pollLabel.CreatedAt,
		)
	}

	pollLabelSqlStr = ReplaceSQL(pollLabelSqlStr, "(?, ?, ?, ?, ?)", len(pollLabelValues))
	stmt, _ := tx.Prepare(pollLabelSqlStr)

	return stmt.Exec(pollLabelValues...)
}

func InsertLabels(
	labels []*models.Label,
	tx *sql.Tx,
) (sql.Result, error) {
	if len(labels) == 0 {
		return nil, nil
	}

	labelValues := []interface{}{}

	labelSqlStr := "INSERT INTO " + models.TableNames.Labels + "(" +
		models.LabelColumns.LabelID + ", " +
		models.LabelColumns.Name + ", " +
		models.LabelColumns.UserAccountID + ", " +
		models.LabelColumns.CreatedAt +
		" ) VALUES %s"

	for _, pollLabel := range labels {
		labelValues = append(
			labelValues,
			pollLabel.LabelID,
			pollLabel.Name,
			pollLabel.UserAccountID,
			pollLabel.CreatedAt,
		)
	}

	labelSqlStr = ReplaceSQL(labelSqlStr, "(?, ?, ?, ?)", len(labelValues))
	stmt, _ := tx.Prepare(labelSqlStr)

	return stmt.Exec(labelValues...)
}
