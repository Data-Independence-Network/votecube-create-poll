package main

import (
	"context"
	"database/sql"
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/models"
	"github.com/valyala/fasthttp"
	"github.com/volatiletech/sqlboiler/queries/qm"
)

func VerifyLabels(
	db *sql.DB,
	batch *RequestBatch,
	ctxMapByLabelName *map[string][]*deserialize.CreatePollDeserializeContext,
) {

	var labelNames = make([]interface{}, len(*ctxMapByLabelName))
	i := 0
	for labelName, _ := range *ctxMapByLabelName {
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
		for _, ctx := range (*ctxMapByLabelName)[existingLabel.Name] {
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
}
