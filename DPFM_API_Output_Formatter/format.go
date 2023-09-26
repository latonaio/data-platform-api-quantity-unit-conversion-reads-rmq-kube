package dpfm_api_output_formatter

import (
	"data-platform-api-quantity-unit-conversion-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToQuantityUnitConversion(rows *sql.Rows) (*[]QuantityUnitConversion, error) {
	defer rows.Close()
	quantityUnitConversion := make([]QuantityUnitConversion, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.QuantityUnitConversion{}

		err := rows.Scan(
			&pm.QuantityUnitFrom,
			&pm.QuantityUnitTo,
			&pm.ConversionCoefficient,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &quantityUnitConversion, nil
		}

		data := pm
		quantityUnitConversion = append(quantityUnitConversion, QuantityUnitConversion{
			QuantityUnitFrom:      data.QuantityUnitFrom,
			QuantityUnitTo:        data.QuantityUnitTo,
			ConversionCoefficient: data.ConversionCoefficient,
		})
	}

	return &quantityUnitConversion, nil
}
