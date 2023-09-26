package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-quantity-unit-conversion-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-quantity-unit-conversion-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var quantityUnitConversion *[]dpfm_api_output_formatter.QuantityUnitConversion
	for _, fn := range accepter {
		switch fn {
		case "QuantityUnitConversion":
			func() {
				quantityUnitConversion = c.QuantityUnitConversion(mtx, input, output, errs, log)
			}()
		case "QuantityUnitConversions":
			func() {
				quantityUnitConversion = c.QuantityUnitConversions(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		QuantityUnitConversion: quantityUnitConversion,
	}

	return data
}

func (c *DPFMAPICaller) QuantityUnitConversion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.QuantityUnitConversion {
	where := fmt.Sprintf("WHERE QuantityUnitConversion = '%s'", input.QuantityUnitConversion.QuantityUnitConversion)

	// if input.QuantityUnitConversion.IsMarkedForDeletion != nil {
	// 	where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.QuantityUnitConversion.IsMarkedForDeletion)
	// }

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_quantity_unit_quantity_unit_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, QuantityUnitConversion DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToQuantityUnitConversion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) QuantityUnitConversions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.QuantityUnitConversion {

	if input.QuantityUnitConversion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.QuantityUnitConversion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_quantity_unit_quantity_unit_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, QuantityUnitConversion DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToQuantityUnitConversion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
