package requests

type QuantityUnitConversion struct {
	QuantityUnitFrom      string  `json:"QuantityUnitFrom"`
	QuantityUnitTo        string  `json:"QuantityUnitTo"`
	ConversionCoefficeint float32 `json:"LastChangeDate"`
}
