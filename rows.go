package main

import "math/rand"

type Row struct {
	Timestamp    int       `parquet:"name=timestamp, type=INT32"`
	Label        Label     `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8"`
	UserId       int       `parquet:"name=user_id, type=INT32"`
	Amount       float64   `parquet:"name=amount, type=DOUBLE, scale=2"`
	MerchantId   int       `parquet:"name=merchant_id, type=INT32"`
	TransType    TransType `parquet:"name=trans_type, type=BYTE_ARRAY"`
	Foreign      bool      `parquet:"name=foreign, type=BOOLEAN"`
	InterArrival float64   `parquet:"name=inter_arrival, type=DOUBLE"`
}

type RowWithoutLabel struct {
	//Label        Label     `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp    int       `parquet:"name=timestamp, type=INT32"`
	UserId       int       `parquet:"name=user_id, type=INT32"`
	Amount       float64   `parquet:"name=amount, type=DOUBLE, scale=2"`
	MerchantId   int       `parquet:"name=merchant_id, type=INT32"`
	TransType    TransType `parquet:"name=trans_type, type=BYTE_ARRAY"`
	Foreign      bool      `parquet:"name=foreign, type=BOOLEAN"`
	InterArrival float64   `parquet:"name=inter_arrival, type=DOUBLE"`
}

type RowWithoutAmount struct {
	//Amount       float64   `parquet:"name=amount, type=DOUBLE, scale=2"`
	Timestamp    int       `parquet:"name=timestamp, type=INT32"`
	Label        Label     `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8"`
	UserId       int       `parquet:"name=user_id, type=INT32"`
	MerchantId   int       `parquet:"name=merchant_id, type=INT32"`
	TransType    TransType `parquet:"name=trans_type, type=BYTE_ARRAY"`
	Foreign      bool      `parquet:"name=foreign, type=BOOLEAN"`
	InterArrival float64   `parquet:"name=inter_arrival, type=DOUBLE"`
}

type RowWithoutMerchant struct {
	//MerchantId   int       `parquet:"name=merchant_id, type=INT32"`
	Timestamp    int       `parquet:"name=timestamp, type=INT32"`
	Label        Label     `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8"`
	UserId       int       `parquet:"name=user_id, type=INT32"`
	Amount       float64   `parquet:"name=amount, type=DOUBLE, scale=2"`
	TransType    TransType `parquet:"name=trans_type, type=BYTE_ARRAY"`
	Foreign      bool      `parquet:"name=foreign, type=BOOLEAN"`
	InterArrival float64   `parquet:"name=inter_arrival, type=DOUBLE"`
}

func corruptRow(r Row) interface{} {
	switch rand.Intn(3) {
	case 0:
		return RowWithoutAmount{
			Timestamp:    r.Timestamp,
			Label:        r.Label,
			UserId:       r.UserId,
			MerchantId:   r.MerchantId,
			TransType:    r.TransType,
			Foreign:      r.Foreign,
			InterArrival: r.InterArrival,
		}
	case 1:
		return RowWithoutLabel{
			Timestamp:    r.Timestamp,
			UserId:       r.UserId,
			Amount:       r.Amount,
			MerchantId:   r.MerchantId,
			TransType:    r.TransType,
			Foreign:      r.Foreign,
			InterArrival: r.InterArrival,
		}
	case 2:
		return RowWithoutMerchant{
			Timestamp:    r.Timestamp,
			Label:        r.Label,
			UserId:       r.UserId,
			Amount:       r.Amount,
			TransType:    r.TransType,
			Foreign:      r.Foreign,
			InterArrival: r.InterArrival,
		}
	}
	return nil
}
