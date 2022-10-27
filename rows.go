package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
)

type TransType string

const (
	TransTypeChip        TransType = "chip_and_pin"
	TransTypeContactLess TransType = "contactless"
	TransTypeOnline      TransType = "online"
	TransTypeSwipe       TransType = "swipe"
	TransTypeManual      TransType = "manual"
)

type Label string

const (
	LabelLegitimate Label = "legitimate"
	LabelFraud            = "fraud"
)

const (
	sep           = "\t"
	initialDate   = 946_684_800 // 2000-01-01 00:00:00
	maxUserID     = 10_000
	maxMerchantID = 20_000
	minMerchantID = 1_000
)

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

var offset = 0
var labels = []Label{LabelLegitimate, LabelFraud}
var transTypes = []TransType{TransTypeChip, TransTypeContactLess, TransTypeOnline, TransTypeSwipe, TransTypeManual}
var corrupted = 0.0

func NewRow(errorType CorruptType, errorRatio uint) interface{} {
	offset += rand.Intn(10)

	r := Row{
		Timestamp:    initialDate + offset,
		Label:        labels[rand.Intn(len(labels))],
		UserId:       rand.Intn(maxUserID),
		Amount:       math.Round(rand.Float64()*float64(rand.Intn(1000)*1000)) / 1000,
		MerchantId:   rand.Intn(maxMerchantID) + minMerchantID,
		TransType:    transTypes[rand.Intn(len(transTypes))],
		Foreign:      rand.Intn(2) == 1,
		InterArrival: math.Round(rand.Float64()*10_000*1000) / 1000,
	}

	if uint(rand.Intn(100)) < errorRatio {
		corrupted++

		switch errorType {
		case CorruptMissing:
			return corruptRowMissing(r)
		case CorruptEmpty:
			return corruptRowEmpty(r)
		}
	}

	return r
}

func (r Row) String() string {
	var parts []string
	rowProps := []string{
		fmt.Sprintf("%d", r.Timestamp),
		string(r.Label),
		fmt.Sprintf("%d", r.UserId),
		fmt.Sprintf("%.3f", r.Amount),
		fmt.Sprintf("%d", r.MerchantId),
		string(r.TransType),
		fmt.Sprintf("%t", r.Foreign),
		fmt.Sprintf("%.3f", r.Amount),
	}
	for _, s := range rowProps {
		if strings.TrimSpace(s) != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, sep)
}

func corruptRowMissing(r Row) interface{} {
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

func corruptRowEmpty(r Row) Row {
	switch rand.Intn(3) {
	case 0:
		r.Amount = 0
	case 1:
		r.Label = ""
	case 2:
		r.MerchantId = 0
	}
	return r
}
