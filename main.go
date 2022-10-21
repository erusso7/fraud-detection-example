package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
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

var offset = 0
var labels = []Label{LabelLegitimate, LabelFraud}
var transTypes = []TransType{TransTypeChip, TransTypeContactLess, TransTypeOnline, TransTypeSwipe, TransTypeManual}
var corrupted = 0.0

func NewRow(errorRatio int) interface{} {
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

	if rand.Intn(100) < errorRatio {
		corrupted++
		return corruptRow(r)
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

func main() {
	rand.Seed(time.Now().UnixNano())
	var err error

	num := 100
	if len(os.Args) > 1 {
		arg, err := strconv.ParseInt(os.Args[1], 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		num = int(arg)
	}

	errorRatio := 0
	if len(os.Args) > 2 {
		arg, err := strconv.ParseInt(os.Args[2], 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		errorRatio = int(arg)
	}

	fileName := fmt.Sprintf("fraud_%d_corrupted_%d.parquet", num, errorRatio)
	//write
	fw, err := local.NewLocalFileWriter(fileName)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(Row), 10)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	for i := 0; i < num; i++ {
		row := NewRow(errorRatio)
		if err = pw.Write(row); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("File created: " + fileName)
	_ = fw.Close()

	log.Printf("Corrupted %.0f/%d (%.2f%%)\n", corrupted, num, corrupted/float64(num)*100)
}
