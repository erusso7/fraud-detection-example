package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/urfave/cli/v2"
)

const (
	ArgRows = "rows"
	ArgCp   = "corrupt-percentage"
	ArgCt   = "corrupt-type"
)

type CorruptType string

const (
	CorruptMissing CorruptType = "missing"
	CorruptEmpty   CorruptType = "empty"
)

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:        ArgRows,
				Aliases:     []string{"r"},
				Value:       100,
				Usage:       "number of rows to generate",
				DefaultText: "100",
			},
			&cli.StringFlag{
				Name:        ArgCt,
				Aliases:     []string{"ct"},
				Value:       "missing",
				Usage:       "the corruption type [accepts: missing, empty]",
				DefaultText: "missing",
			},
			&cli.UintFlag{
				Name:        ArgCp,
				Aliases:     []string{"cp"},
				Value:       20,
				Usage:       "the approximate percentage of corrupted data",
				DefaultText: "20",
			},
		},
		Action: generateRows,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func generateRows(cCtx *cli.Context) error {
	rand.Seed(time.Now().UnixNano())
	var err error

	num := cCtx.Uint(ArgRows)
	cRatio := cCtx.Uint(ArgCp)
	cType := CorruptType(cCtx.String(ArgCt))

	fileName := fmt.Sprintf("fraud_%d_%s_%d.parquet", num, cType, cRatio)

	//write
	fw, err := local.NewLocalFileWriter(fileName)
	if err != nil {
		log.Println("Can't open file", err)
		return err
	}
	pw, err := writer.NewParquetWriter(fw, new(Row), 10)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return err
	}

	for i := 0; uint(i) < num; i++ {
		row := NewRow(cType, cRatio)
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
	return nil
}
