package main

import (
	client "github.com/influxdata/influxdb/client/v2"
)

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type dbConfig struct {
	DBName          string
	TimePercision   string
	MeasurementName string
	HTTPAddr        string
	TimerResolution int
}

type dbRow struct {
	logLine         string
	time            string
	location        string
	path            string
	percentile100th int
	percentile99th  int
	percentile95th  int
	percentile80th  int
	percentile50th  int
}

var db dbConfig

var measurements = map[string]string{
	"dsa":   "dsalatency",
	"infer": "inferlatency",
}

var layout = "2006-01-02T15:04:05.000Z"

func init() {
	db.DBName = "latencyCalculation"
	db.HTTPAddr = "http://localhost:8086"
	db.TimePercision = "s"
	db.TimerResolution = 5
}

func main() {

	if len(os.Args) < 1 {
		//TODO: remove print statement
		fmt.Println("Usage: parsePercentile <Input file>")
		return
	}

	file, err := os.OpenFile(os.Args[1], os.O_RDONLY, os.ModePerm)
	if err != nil {
		//TODO: Remove print statement
		fmt.Println(err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	conn, batchPoint, err := setUpDB()
	if err != nil {
		//TODO: Remove print statement
		fmt.Println(err)
		return
	}

	defer conn.Close()

	linecounter := 0

	var entry dbRow

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			//TODO: Remove print statement
			fmt.Println(err)
			return
		}

		linecounter++

		lineString := strings.Split(string(line), "\t")

		if len(lineString) < 2 {
			return
		}

		if entry.path != "" && (entry.path != lineString[2] ||
			entry.time != lineString[1]) {
			err := addToDB(conn, batchPoint, entry)
			if err != nil {
				//TODO
				fmt.Println(err)
				return
			}
		}

		entry.logLine = lineString[0]
		entry.time = lineString[1]
		entry.path = lineString[2]
		if entry.logLine == "dsa" {
			entry.location = lineString[3]
			getPercentile(&entry, lineString[4:])
		} else {
			entry.location = ""
			getPercentile(&entry, lineString[3:])
		}

	}

	if entry.path != "" {
		err := addToDB(conn, batchPoint, entry)
		if err != nil {
			//TODO
			fmt.Println(err)
			return
		}
	}

}

func setUpDB() (client.Client, client.BatchPoints, error) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: db.HTTPAddr,
	})
	if err != nil {
		return nil, nil, err
	}
	batchpoint, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db.DBName,
		Precision: db.TimePercision,
	})
	if err != nil {
		return nil, nil, err
	}
	return conn, batchpoint, err
}

func addToDB(conn client.Client, batchPoint client.BatchPoints, entry dbRow) error {
	measurement := measurements[entry.logLine]
	tag := map[string]string{
		"path": entry.location + " " + entry.path,
	}
	fields := map[string]interface{}{
		"100": entry.percentile100th,
		"99":  entry.percentile99th,
		"95":  entry.percentile95th,
		"80":  entry.percentile80th,
		"50":  entry.percentile50th,
	}

	logtime, err := time.Parse(layout, entry.time)
	if err != nil {
		return err
	}
	point, err := client.NewPoint(measurement, tag, fields, logtime)
	if err != nil {
		fmt.Println(" while making point")
		return err
	}

	batchPoint.AddPoint(point)

	if err := conn.Write(batchPoint); err != nil {
		// TODO
		fmt.Println(err)
		return err
	}

	return nil

}

func getPercentile(entry *dbRow, line []string) {

	percentileVal, err := strconv.Atoi(line[1])
	if err != nil {
		fmt.Println(line[1], err)
		return
	}

	percentileVal = percentileVal / 1000

	switch line[0] {
	case "100.0":
		entry.percentile100th = percentileVal
	case "99.0":
		entry.percentile99th = percentileVal
	case "95.0":
		entry.percentile95th = percentileVal
	case "80.0":
		entry.percentile80th = percentileVal
	case "50.0":
		entry.percentile50th = percentileVal
	}
}
