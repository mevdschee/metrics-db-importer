package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/mevdschee/php-observability/statistics"
)

func safe(str string) string {
	var re = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	return re.ReplaceAllString(str, "")
}

func addSummaryTable(txn *sql.Tx, ss statistics.StatisticSet, tableName, tagName, datetime string) {
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" TIMESTAMPTZ NOT NULL, \"%s\" VARCHAR(255), \"duration\" DOUBLE PRECISION, \"count\" BIGINT);", tableName, tagName)
	_, err := txn.Exec(createSql)
	if err != nil {
		log.Fatal(err)
	}

	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\", \"%s\");", tableName, tableName, tagName)
	_, err = txn.Exec(indexSql)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, "time", tagName, "duration", "count"))
	if err != nil {
		log.Fatal(err)
	}

	for tagValue, count := range ss.Counters {
		duration := ss.Durations[tagValue]
		_, err = stmt.Exec(datetime, tagValue, duration, count)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func addHistogramTable(txn *sql.Tx, buckets []statistics.Bucket, ss statistics.StatisticSet, tableName, datetime string) {
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" TIMESTAMPTZ NOT NULL, \"duration\" DOUBLE PRECISION, \"count\" BIGINT);", tableName)
	_, err := txn.Exec(createSql)
	if err != nil {
		log.Fatal(err)
	}

	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\", \"duration\");", tableName, tableName)
	_, err = txn.Exec(indexSql)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, "time", "duration", "count"))
	if err != nil {
		log.Fatal(err)
	}

	for _, b := range buckets {
		duration := b.Value
		count := ss.Buckets[b.Name]
		_, err = stmt.Exec(datetime, duration, count)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func addTotalsTable(txn *sql.Tx, ss statistics.StatisticSet, tableName, datetime string) {
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" TIMESTAMPTZ NOT NULL, \"duration\" DOUBLE PRECISION, \"count\" BIGINT);", tableName)
	_, err := txn.Exec(createSql)
	if err != nil {
		log.Fatal(err)
	}

	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\");", tableName, tableName)
	_, err = txn.Exec(indexSql)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, "time", "duration", "count"))
	if err != nil {
		log.Fatal(err)
	}

	totalCount := uint64(0)
	totalDuration := float64(0)
	for tagValue, count := range ss.Counters {
		duration := ss.Durations[tagValue]
		totalDuration += duration
		totalCount += count
	}

	_, err = stmt.Exec(datetime, totalDuration, totalCount)
	if err != nil {
		log.Fatal(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func updateDatabase(driverName, dataSourceName string, stats *statistics.Statistics) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalf("cannot connect: %v", err)
	}
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	now := time.Now()
	datetime := now.Format(time.RFC3339)
	dateString := now.Format("2006_01_02")
	for key, ss := range stats.Names {
		parts := strings.SplitN(key, "|", 2)
		name := safe(parts[0])
		tagName := safe(parts[1])

		var tableName string
		tableName = fmt.Sprintf("%s_summary_by_%s_for_%s", name, tagName, dateString)
		addSummaryTable(txn, ss, tableName, tagName, datetime)
		tableName = fmt.Sprintf("%s_histogram_for_%s", name, dateString)
		addHistogramTable(txn, stats.Buckets, ss, tableName, datetime)
		tableName = fmt.Sprintf("%s_totals_for_%s", name, dateString)
		addTotalsTable(txn, ss, tableName, datetime)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func getMetrics(url string) (*statistics.Statistics, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status: %v", resp.StatusCode)
	}
	s := statistics.New()
	err = s.ReadGob(resp)
	if err != nil {
		return nil, fmt.Errorf("http read body: %v", err)
	}
	return s, nil
}

func main() {
	urlToScrape := flag.String("scrape", "http://localhost:9999/", "single URL to scrape for Gob metrics")
	scrapeEvery := flag.Duration("every", 5*time.Second, "seconds to wait between scrape requests")
	driverName := flag.String("db", "postgres", "")
	dataSourceName := flag.String("dsn", "dbname=metrics sslmode=disable user=metrics password=metrics search_path=public", "")
	flag.Parse()

	for {
		stats, err := getMetrics(*urlToScrape)
		if err != nil {
			log.Println(err)
		}
		updateDatabase(*driverName, *dataSourceName, stats)
		time.Sleep(*scrapeEvery)
	}
}
