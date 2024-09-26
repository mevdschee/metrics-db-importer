package main

import (
	"database/sql"
	"encoding/gob"
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

func updateDatabase(driverName, dataSourceName string, stats *statistics.Statistics) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalf("cannot connect: %v", err)
	}
	defer db.Close()

	now := time.Now()
	datetime := now.Format(time.RFC3339)
	dateString := now.Format("2006_01")
	for key, ss := range stats.Names {
		parts := strings.SplitN(key, "|", 2)
		name := safe(parts[0])
		tagName := safe(parts[1])
		tableName := fmt.Sprintf("%s_by_%s_in_%s", name, tagName, dateString)
		log.Println(tableName)
		createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" TIMESTAMPTZ NOT NULL, \"%s\" VARCHAR(255), \"count\" BIGINT, \"duration\" DOUBLE PRECISION);", tableName, tagName)

		_, err := db.Exec(createSql)
		if err != nil {
			log.Fatal(err)
		}

		txn, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn(tableName, "time", tagName, "count", "duration"))
		if err != nil {
			log.Fatal(err)
		}

		for tagValue, count := range ss.Counters {
			duration := ss.Durations[tagValue]
			_, err = stmt.Exec(datetime, tagValue, count, duration)
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

		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
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
	dec := gob.NewDecoder(resp.Body)
	s := statistics.Statistics{}
	err = dec.Decode(&s)
	if err != nil {
		return nil, fmt.Errorf("http read body: %v", err)
	}
	return &s, nil
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
