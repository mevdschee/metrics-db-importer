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

func createCountsTable(db *sql.DB, driverName, name, labelName string) error {
	tableName := fmt.Sprintf("%s_count_by_%s", name, labelName)
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" timestamptz NOT NULL, \"%s\" varchar(255), \"count\" bigint);", tableName, labelName)
	if driverName == "mysql" {
		createSql = strings.ReplaceAll(createSql, "\"", "`")
		createSql = strings.ReplaceAll(createSql, "timestamptz", "timestamp")
	}
	_, err := db.Exec(createSql)
	if err != nil {
		return err
	}
	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\", \"%s\");", tableName, tableName, labelName)
	if driverName == "mysql" {
		indexSql = strings.ReplaceAll(indexSql, "\"", "`")
	}
	_, err = db.Exec(indexSql)
	if err != nil {
		return err
	}
	return nil
}

func insertCountsMysql(txn *sql.Tx, ss statistics.StatisticSet, name, labelName, datetime string) error {
	sqlStr := fmt.Sprintf("INSERT INTO `%s_count_by_%s` (`time`, `%s`, `count`) VALUES ", name, labelName, labelName)
	vals := []interface{}{}
	for labelValue, count := range ss.Counters {
		sqlStr += "(?, ?, ?),"
		vals = append(vals, datetime, labelValue, count)
	}
	if len(vals) == 0 {
		return nil
	}
	sqlStr = strings.TrimSuffix(sqlStr, ",")
	stmt, err := txn.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(vals...)
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func insertCountsPostgres(txn *sql.Tx, ss statistics.StatisticSet, name, labelName, datetime string) error {
	stmt, err := txn.Prepare(pq.CopyIn(fmt.Sprintf("%s_count_by_%s", name, labelName), "time", labelName, "count"))
	if err != nil {
		return err
	}
	for labelValue, count := range ss.Counters {
		_, err = stmt.Exec(datetime, labelValue, count)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func createSummaryTable(db *sql.DB, driverName, name, labelName string) error {
	tableName := fmt.Sprintf("%s_by_%s", name, labelName)
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" timestamptz NOT NULL, \"%s\" varchar(255), \"duration\" double precision, \"count\" bigint);", tableName, labelName)
	if driverName == "mysql" {
		createSql = strings.ReplaceAll(createSql, "\"", "`")
		createSql = strings.ReplaceAll(createSql, "timestamptz", "timestamp")
		createSql = strings.ReplaceAll(createSql, "double precision", "double")
	}
	_, err := db.Exec(createSql)
	if err != nil {
		return err
	}
	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\", \"%s\");", tableName, tableName, labelName)
	if driverName == "mysql" {
		indexSql = strings.ReplaceAll(indexSql, "\"", "`")
	}
	_, err = db.Exec(indexSql)
	if err != nil {
		return err
	}
	return nil
}

func insertSummaryMysql(txn *sql.Tx, ss statistics.StatisticSet, name, labelName, datetime string) error {
	sqlStr := fmt.Sprintf("INSERT INTO `%s_by_%s` (`time`, `%s`, `duration`, `count`) VALUES ", name, labelName, labelName)
	vals := []interface{}{}
	for labelValue, count := range ss.Measures {
		duration := ss.Durations[labelValue]
		sqlStr += "(?, ?, ?, ?),"
		vals = append(vals, datetime, labelValue, duration, count)
	}
	if len(vals) == 0 {
		return nil
	}
	sqlStr = strings.TrimSuffix(sqlStr, ",")
	stmt, err := txn.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(vals...)
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func insertSummaryPostgres(txn *sql.Tx, ss statistics.StatisticSet, name, labelName, datetime string) error {
	stmt, err := txn.Prepare(pq.CopyIn(fmt.Sprintf("%s_by_%s", name, labelName), "time", labelName, "duration", "count"))
	if err != nil {
		return err
	}
	for labelValue, count := range ss.Measures {
		duration := ss.Durations[labelValue]
		_, err = stmt.Exec(datetime, labelValue, duration, count)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func createHistogramTable(db *sql.DB, driverName string, name string) error {
	tableName := fmt.Sprintf("%s_histogram", name)
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" timestamptz NOT NULL, \"duration\" double precision, \"count\" bigint);", tableName)
	if driverName == "mysql" {
		createSql = strings.ReplaceAll(createSql, "\"", "`")
		createSql = strings.ReplaceAll(createSql, "timestamptz", "timestamp")
		createSql = strings.ReplaceAll(createSql, "double precision", "double")
	}
	_, err := db.Exec(createSql)
	if err != nil {
		return err
	}
	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\", \"duration\");", tableName, tableName)
	if driverName == "mysql" {
		indexSql = strings.ReplaceAll(indexSql, "\"", "`")
	}
	_, err = db.Exec(indexSql)
	if err != nil {
		return err
	}
	return nil
}

func insertHistogramMysql(txn *sql.Tx, ss statistics.StatisticSet, buckets []statistics.Bucket, name, datetime string) error {
	sqlStr := fmt.Sprintf("INSERT INTO `%s_histogram` (`time`, `duration`, `count`) VALUES ", name)
	vals := []interface{}{}
	for _, b := range buckets {
		duration := b.Value
		count := ss.Buckets[b.Name]
		sqlStr += "(?, ?, ?),"
		vals = append(vals, datetime, duration, count)
	}
	if len(vals) == 0 {
		return nil
	}
	sqlStr = strings.TrimSuffix(sqlStr, ",")
	stmt, err := txn.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(vals...)
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func insertHistogramPostgres(txn *sql.Tx, ss statistics.StatisticSet, buckets []statistics.Bucket, name, datetime string) error {
	stmt, err := txn.Prepare(pq.CopyIn(fmt.Sprintf("%s_histogram", name), "time", "duration", "count"))
	if err != nil {
		log.Fatal(err)
	}
	for _, b := range buckets {
		duration := b.Value
		count := ss.Buckets[b.Name]
		_, err = stmt.Exec(datetime, duration, count)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func createTotalsTable(db *sql.DB, driverName string, name string) error {
	tableName := fmt.Sprintf("%s_totals", name)
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\"time\" timestamptz NOT NULL, \"duration\" double precision, \"count\" bigint);", tableName)
	if driverName == "mysql" {
		createSql = strings.ReplaceAll(createSql, "\"", "`")
		createSql = strings.ReplaceAll(createSql, "timestamptz", "timestamp")
		createSql = strings.ReplaceAll(createSql, "double precision", "double")
	}
	_, err := db.Exec(createSql)
	if err != nil {
		return err
	}
	indexSql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS \"%s_idx\" ON \"%s\"(\"time\");", tableName, tableName)
	if driverName == "mysql" {
		indexSql = strings.ReplaceAll(indexSql, "\"", "`")
	}
	_, err = db.Exec(indexSql)
	if err != nil {
		return err
	}
	return nil
}

func insertTotalsMysql(txn *sql.Tx, ss statistics.StatisticSet, name, datetime string) error {
	sqlStr := fmt.Sprintf("INSERT INTO `%s_totals` (`time`, `duration`, `count`) VALUES (?, ?, ?)", name)
	totalCount := uint64(0)
	totalDuration := float64(0)
	for labelValue, count := range ss.Measures {
		duration := ss.Durations[labelValue]
		totalDuration += duration
		totalCount += count
	}
	stmt, err := txn.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(datetime, totalDuration, totalCount)
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func insertTotalsPostgres(txn *sql.Tx, ss statistics.StatisticSet, name, datetime string) error {
	stmt, err := txn.Prepare(pq.CopyIn(fmt.Sprintf("%s_totals", name), "time", "duration", "count"))
	if err != nil {
		return err
	}
	totalCount := uint64(0)
	totalDuration := float64(0)
	for labelValue, count := range ss.Measures {
		duration := ss.Durations[labelValue]
		totalDuration += duration
		totalCount += count
	}
	_, err = stmt.Exec(datetime, totalDuration, totalCount)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

func createTables(db *sql.DB, driverName string, stats *statistics.Statistics) error {
	for key, ss := range stats.Names {
		parts := strings.SplitN(key, "|", 2)
		name := safe(parts[0])
		labelName := safe(parts[1])
		if len(ss.Counters) > 0 {
			err := createCountsTable(db, driverName, name, labelName)
			if err != nil {
				return fmt.Errorf("create counts table: %v", err)
			}
		}
		if len(ss.Measures) > 0 {
			err := createSummaryTable(db, driverName, name, labelName)
			if err != nil {
				return fmt.Errorf("create summary table: %v", err)
			}
			err = createHistogramTable(db, driverName, name)
			if err != nil {
				return fmt.Errorf("create summary table: %v", err)
			}
			err = createTotalsTable(db, driverName, name)
			if err != nil {
				return fmt.Errorf("create totals table: %v", err)
			}
		}
	}
	return nil
}

func insertRecords(txn *sql.Tx, driverName string, stats *statistics.Statistics) error {
	now := time.Now()
	for key, ss := range stats.Names {
		parts := strings.SplitN(key, "|", 2)
		name := safe(parts[0])
		labelName := safe(parts[1])

		switch driverName {
		case "mysql":
			datetime := now.Format("2006-01-02 15:04:05") // mysql format
			if len(ss.Counters) > 0 {
				err := insertCountsMysql(txn, ss, name, labelName, datetime)
				if err != nil {
					return err
				}
			}
			if len(ss.Measures) > 0 {
				err := insertSummaryMysql(txn, ss, name, labelName, datetime)
				if err != nil {
					return err
				}
				err = insertHistogramMysql(txn, ss, stats.Buckets, name, datetime)
				if err != nil {
					return err
				}
				err = insertTotalsMysql(txn, ss, name, datetime)
				if err != nil {
					return err
				}
			}
		case "postgres":
			datetime := now.Format(time.RFC3339)
			if len(ss.Counters) > 0 {
				err := insertCountsPostgres(txn, ss, name, labelName, datetime)
				if err != nil {
					return err
				}
			}
			if len(ss.Measures) > 0 {
				err := insertSummaryPostgres(txn, ss, name, labelName, datetime)
				if err != nil {
					return err
				}
				err = insertHistogramPostgres(txn, ss, stats.Buckets, name, datetime)
				if err != nil {
					return err
				}
				err = insertTotalsPostgres(txn, ss, name, datetime)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func deleteRecords(db *sql.DB, driverName string, stats *statistics.Statistics, retentionInDays int) error {
	datetime := time.Now().AddDate(0, 0, -1*retentionInDays).Format(time.RFC3339)
	for key, ss := range stats.Names {
		parts := strings.SplitN(key, "|", 2)
		name := safe(parts[0])
		labelName := safe(parts[1])

		if len(ss.Counters) > 0 {
			deleteSql := fmt.Sprintf("DELETE FROM \"%s_count_by_%s\" WHERE \"time\" < '%s'", name, labelName, datetime)
			if driverName == "mysql" {
				deleteSql = strings.ReplaceAll(deleteSql, "\"", "`")
			}
			_, err := db.Exec(deleteSql)
			if err != nil {
				return err
			}
		}
		if len(ss.Measures) > 0 {
			deleteSql := fmt.Sprintf("DELETE FROM \"%s_by_%s\" WHERE \"time\" < '%s'", name, labelName, datetime)
			if driverName == "mysql" {
				deleteSql = strings.ReplaceAll(deleteSql, "\"", "`")
			}
			_, err := db.Exec(deleteSql)
			if err != nil {
				return err
			}
			deleteSql = fmt.Sprintf("DELETE FROM \"%s_histogram\" WHERE \"time\" < '%s';", name, datetime)
			if driverName == "mysql" {
				deleteSql = strings.ReplaceAll(deleteSql, "\"", "`")
			}
			_, err = db.Exec(deleteSql)
			if err != nil {
				return err
			}
			deleteSql = fmt.Sprintf("DELETE FROM \"%s_totals\" WHERE \"time\" < '%s';", name, datetime)
			if driverName == "mysql" {
				deleteSql = strings.ReplaceAll(deleteSql, "\"", "`")
			}
			_, err = db.Exec(deleteSql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func updateDatabase(driverName, dataSourceName string, stats *statistics.Statistics, retentionInDays int) error {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalf("cannot connect: %v", err)
	}
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		return err
	}
	err = insertRecords(txn, driverName, stats)
	if err != nil {
		txn.Rollback()
		err = createTables(db, driverName, stats)
		if err != nil {
			return err
		}
		txn, err = db.Begin()
		if err != nil {
			return err
		}
		err = insertRecords(txn, driverName, stats)
		if err != nil {
			txn.Rollback()
			return err
		} else {
			txn.Commit()
		}
	} else {
		txn.Commit()
	}
	err = deleteRecords(db, driverName, stats, retentionInDays)
	if err != nil {
		return err
	}
	return err
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
	scrapeEvery := flag.Duration("every", 1*time.Second, "seconds to wait between scrape requests")
	retentionInDays := flag.Int("retention", 30, "retention in days")
	driverName := flag.String("db", "mysql", "drivername, either 'postgres' or 'mysql'")
	//dataSourceName := flag.String("dsn", "dbname=metrics sslmode=disable user=metrics password=metrics search_path=public", "dsn for the driver, see go sql documentation")
	dataSourceName := flag.String("dsn", "metrics:metrics@unix(/var/run/mysqld/mysqld.sock)/metrics", "dsn for the driver, see go sql documentation")
	flag.Parse()

	ticker := time.NewTicker(*scrapeEvery)
	for range ticker.C {
		stats, err := getMetrics(*urlToScrape)
		if err != nil {
			log.Println(err)
		}
		if stats != nil {
			err = updateDatabase(*driverName, *dataSourceName, stats, *retentionInDays)
			if err != nil {
				log.Println(err)
			}
		}
	}
}
