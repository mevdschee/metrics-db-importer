# metrics-db-importer

Import metrics into a RDBMS such as MariaDB or PostgreSQL. Read the blog post:

[https://tqdev.com/2024-high-frequency-metrics-in-php-using-tcp-sockets](https://tqdev.com/2024-high-frequency-metrics-in-php-using-tcp-sockets)

and

[https://tqdev.com/2024-distributed-metrics-in-php-using-go-and-gob](https://tqdev.com/2024-distributed-metrics-in-php-using-go-and-gob)

### Usage

    Usage of metrics-db-importer:
        -db string
                drivername, either 'postgres' or 'mysql' (default "mysql")
        -dsn string
                dsn for the driver, see go sql documentation (default "metrics:metrics@unix(/var/run/mysqld/mysqld.sock)/metrics")
        -every duration
                seconds to wait between scrape requests (default 1s)
        -retention int
                retention in days (default 30)
        -scrape string
                single URL to scrape for Gob metrics (default "http://localhost:9999/")
