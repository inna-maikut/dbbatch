module github.com/inna-maikut/dbbatch/tests/pgx_v4

go 1.21.0

replace github.com/inna-maikut/dbbatch => ../..

require (
	github.com/inna-maikut/dbbatch v0.0.0-19700101000000-5657c1cc4545
	github.com/jackc/pgx/v5 v5.5.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/stretchr/testify v1.8.2
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/lib/pq v1.10.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.9.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
