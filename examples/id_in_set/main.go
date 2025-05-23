package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/ferumlabs/pggen/examples/id_in_set/models"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	ctx := context.Background()

	conn, err := sql.Open("pgx", os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}

	pgClient := models.NewPGClient(conn)

	bar := "bar"
	foo1ID, err := pgClient.InsertFoo(ctx, &models.Foo{
		Value: &bar,
	})
	if err != nil {
		log.Fatal(err)
	}

	baz := "baz"
	foo2ID, err := pgClient.InsertFoo(ctx, &models.Foo{
		Value: &baz,
	})
	if err != nil {
		log.Fatal(err)
	}

	values, err := pgClient.GetFooValues(ctx, []int64{foo1ID, foo2ID})
	if err != nil {
		log.Fatal(err)
	}

	// ensure stable output
	sort.Slice(values, func(i, j int) bool {
		return *values[i] < *values[j]
	})

	for _, v := range values {
		fmt.Printf("%s\n", *v)
	}
}
