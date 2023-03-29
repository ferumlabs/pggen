package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/ferumlabs/pggen"
	"github.com/ferumlabs/pggen/examples/statement/models"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	ctx := context.Background()

	conn, err := sql.Open("pgx", os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}

	pgClient := models.NewPGClient(conn)

	id, err := pgClient.InsertUser(ctx, &models.User{
		Email:    "alphonso@yehaw.website",
		Nickname: "Alph",
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = pgClient.DeleteUsersByNickname(ctx, "Alph")
	if err != nil {
		log.Fatal(err)
	}

	_, err = pgClient.GetUser(ctx, id)
	if err == nil {
		log.Fatal("Alph is unexpectedly still in the db")
	}
	if pggen.IsNotFoundError(err) {
		fmt.Printf("Alph not found\n")
	}
}
