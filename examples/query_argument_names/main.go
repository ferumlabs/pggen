package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/ferumlabs/pggen"
	"github.com/ferumlabs/pggen/examples/query_argument_names/models"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	ctx := context.Background()

	conn, err := sql.Open("pgx", os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}

	pgClient := models.NewPGClient(conn)

	//
	// fill in some users
	//

	_, err = pgClient.InsertUser(ctx, &models.User{
		Email:    "todd@seriouscorp.com",
		Nickname: "Todd",
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = pgClient.InsertUser(ctx, &models.User{
		Email:    "alphonso@yehaw.website",
		Nickname: "Iceman",
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = pgClient.InsertUser(ctx, &models.User{
		Email:    "kylerfry87@yahoo.com",
		Nickname: "ChudneyK",
	})
	if err != nil {
		log.Fatal(err)
	}
	mikeID, err := pgClient.InsertUser(ctx, &models.User{
		Email:    "mikemasters@hotmail.com",
		Nickname: "Iceman",
	})
	if err != nil {
		log.Fatal(err)
	}

	//
	// Run a query and statement with custom argument names
	//

	todd, err := pgClient.GetUserByEmailOrNickname(ctx, "todd@sillycorp.io" /* whoops wrong domain */, "Todd")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("todd email = %s\n", todd[0].Email)

	_, err = pgClient.DeleteUsersByNickname(ctx, "Iceman")
	if err != nil {
		log.Fatal(err)
	}
	_, err = pgClient.GetUser(ctx, mikeID)
	if err == nil {
		log.Fatal("mike is unexpectedly still in the db")
	}
	if pggen.IsNotFoundError(err) {
		fmt.Printf("mike not found\n")
	}
}
