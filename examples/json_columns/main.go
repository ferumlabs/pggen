package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/ferumlabs/pggen/examples/json_columns/config"
	"github.com/ferumlabs/pggen/examples/json_columns/models"
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
		Email: "jonny@pielovers.net",
		Bio: models.UserBio{
			Name:        "Jonny Jet",
			FavoritePie: "All of them!",
		},
		Config: config.Config{
			HomepageIsPublic: false,
			Deactivated:      true,
		},
		Homepage: []byte(`{"status": "under construction"}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	user, err := pgClient.GetUser(ctx, id)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("name =", user.Bio.Name)
	fmt.Println("deactivated =", user.Config.Deactivated)
	fmt.Println("homepage =", string(user.Homepage))
}
