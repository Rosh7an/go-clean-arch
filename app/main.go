package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	// "github.com/go-sql-driver/mysql" // Import MySQL driver package
	"github.com/labstack/echo"
	_ "github.com/lib/pq" // Import PostgreSQL driver package
	"github.com/spf13/viper"

	_articleHttpDelivery "github.com/bxcodec/go-clean-arch/article/delivery/http"
	_articleHttpDeliveryMiddleware "github.com/bxcodec/go-clean-arch/article/delivery/http/middleware"
	_articleRepo "github.com/bxcodec/go-clean-arch/article/repository/postgres" // Update the article repository import
	_articleUcase "github.com/bxcodec/go-clean-arch/article/usecase"
	_authorRepo "github.com/bxcodec/go-clean-arch/author/repository/postgres" // Update the author repository import
)

func init() {
	viper.SetConfigFile(`config.json`)
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	if viper.GetBool(`debug`) {
		log.Println("Service RUN on DEBUG mode")
	}
}

func main() {
	dbHost := viper.GetString(`database.host`)
	dbPort := viper.GetString(`database.port`)
	dbUser := viper.GetString(`database.user`)
	dbPass := viper.GetString(`database.pass`)
	dbName := viper.GetString(`database.name`)

	// Construct the PostgreSQL connection string
	connection := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName,
	)

	dbConn, err := sql.Open(`postgres`, connection)

	log.Print(dbConn)

	if err != nil {
		log.Fatal(err)
	}
	err = dbConn.Ping()
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err := dbConn.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	e := echo.New()
	middL := _articleHttpDeliveryMiddleware.InitMiddleware()
	e.Use(middL.CORS)
	authorRepo := _authorRepo.NewPostgresAuthorRepository(dbConn)    // Update the author repository creation
	articleRepo := _articleRepo.NewPostgresArticleRepository(dbConn) // Update the article repository creation

	timeoutContext := time.Duration(viper.GetInt("context.timeout")) * time.Second
	articleUsecase := _articleUcase.NewArticleUsecase(articleRepo, authorRepo, timeoutContext)
	_articleHttpDelivery.NewArticleHandler(e, articleUsecase)

	log.Fatal(e.Start(viper.GetString("server.address")))
}
