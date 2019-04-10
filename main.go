package main

import (
	"context"
	"flag"

	"github.com/sirupsen/logrus"

	"github.com/olivere/elastic"
)

var (
	elasticClient *elastic.Client
)

func main() {
	// program parameters
	var mstr = flag.String("mstr", "mongodb://localhost:27017", "mongodb connection string [mongodb://localhost:27017]")
	var estr = flag.String("estr", "http://localhost:9200", "elasticsearch connection string [http://localhost:9200]")
	var db = flag.String("db", "", "mongodb database[required]")
	var c = flag.String("c", "", "mongodb database collection[required]")
	if db == nil || c == nil || *db == "" || *c == "" {
		logrus.Fatalln("database && collection could not be empty")
	}
	// init elasticsearch client
	getElasticSearchClient(*estr)

}

func getElasticSearchClient(conStr string) {
	/* elasticsearch client */
	ctx := context.Background()
	elasticClient, err := elastic.NewClient(
		elastic.SetURL(conStr),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}
	if _, _, err := elasticClient.Ping("http://localhost:9200").Do(ctx); err != nil {
		panic(err)
	}
}
