package main

import (
	"context"

	"github.com/olivere/elastic"
)

var (
	elasticClient *elastic.Client
)

func init() {
	ctx := context.Background()
	/* elasticsearch client */
	elasticClient, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200/"),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}

	if _, _, err := elasticClient.Ping("http://localhost:9200").Do(ctx); err != nil {
		panic(err)
	}
	/*  */
}

func main() {
}
