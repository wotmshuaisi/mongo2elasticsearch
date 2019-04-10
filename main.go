package main

import (
	"context"
	"flag"
	"time"

	"gopkg.in/cheggaaa/pb.v1"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/sirupsen/logrus"

	"github.com/olivere/elastic"
)

var (
	elasticClient *elastic.Client
	collection    *mongo.Collection
)

func main() {
	/* variable definition */
	// var dataList = make(chan bson., size)
	/* program parameters */
	var mstr = flag.String("mstr", "mongodb://localhost:27017", "mongodb connection string [mongodb://localhost:27017]")
	var estr = flag.String("estr", "http://localhost:9200", "elasticsearch connection string [http://localhost:9200]")
	var db = flag.String("db", "", "mongodb database[required]")
	var c = flag.String("c", "", "mongodb database collection[required]")
	flag.Parse()
	// if db == nil || c == nil || *db == "" || *c == "" {
	// 	logrus.Fatalln("database && collection could not be empty")
	// }
	/* init clients */
	getClients(*estr, *mstr, *db, *c)
	/* processing */
	// get document count
	count, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		logrus.WithError(err).Fatalln("mongo client count() error")
	}
	// cursor, err := collection.Find(context.Background(), bson.M{})
	// if err != nil {
	// 	logrus.WithError(err).Fatalln("mongo client find() error")
	// }
	// for cursor.Next(ctx) {

	// }

	/* progress bar */
	bar := pb.StartNew(int(count))
	for i := int64(0); i < count; i++ {
		bar.Increment()
		time.Sleep(time.Millisecond)
	}
	bar.FinishPrint("done.")
}

func getClients(eStr, mStr, db, c string) {
	/* elasticsearch client */
	ctx := context.Background()
	elasticClient, err := elastic.NewClient(
		elastic.SetURL(eStr),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}
	if _, _, err := elasticClient.Ping("http://localhost:9200").Do(ctx); err != nil {
		panic(err)
	}
	/* mongodb client */
	mgoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mStr))
	if err != nil {
		panic(err)
	}
	if err := mgoClient.Ping(ctx, nil); err != nil {
		panic(err)
	}
	collection = mgoClient.Database(db).Collection(c)
}
