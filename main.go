package main

import (
	"context"
	"flag"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/cheggaaa/pb.v1"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/sirupsen/logrus"

	"github.com/olivere/elastic"
)

var (
	elasticIndex       *elastic.IndexService
	mgocollection      *mongo.Collection
	idRegex, _         = regexp.Compile(`"_id":\{"\$oid":"(\w+)"\},`)
	dateRegex, _       = regexp.Compile(`\{"\$date":"([\w\d\-\:]+)"\}`)
	numberDateRegex, _ = regexp.Compile(`{"\$date":{"\$numberLong":"-?(\d{9})[\d]+"}}`)
	metaDataPipe       = make(chan string, 200)
	bar                *pb.ProgressBar
	wg                 = sync.WaitGroup{}
)

func init() {
	/* init logrus */
	logfile, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	logrus.SetOutput(io.MultiWriter(logfile, os.Stdout))
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
	if ok, err := elasticClient.IndexExists(c).Do(ctx); err != nil {
		panic(err)
	} else {
		if !ok {
			res, err := elasticClient.CreateIndex(c).Do(ctx)
			if err != nil {
				panic(err)
			}
			if res.Acknowledged != true {
				panic(res.Acknowledged)
			}
		}
	}
	elasticIndex = elasticClient.Index().Index(c)
	/* mongodb client */
	mgoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mStr))
	if err != nil {
		panic(err)
	}
	if err := mgoClient.Ping(ctx, nil); err != nil {
		panic(err)
	}
	mgocollection = mgoClient.Database(db).Collection(c)
}

func main() {
	/* program parameters */
	var mstr = flag.String("mstr", "mongodb://localhost:27017", "mongodb connection string [mongodb://localhost:27017]")
	var estr = flag.String("estr", "http://localhost:9200", "elasticsearch connection string [http://localhost:9200]")
	var db = flag.String("db", "", "mongodb database[required]")
	var c = flag.String("c", "", "mongodb database collection[required]")
	flag.Parse()
	if db == nil || c == nil || *db == "" || *c == "" {
		logrus.Fatalln("database && collection could not be empty")
	}
	/* init clients */
	getClients(*estr, *mstr, *db, *c)
	/* processing */
	// get document count
	count, err := mgocollection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		logrus.WithError(err).Fatalln("mongo client count() error")
		return
	}
	cursor, err := mgocollection.Find(context.Background(), bson.M{})
	if err != nil {
		logrus.WithError(err).Fatalln("mongo client find() error")
		return
	}
	// progress bar
	bar = pb.StartNew(int(count))
	wg.Add(int(count))
	go func() {
		for m := range metaDataPipe {
			idMatch := idRegex.FindStringSubmatch(m)
			insertIntoElastic(idMatch[1], idRegex.ReplaceAllString(m, ""), *c)
			wg.Done()
		}
	}()

	for cursor.Next(context.Background()) {
		itemBytes, err := bson.MarshalExtJSON(cursor.Current, false, true)
		if err != nil {
			logrus.WithError(err).Warnln("cursor decode error")
			continue
		}
		// regex processing [_id / datetime]
		tmpStr := string(itemBytes)
		metadataDateTimeConvert(&tmpStr)
		metaDataPipe <- tmpStr
	}
	wg.Wait()
	close(metaDataPipe)
	bar.Finish()
}

func metadataDateTimeConvert(metadata *string) {
	// for unix timestamp
	if ok := numberDateRegex.MatchString(*metadata); ok {
		for _, match := range numberDateRegex.FindAllStringSubmatch(*metadata, -1) {
			tmpTime, _ := strconv.Atoi(match[1])
			*metadata = strings.Replace(*metadata, match[0], "\""+time.Unix(int64(tmpTime), 0).Format(time.RFC3339)+"\"", 1)
		}
	}
	// for isodate
	if ok := dateRegex.MatchString(*metadata); ok {
		*metadata = dateRegex.ReplaceAllString(*metadata, "\"$1\"")
	}
}

func insertIntoElastic(id, metadata, index string) {
	defer func() { bar.Increment() }()
	res, err := elasticIndex.Index(index).Type(index).Id(id).BodyString(metadata).Refresh("true").Do(context.Background())
	if err != nil {
		logrus.WithError(err).Errorf("insert into elastic ID=%s Response=%#v\n", id, res)
		return
	}
	if res.Result != "created" && res.Result != "updated" {
		logrus.Warnf("insert into elastic failed - %#v", res)
	}
}
