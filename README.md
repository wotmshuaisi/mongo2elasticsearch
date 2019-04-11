mongo2elasticsearch
===================

[![forthebadge](https://forthebadge.com/images/badges/made-with-go.svg)](https://forthebadge.com)

Elasticsearch transfer tool for MongoDB database (Golang version)

## HOWTOUSE

```golang
> ./mongo2elasticsearch
Usage of ./mongo2elasticsearch:
  -c string
        mongodb database collection[required]
  -db string
        mongodb database[required]
  -estr string
        elasticsearch connection string [http://localhost:9200] (default "http://localhost:9200")
  -mstr string
        mongodb connection string [mongodb://localhost:27017] (default "mongodb://localhost:27017")
```

## LICENSE

[![](http://www.wtfpl.net/wp-content/uploads/2012/12/wtfpl-badge-4.png)](http://www.wtfpl.net/)