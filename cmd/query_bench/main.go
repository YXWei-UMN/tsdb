package main

import (
	"fmt"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	//tsdb "github.com/naivewong/tsdb-group"
	//"github.com/naivewong/tsdb-group/labels"
	//"github.com/naivewong/tsdb-group/testutil"
	//tsdb_origin "github.com/prometheus/tsdb"
	//origin_labels "github.com/prometheus/tsdb/labels"
)

func DBQuery112(db *tsdb.DB, host_matchers, Metric_Matchers []labels.Matcher, starts, ends []int64) error {
	fmt.Println("-------- DBQuery1-1-2 ---------")

	totalSamples := 0

	start := time.Now()
	// the most recent two hrs, query_bench in a time range of 5 mins
	for i := len(starts) / 12 * 10; i < len(starts); i++ {
		q, err := db.Querier(starts[i], ends[i])
		if err != nil {
			return err
		}

		ss, err := q.Select(host_matchers[rand.Intn(len(host_matchers))], Metric_Matchers[rand.Intn(len(Metric_Matchers))])
		if err != nil {
			return err
		}

		for ss.Next() {
			it := ss.At().Iterator()
			for it.Next() {
				totalSamples += 1
			}
		}
		q.Close()
	}
	fmt.Printf("> complete stage=DBQueryOrigin duration=%f\n", time.Since(start).Seconds())
	fmt.Printf("  > total samples=%d\n", totalSamples)
	fmt.Println("----------------------------------")
	return nil
}

func main() {
	//dbPath := "db_bench_query/" + strconv.Itoa(int(timeDelta))

	dbPath := "db_bench_query/"
	//TODO 这里是open db + load data blocks 还是create db？ 现在写的是create db
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	db, err := tsdb.Open(dbPath, logger, nil, &tsdb.Options{
		WALSegmentSize:         -1,
		RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:            tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
		NoLockfile:             false,
		AllowOverlappingBlocks: false,
	})

	if err != nil {
		println("error in open db when do query_bench bench")
	}

	host_matchers := []labels.Matcher{}
	for i := 0; i < 50; i++ { //donest sure whether the host range has effect on query_bench, temporarily use rand % 10M
		host_matchers = append(host_matchers, labels.NewEqualMatcher("host", strconv.Itoa(rand.Intn(10000000))))
	}

	Metric_Matchers := []labels.Matcher{
		labels.NewEqualMatcher("region", "cn-central-2"),
		labels.NewEqualMatcher("region", "cn-central-1"),
		labels.NewEqualMatcher("region", "cn-west-2"),
		labels.NewEqualMatcher("region", "cn-west-1"),
		labels.NewEqualMatcher("os", "centos7"),
		labels.NewEqualMatcher("os", "centos8"),
		labels.NewEqualMatcher("os", "Ubuntu20"),
		labels.NewEqualMatcher("team", "CHI"),
		labels.NewEqualMatcher("team", "LON"),
		labels.NewEqualMatcher("team", "NYC"),
	}

	time.Sleep(time.Second * 25)

	starts := []int64{}
	ends := []int64{}
	for i := int64(0); i < 3600*12*1000/1000; i += 300000 / 1000 {
		starts = append(starts, i*1000)
		ends = append(ends, (i+300000/1000)*1000)
	}

	DBQuery112(db, host_matchers, Metric_Matchers, starts, ends)

	/*switch typ {
	case 1:
		DBQuery112(db, queryLabels, timeDelta)
	case 2:
		DBQuery1112(db, queryLabels, timeDelta)
	case 3:
		DBQuery152(db, timeDelta)
	case 4:
		DBQuery1512(db, timeDelta)
	case 5:
		DBQuery512(db, timeDelta)
	case 6:
		DBQuery5112(db, timeDelta)
	default:
		println("plz define a query_bench type")
	}*/
}
