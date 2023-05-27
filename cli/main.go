package main

import (
	"context"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/promql-engine/engine"
)

var (
	ctx            = context.Background()
	flagCpuProfile = flag.String("cpuprofile", "", "Path to cpu profile")
	flagMemProfile = flag.String("memprofile", "", "Path to mem profile")

	flagQuery        = flag.String("query", "", "The instant query to perform")
	flagTimestamp    = flag.Int64("timestamp", 0, "Timestamp to query")
	flagBlockDir     = flag.String("block", "", "Path to block directory")
	flagQueryTimeout = flag.Duration("timeout", 10*time.Second, "Query timeout")

	logger = log.Default()
)

type blockStorage struct {
	*tsdb.Block
}

func (bs *blockStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return tsdb.NewBlockQuerier(bs.Block, mint, maxt)
}

func main() {
	flag.Parse()

	block, err := tsdb.OpenBlock(nil, *flagBlockDir, tsdb.DefaultHeadOptions().ChunkPool)
	if err != nil {
		logger.Fatal("unable to open block directory: ", err)
	}
	blockStore := &blockStorage{block}

	if *flagCpuProfile != "" {
		cpuProfileFile, err := os.Create(*flagCpuProfile)
		if err != nil {
			logger.Fatal("unable to create cpu profile: ", err)
		}
		defer cpuProfileFile.Close()

		if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
			logger.Fatal("unable to start cpu profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	eng := engine.New(engine.Opts{EngineOpts: promql.EngineOpts{Timeout: *flagQueryTimeout}})
	query, err := eng.NewInstantQuery(ctx, blockStore, &promql.QueryOpts{}, *flagQuery, time.UnixMilli(*flagTimestamp))
	if err != nil {
		logger.Fatal("unable to create instant query: ", err)
	}

	res := query.Exec(ctx)

	if *flagMemProfile != "" {
		memProfileFile, err := os.Create(*flagMemProfile)
		if err != nil {
			logger.Fatal("unable to create memory profile: ", err)
		}
		defer memProfileFile.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(memProfileFile); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	logger.Println("results: \n", res)
}
