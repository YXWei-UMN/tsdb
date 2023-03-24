// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// ExponentialBlockRanges returns the time ranges based on the stepSize.
func ExponentialBlockRanges(minSize int64, steps, stepSize int) []int64 {
	ranges := make([]int64, 0, steps)
	curRange := minSize
	for i := 0; i < steps; i++ {
		ranges = append(ranges, curRange)
		curRange = curRange * int64(stepSize)
	}

	return ranges
}

// Compactor provides compaction against an underlying storage
// of time series data.
type Compactor interface {
	// Plan returns a set of directories that can be compacted concurrently.
	// The directories can be overlapping.
	// Results returned when compactions are in progress are undefined.
	Plan(dir string) ([]string, error)

	// Write persists a Block into a directory.
	// No Block is written when resulting Block has 0 samples, and returns empty ulid.ULID{}.
	Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error)

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	// Can optionally pass a list of already open blocks,
	// to avoid having to reopen them.
	// When resulting Block has 0 samples
	//  * No block is written.
	//  * The source dirs are marked Deletable.
	//  * Returns empty ulid.ULID{}.
	Compact(dest string, dirs []string, open []*Block) (ulid.ULID, error)
}

// LeveledCompactor implements the Compactor interface.
type LeveledCompactor struct {
	metrics   *compactorMetrics
	logger    log.Logger
	ranges    []int64
	chunkPool chunkenc.Pool
	ctx       context.Context
	mtx       sync.RWMutex
}

type compactorMetrics struct {
	ran                    prometheus.Counter
	populatingBlocks       prometheus.Gauge
	overlappingBlocks      prometheus.Counter
	duration               prometheus.Histogram
	write_block_duration   uint64
	compact_block_duration uint64
	chunkSize              prometheus.Histogram
	chunkSamples           prometheus.Histogram
	chunkRange             prometheus.Histogram
}

func newCompactorMetrics(r prometheus.Registerer) *compactorMetrics {
	m := &compactorMetrics{}

	m.write_block_duration = uint64(0)
	m.compact_block_duration = uint64(0)
	m.ran = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	m.populatingBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_compaction_populating_block",
		Help: "Set to 1 when a block is currently being written to the disk.",
	})
	m.overlappingBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_vertical_compactions_total",
		Help: "Total number of compactions done on overlapping blocks.",
	})
	m.duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_duration_seconds",
		Help:    "Duration of compaction runs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	m.chunkSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_size_bytes",
		Help:    "Final size of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(32, 1.5, 12),
	})
	m.chunkSamples = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_samples",
		Help:    "Final number of samples on their first compaction",
		Buckets: prometheus.ExponentialBuckets(4, 1.5, 12),
	})
	m.chunkRange = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_range_seconds",
		Help:    "Final time range of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(100, 4, 10),
	})

	if r != nil {
		r.MustRegister(
			m.ran,
			m.populatingBlocks,
			m.overlappingBlocks,
			m.duration,
			m.chunkRange,
			m.chunkSamples,
			m.chunkSize,
		)
	}
	return m
}

// NewLeveledCompactor returns a LeveledCompactor.
func NewLeveledCompactor(ctx context.Context, r prometheus.Registerer, l log.Logger, ranges []int64, pool chunkenc.Pool) (*LeveledCompactor, error) {
	if len(ranges) == 0 {
		return nil, errors.Errorf("at least one range must be provided")
	}
	if pool == nil {
		pool = chunkenc.NewPool()
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	return &LeveledCompactor{
		ranges:    ranges,
		chunkPool: pool,
		logger:    l,
		metrics:   newCompactorMetrics(r),
		ctx:       ctx,
	}, nil
}

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

// Plan returns a list of compactable blocks in the provided directory.
func (c *LeveledCompactor) Plan(dir string) ([]string, error) {
	dirs, err := blockDirs(dir)
	if err != nil {
		return nil, err
	}
	if len(dirs) < 1 {
		return nil, nil
	}

	var dms []dirMeta
	for _, dir := range dirs {
		meta, _, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dirMeta{dir, meta})
	}
	return c.plan(dms)
}

func (c *LeveledCompactor) plan(dms []dirMeta) ([]string, error) {
	sort.Slice(dms, func(i, j int) bool {
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})

	res := c.selectOverlappingDirs(dms)
	if len(res) > 0 {
		return res, nil
	}
	// No overlapping blocks, do compaction the usual way.
	// We do not include a recently created block with max(minTime), so the block which was just created from WAL.
	// This gives users a window of a full block size to piece-wise backup new data without having to care about data overlap.
	dms = dms[:len(dms)-1]

	for _, dm := range c.selectDirs(dms) {
		res = append(res, dm.dir)
	}
	if len(res) > 0 {
		return res, nil
	}

	// Compact any blocks with big enough time range that have >5% tombstones.
	for i := len(dms) - 1; i >= 0; i-- {
		meta := dms[i].meta
		if meta.MaxTime-meta.MinTime < c.ranges[len(c.ranges)/2] {
			break
		}
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []string{dms[i].dir}, nil
		}
	}

	return nil, nil
}

// selectDirs returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
func (c *LeveledCompactor) selectDirs(ds []dirMeta) []dirMeta {
	if len(c.ranges) < 2 || len(ds) < 1 {
		return nil
	}

	highTime := ds[len(ds)-1].meta.MinTime

	for _, iv := range c.ranges[1:] {
		parts := splitByRange(ds, iv)
		if len(parts) == 0 {
			continue
		}

	Outer:
		for _, p := range parts {
			// Do not select the range if it has a block whose compaction failed.
			for _, dm := range p {
				if dm.meta.Compaction.Failed {
					continue Outer
				}
			}

			mint := p[0].meta.MinTime
			maxt := p[len(p)-1].meta.MaxTime
			// Pick the range of blocks if it spans the full range (potentially with gaps)
			// or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same
			// size still fits in the range.
			if (maxt-mint == iv || maxt <= highTime) && len(p) > 1 {
				return p
			}
		}
	}

	return nil
}

// selectOverlappingDirs returns all dirs with overlapping time ranges.
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
func (c *LeveledCompactor) selectOverlappingDirs(ds []dirMeta) []string {
	if len(ds) < 2 {
		return nil
	}
	var overlappingDirs []string
	globalMaxt := ds[0].meta.MaxTime
	for i, d := range ds[1:] {
		if d.meta.MinTime < globalMaxt {
			if len(overlappingDirs) == 0 { // When it is the first overlap, need to add the last one as well.
				overlappingDirs = append(overlappingDirs, ds[i].dir)
			}
			overlappingDirs = append(overlappingDirs, d.dir)
		} else if len(overlappingDirs) > 0 {
			break
		}
		if d.meta.MaxTime > globalMaxt {
			globalMaxt = d.meta.MaxTime
		}
	}
	return overlappingDirs
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	var splitDirs [][]dirMeta

	for i := 0; i < len(ds); {
		var (
			group []dirMeta
			t0    int64
			m     = ds[i].meta
		)
		// Compute start of aligned time range of size tr closest to the current block's start.
		if m.MinTime >= 0 {
			t0 = tr * (m.MinTime / tr)
		} else {
			t0 = tr * ((m.MinTime - tr + 1) / tr)
		}
		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > t0+tr {
			i++
			continue
		}

		// Add all dirs to the current group that are within [t0, t0+tr].
		for ; i < len(ds); i++ {
			// Either the block falls into the next range or doesn't fit at all (checked above).
			if ds[i].meta.MaxTime > t0+tr {
				break
			}
			group = append(group, ds[i])
		}

		if len(group) > 0 {
			splitDirs = append(splitDirs, group)
		}
	}

	return splitDirs
}

func compactBlockMetas(uid ulid.ULID, blocks ...*BlockMeta) *BlockMeta {
	res := &BlockMeta{
		ULID:    uid,
		MinTime: blocks[0].MinTime,
	}

	sources := map[ulid.ULID]struct{}{}
	// For overlapping blocks, the Maxt can be
	// in any block so we track it globally.
	maxt := int64(math.MinInt64)

	for _, b := range blocks {
		if b.MaxTime > maxt {
			maxt = b.MaxTime
		}
		if b.Compaction.Level > res.Compaction.Level {
			res.Compaction.Level = b.Compaction.Level
		}
		for _, s := range b.Compaction.Sources {
			sources[s] = struct{}{}
		}
		res.Compaction.Parents = append(res.Compaction.Parents, BlockDesc{
			ULID:    b.ULID,
			MinTime: b.MinTime,
			MaxTime: b.MaxTime,
		})
	}
	res.Compaction.Level++

	for s := range sources {
		res.Compaction.Sources = append(res.Compaction.Sources, s)
	}
	sort.Slice(res.Compaction.Sources, func(i, j int) bool {
		return res.Compaction.Sources[i].Compare(res.Compaction.Sources[j]) < 0
	})

	res.MaxTime = maxt
	return res
}

// Compact creates a new block in the compactor's directory from the blocks in the
// provided directories.
func (c *LeveledCompactor) Compact(dest string, dirs []string, open []*Block) (uid ulid.ULID, err error) {
	var (
		blocks []BlockReader
		bs     []*Block
		metas  []*BlockMeta
		uids   []string
	)
	start := time.Now()

	for _, d := range dirs {
		meta, _, err := readMetaFile(d)
		if err != nil {
			return uid, err
		}

		var b *Block

		// Use already open blocks if we can, to avoid
		// having the index data in memory twice.
		for _, o := range open {
			if meta.ULID == o.Meta().ULID {
				b = o
				break
			}
		}

		if b == nil {
			var err error
			b, err = OpenBlock(c.logger, d, c.chunkPool)
			if err != nil {
				return uid, err
			}
			defer b.Close()
		}

		metas = append(metas, meta)
		blocks = append(blocks, b)
		bs = append(bs, b)
		uids = append(uids, meta.ULID.String())
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid = ulid.MustNew(ulid.Now(), entropy)

	meta := compactBlockMetas(uid, metas...)
	err = c.write(dest, meta, blocks...)
	if err == nil {
		if meta.Stats.NumSamples == 0 {
			for _, b := range bs {
				b.meta.Compaction.Deletable = true
				n, err := writeMetaFile(c.logger, b.dir, &b.meta)
				if err != nil {
					level.Error(c.logger).Log(
						"msg", "Failed to write 'Deletable' to meta file after compaction",
						"ulid", b.meta.ULID,
					)
				}
				b.numBytesMeta = n
			}
			uid = ulid.ULID{}
			level.Info(c.logger).Log(
				"msg", "compact blocks resulted in empty block",
				"count", len(blocks),
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		} else {
			level.Info(c.logger).Log(
				"msg", "compact blocks",
				"count", len(blocks),
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"ulid", meta.ULID,
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		}

		c.metrics.compact_block_duration = c.metrics.compact_block_duration + uint64(time.Since(start)/1e6)
		println("total compaction duration (millisecond)", c.metrics.compact_block_duration)
		return uid, nil
	}

	var merr tsdb_errors.MultiError
	merr.Add(err)
	if err != context.Canceled {
		for _, b := range bs {
			if err := b.setCompactionFailed(); err != nil {
				merr.Add(errors.Wrapf(err, "setting compaction failed for block: %s", b.Dir()))
			}
		}
	}

	return uid, merr
}

func (c *LeveledCompactor) Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error) {
	start := time.Now()

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	meta := &BlockMeta{
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
	}
	meta.Compaction.Level = 1
	meta.Compaction.Sources = []ulid.ULID{uid}

	//Write似乎就是flush， compact 有另外的函数， 对于flush 没有parent 所以这一步似乎冗余
	if parent != nil {
		meta.Compaction.Parents = []BlockDesc{
			{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
		}
	}
	//似乎还需进去write来切分
	err := c.write(dest, meta, b)
	if err != nil {
		return uid, err
	}

	if meta.Stats.NumSamples == 0 {
		return ulid.ULID{}, nil
	}

	level.Info(c.logger).Log(
		"msg", "write block",
		"mint", meta.MinTime,
		"maxt", meta.MaxTime,
		"ulid", meta.ULID,
		"duration", time.Since(start),
	)

	c.metrics.write_block_duration = c.metrics.write_block_duration + uint64(time.Since(start)/1e6)
	println("total write block duratioin (millisecond)", c.metrics.write_block_duration)

	return uid, nil
}

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
type instrumentedChunkWriter struct {
	ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunks.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

// write creates a new block that is the union of the provided blocks into dir.
// It cleans up all files of the old blocks after completing successfully.
func (c *LeveledCompactor) write(dest string, meta *BlockMeta, blocks ...BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + ".tmp" //有tmp 那就有删tmp和合tmp得机会 就有multiple tmp的机会
	var closers []io.Closer
	defer func(t time.Time) {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(c.logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
		c.metrics.ran.Inc()
		c.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	var chunkw ChunkWriter

	chunkw, err = chunks.NewWriter(chunkDir(tmp))
	if err != nil {
		return errors.Wrap(err, "open chunk writer")
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        c.metrics.chunkSize,
			samples:     c.metrics.chunkSamples,
			trange:      c.metrics.chunkRange,
		}
	}

	indexw, err := index.NewWriter(filepath.Join(tmp, indexFilename))
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}
	closers = append(closers, indexw)

	if err := c.populateBlock(blocks, meta, indexw, chunkw); err != nil {
		return errors.Wrap(err, "write compaction")
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	var merr tsdb_errors.MultiError
	for _, w := range closers {
		merr.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if merr.Err() != nil {
		return merr.Err()
	}

	// Populated block is empty, so exit early.
	if meta.Stats.NumSamples == 0 {
		return nil
	}

	if _, err = writeMetaFile(c.logger, tmp, meta); err != nil {
		return errors.Wrap(err, "write merged meta")
	}

	// Create an empty tombstones file.
	if _, err := writeTombstoneFile(c.logger, tmp, newMemTombstones()); err != nil {
		return errors.Wrap(err, "write new tombstones file")
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := df.Sync(); err != nil {
		return errors.Wrap(err, "sync temporary dir file")
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make visible and remove old ones.
	//就是rename tmp 到 dir
	if err := fileutil.Replace(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
func (c *LeveledCompactor) populateBlock(blocks []BlockReader, meta *BlockMeta, indexw IndexWriter, chunkw ChunkWriter) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		set         ChunkSeriesSet
		allSymbols  = make(map[string]struct{}, 1<<16)
		closers     = []io.Closer{}
		overlapping bool
	)
	defer func() {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()
		c.metrics.populatingBlocks.Set(0)
	}()
	c.metrics.populatingBlocks.Set(1)

	globalMaxt := blocks[0].Meta().MaxTime
	// 遍历 all blocks 如果是flush 就是一个block; 每个block取出一个SeriesSet， 组成一个merger （好像就是两个chunk SeriesSet 的wrap）
	for i, b := range blocks {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if !overlapping {
			if i > 0 && b.Meta().MinTime < globalMaxt {
				c.metrics.overlappingBlocks.Inc()
				overlapping = true
				level.Warn(c.logger).Log("msg", "found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.Meta().MaxTime > globalMaxt {
				globalMaxt = b.Meta().MaxTime
			}
		}

		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %s", b)
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %s", b)
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %s", b)
		}
		closers = append(closers, tombsr)

		symbols, err := indexr.Symbols()
		if err != nil {
			return errors.Wrap(err, "read symbols")
		}
		for s := range symbols {
			allSymbols[s] = struct{}{}
		}

		all, err := indexr.Postings(index.AllPostingsKey())
		if err != nil {
			return err
		}
		all = indexr.SortedPostings(all)

		s := newCompactionSeriesSet(indexr, chunkr, tombsr, all)

		if i == 0 {
			set = s //第一个block的CompactionSeriesSet，如果有多个block （compaction），那就合并成CompactionMerger,如果是flush,那就一个CompactionSeriesSet
			continue
		}

		// 反复拿最新的block与上一层merger合并形成一个新的 merger，  但是 newCompactionMerger(set, s) 接收的是两个chunkSeriesSet啊 不是一个merger一个set
		set, err = newCompactionMerger(set, s)
		if err != nil {
			return err
		}
	}

	// We fully rebuild the postings list index from merged series.
	var (
		postings = index.NewMemPostings()
		values   = map[string]stringset{}
		i        = uint64(0)
	)

	if err := indexw.AddSymbols(allSymbols); err != nil {
		return errors.Wrap(err, "add symbols")
	}

	delIter := &deletedIterator{}

	// 遍历 merger 或 当前head flush下来的CompactionSeriesSet：所有series的集合
	//这里可以开始基于series的concurrent 操作
	for set.Next() { //这个 Next()depends on whether 你是 CompactionSeriesSet 还是merger
		// 如果是CompactionSeriesSet， Next() 返回下一个series对应的各种信息 label set / chunks / interval (删掉的时间段/有效的时间段)
		// 如果是merger，Next()根据label有序return下一个series （可以是组成merger的两个seriesSet里任意的一个series）. 因为是有序，如果两个seriesSet包含同一个series时，这个series的chunk和interval会被合并，加上label Set 一起返回

		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		lset, chks, dranges := set.At() // The chunks here are not fully deleted.
		if overlapping {
			// If blocks are overlapping, it is possible to have unsorted chunks.
			sort.Slice(chks, func(i, j int) bool {
				return chks[i].MinTime < chks[j].MinTime
			})
		}

		// Skip the series with all deleted chunks.
		if len(chks) == 0 {
			continue
		}

		//只是对该series的chunks里的每个chunk做一些检查 如果有问题就re encode，暂时没看懂具体做的什么
		for i, chk := range chks {
			// Re-encode head chunks that are still open (being appended to) or
			// outside the compacted MaxTime range.
			// The chunk.Bytes() method is not safe for open chunks hence the re-encoding.
			// This happens when snapshotting the head block.
			//
			// Block time range is half-open: [meta.MinTime, meta.MaxTime) and
			// chunks are closed hence the chk.MaxTime >= meta.MaxTime check.
			//
			// TODO think how to avoid the typecasting to verify when it is head block.
			if _, isHeadChunk := chk.Chunk.(*safeChunk); isHeadChunk && chk.MaxTime >= meta.MaxTime {
				dranges = append(dranges, Interval{Mint: meta.MaxTime, Maxt: math.MaxInt64})

			} else
			// Sanity check for disk blocks.
			// chk.MaxTime == meta.MaxTime shouldn't happen as well, but will brake many users so not checking for that.
			if chk.MinTime < meta.MinTime || chk.MaxTime > meta.MaxTime {
				return errors.Errorf("found chunk with minTime: %d maxTime: %d outside of compacted minTime: %d maxTime: %d",
					chk.MinTime, chk.MaxTime, meta.MinTime, meta.MaxTime)
			}

			if len(dranges) > 0 {
				// Re-encode the chunk to not have deleted values.
				if !chk.OverlapsClosedInterval(dranges[0].Mint, dranges[len(dranges)-1].Maxt) {
					continue
				}
				newChunk := chunkenc.NewXORChunk()
				app, err := newChunk.Appender()
				if err != nil {
					return err
				}

				delIter.it = chk.Chunk.Iterator(delIter.it)
				delIter.intervals = dranges

				var (
					t int64
					v float64
				)
				for delIter.Next() {
					t, v = delIter.At()
					app.Append(t, v)
				}
				if err := delIter.Err(); err != nil {
					return errors.Wrap(err, "iterate chunk while re-encoding")
				}

				chks[i].Chunk = newChunk
				chks[i].MaxTime = t
			}
		}

		mergedChks := chks
		if overlapping {
			mergedChks, err = chunks.MergeOverlappingChunks(chks)
			if err != nil {
				return errors.Wrap(err, "merge overlapping chunks")
			}
		}

		//后续开始update各种共享数据 所以需要lock
		c.mtx.Lock()

		//简单看了 chunks.go 里实现的WriteChunks, 应该就是把属于一个series的多个chunk写一起
		//官方的话是：serializes a time block of chunked series data
		if err := chunkw.WriteChunks(mergedChks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}

		// 具体实现在index.go里, 插入series 构建到chunk的index
		if err := indexw.AddSeries(i, lset, mergedChks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		meta.Stats.NumChunks += uint64(len(mergedChks))
		meta.Stats.NumSeries++
		for _, chk := range mergedChks {
			meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		//TODO 这里和上面的WriteChunks的区别是？？
		for _, chk := range mergedChks {
			if err := c.chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}

		for _, l := range lset {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)
		}
		c.mtx.Unlock()
		//这里自己有锁了
		postings.Add(i, lset)

		i++
	}

	// 遍历 merger 或 当前head flush下来的CompactionSeriesSet：所有series的集合
	//这里可以开始基于series的concurrent 操作

	/*re:
	for true { //这个 Next()depends on whether 你是 CompactionSeriesSet 还是merger
		// 如果是CompactionSeriesSet， Next() 返回下一个series对应的各种信息 label set / chunks / interval (删掉的时间段/有效的时间段)
		// 如果是merger，Next()根据label有序return下一个series （可以是组成merger的两个seriesSet里任意的一个series）. 因为是有序，如果两个seriesSet包含同一个series时，这个series的chunk和interval会被合并，加上label Set 一起返回

		numOfWorkers := 8

		worker0ch := make(chan struct{}, 1)
		worker1ch := make(chan struct{}, 1)
		worker2ch := make(chan struct{}, 1)
		worker3ch := make(chan struct{}, 1)
		worker4ch := make(chan struct{}, 1)
		worker5ch := make(chan struct{}, 1)
		worker6ch := make(chan struct{}, 1)
		worker7ch := make(chan struct{}, 1)
		worker0ch <- struct{}{}

		var wg sync.WaitGroup
		for k := 0; k < numOfWorkers; k++ {
			j := k

			if !set.Next() {
				break re //跳到re 跳出for 循环
			}

			lset_global, chks_global, dranges_global := set.At() // The chunks here are not fully deleted.
			//lset_local := lset_global
			//chks_local := chks_global
			//dranges_local := dranges_global
			lset_local := make(labels.Labels, len(lset_global))
			copy(lset_local, lset_global)
			chks_local := make([]chunks.Meta, len(chks_global))
			copy(chks_local, chks_global)
			dranges_local := make([]Interval, len(dranges_global))
			copy(dranges_local, dranges_global)

			//TODO 到时候直接拆解Next函数

			//va := lset_local.Get("host")
			//println(j, "outside go func", va, &lset_global, &lset_local)

			//是同一个set 被多个goroutine共用， call的都是一个series, 所以用一个tmp 变量传进去 set是只读的所以没事

			wg.Add(1)
			//thread_num 是有序的  label越小 thread num越小
			go func(thread_num int) (err error) {

				select {
				case <-c.ctx.Done():
					return c.ctx.Err()
				default:
				}

				//va1 := lset_local.Get("host")
				//println(j, "inside go func", va1, &lset_local)

				if overlapping {
					// If blocks are overlapping, it is possible to have unsorted chunks.
					sort.Slice(chks_local, func(i, j int) bool {
						return chks_local[i].MinTime < chks_local[j].MinTime
					})
				}

				// Skip the series with all deleted chunks.
				if len(chks_local) == 0 {
					runtime.Goexit()
				}
				//只是对该series的chunks里的每个chunk做一些检查 如果有问题就re encode，暂时没看懂具体做的什么
				for i, chk := range chks_local {
					// Re-encode head chunks that are still open (being appended to) or
					// outside the compacted MaxTime range.
					// The chunk.Bytes() method is not safe for open chunks hence the re-encoding.
					// This happens when snapshotting the head block.
					//
					// Block time range is half-open: [meta.MinTime, meta.MaxTime) and
					// chunks are closed hence the chk.MaxTime >= meta.MaxTime check.
					//
					// TODO think how to avoid the typecasting to verify when it is head block.
					if _, isHeadChunk := chk.Chunk.(*safeChunk); isHeadChunk && chk.MaxTime >= meta.MaxTime {
						dranges_local = append(dranges_local, Interval{Mint: meta.MaxTime, Maxt: math.MaxInt64})

					} else
					// Sanity check for disk blocks.
					// chk.MaxTime == meta.MaxTime shouldn't happen as well, but will brake many users so not checking for that.
					if chk.MinTime < meta.MinTime || chk.MaxTime > meta.MaxTime {
						return errors.Errorf("found chunk with minTime: %d maxTime: %d outside of compacted minTime: %d maxTime: %d",
							chk.MinTime, chk.MaxTime, meta.MinTime, meta.MaxTime)
					}

					if len(dranges_local) > 0 {
						// Re-encode the chunk to not have deleted values.
						if !chk.OverlapsClosedInterval(dranges_local[0].Mint, dranges_local[len(dranges_local)-1].Maxt) {
							continue
						}
						newChunk := chunkenc.NewXORChunk()
						app, err := newChunk.Appender()
						if err != nil {
							return err
						}

						//TODO 这里似乎也有冲突 不过我们目前没有删除 所以可以先放着
						delIter.it = chk.Chunk.Iterator(delIter.it)
						delIter.intervals = dranges_local

						var (
							t int64
							v float64
						)
						for delIter.Next() {
							t, v = delIter.At()
							app.Append(t, v)
						}
						if err := delIter.Err(); err != nil {
							return errors.Wrap(err, "iterate chunk while re-encoding")
						}

						chks_local[i].Chunk = newChunk
						chks_local[i].MaxTime = t
					}
				}

				mergedChks := chks_local
				if overlapping {
					mergedChks, err = chunks.MergeOverlappingChunks(chks_local)
					if err != nil {
						return errors.Wrap(err, "merge overlapping chunks")
					}
				}

				//println(j, " waiting for the channel-", thread_num)

				if thread_num == 0 {
					select {
					case <-worker0ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 1 {
					select {
					case <-worker1ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 2 {
					select {
					case <-worker2ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 3 {
					select {
					case <-worker3ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 4 {
					select {
					case <-worker4ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 5 {
					select {
					case <-worker5ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 6 {
					select {
					case <-worker6ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				} else if thread_num == 7 {
					select {
					case <-worker7ch:
						//fmt.Println("get channel thread-", thread_num)
					}
				}

				//因为是time based block， 所有series的chunk和index都在一个block里，哪怕按照series并行了，在写block的时候还是需要lock. 不过因为我们前面利用channel实现了有序写 所以等同于有lock了
				//c.mtx.Lock()

				//简单看了 chunks.go 里实现的WriteChunks, 应该就是把属于一个series的多个chunk写一起
				//官方的话是：serializes a time block of chunked series data
				if err := chunkw.WriteChunks(mergedChks...); err != nil {
					return errors.Wrap(err, "write chunks")
				}

				// 具体实现在index.go里, 插入series, 构建到chunk的index
				if err := indexw.AddSeries(i, lset_local, mergedChks...); err != nil {
					return errors.Wrap(err, "add series")
				}

				meta.Stats.NumChunks += uint64(len(mergedChks))
				meta.Stats.NumSeries++
				for _, chk := range mergedChks {
					meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
				}

				//TODO 这里和上面的WriteChunks的区别是？？
				for _, chk := range mergedChks {
					if err := c.chunkPool.Put(chk.Chunk); err != nil {
						return errors.Wrap(err, "put chunk")
					}
				}

				for _, l := range lset_local {
					valset, ok := values[l.Name]
					if !ok {
						valset = stringset{}
						values[l.Name] = valset
					}
					valset.set(l.Value)
				}
				//c.mtx.Unlock()
				//这里自己有锁了
				postings.Add(i, lset_local)

				i++
				wg.Done()

				if thread_num == 0 {
					worker1ch <- struct{}{}
				} else if thread_num == 1 {
					worker2ch <- struct{}{}
				} else if thread_num == 2 {
					worker3ch <- struct{}{}
				} else if thread_num == 3 {
					worker4ch <- struct{}{}
				} else if thread_num == 4 {
					worker5ch <- struct{}{}
				} else if thread_num == 5 {
					worker6ch <- struct{}{}
				} else if thread_num == 6 {
					worker7ch <- struct{}{}
				} else if thread_num == 7 {

				}
				return nil
			}(j)
		}
		wg.Wait()
	}*/

	if set.Err() != nil {
		return errors.Wrap(set.Err(), "iterate compaction set")
	}

	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			s = append(s, x)
		}
		if err := indexw.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}

	for _, l := range postings.SortedKeys() {
		if err := indexw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

type compactionSeriesSet struct {
	p          index.Postings
	index      IndexReader
	chunks     ChunkReader
	tombstones TombstoneReader

	l         labels.Labels
	c         []chunks.Meta
	intervals Intervals
	err       error
}

func newCompactionSeriesSet(i IndexReader, c ChunkReader, t TombstoneReader, p index.Postings) *compactionSeriesSet {
	return &compactionSeriesSet{
		index:      i,
		chunks:     c,
		tombstones: t,
		p:          p,
	}
}

func (c *compactionSeriesSet) Next() bool {
	if !c.p.Next() {
		return false
	}
	var err error

	//c.p.At()大概是return一个series Ref，还没看posting的构造 不知道为什么posting的iterator会return series Ref
	c.intervals, err = c.tombstones.Get(c.p.At()) //这里的intervals是deleted time range (tombstones) 也许后续会有反转来获得真的intervals
	if err != nil {
		c.err = errors.Wrap(err, "get tombstones")
		return false
	}

	//根据c.p.At() return的series Ref 去index里获得对应series的label set 和 chunks (chunks = a set of chunks 本质上是 []chunks.meta   Meta holds information about a chunk of data. )
	if err = c.index.Series(c.p.At(), &c.l, &c.c); err != nil {
		c.err = errors.Wrapf(err, "get series %d", c.p.At())
		return false
	}

	// Remove completely deleted chunks.
	if len(c.intervals) > 0 {
		chks := make([]chunks.Meta, 0, len(c.c))
		for _, chk := range c.c {
			if !(Interval{chk.MinTime, chk.MaxTime}.isSubrange(c.intervals)) {
				chks = append(chks, chk)
			}
		}

		c.c = chks
	}

	//检查在Next()里获得的chunks 是不是在chunk reader里也有存在
	for i := range c.c {
		chk := &c.c[i]

		chk.Chunk, err = c.chunks.Chunk(chk.Ref)
		if err != nil {
			c.err = errors.Wrapf(err, "chunk %d not found", chk.Ref)
			return false
		}
	}

	return true
}

func (c *compactionSeriesSet) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.p.Err()
}

func (c *compactionSeriesSet) At() (labels.Labels, []chunks.Meta, Intervals) {
	return c.l, c.c, c.intervals
}

type compactionMerger struct {
	a, b ChunkSeriesSet

	aok, bok  bool
	l         labels.Labels
	c         []chunks.Meta
	intervals Intervals
}

func newCompactionMerger(a, b ChunkSeriesSet) (*compactionMerger, error) {
	c := &compactionMerger{
		a: a,
		b: b,
	}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	c.aok = c.a.Next()
	c.bok = c.b.Next()

	return c, c.Err()
}

func (c *compactionMerger) compare() int {
	if !c.aok {
		return 1
	}
	if !c.bok {
		return -1
	}
	a, _, _ := c.a.At()
	b, _, _ := c.b.At()
	return labels.Compare(a, b)
}

func (c *compactionMerger) Next() bool {
	if !c.aok && !c.bok || c.Err() != nil {
		return false
	}
	// While advancing child iterators the memory used for labels and chunks
	// may be reused. When picking a series we have to store the result.
	var lset labels.Labels
	var chks []chunks.Meta

	//比较两个SeriesSet各自的下一个series的LabelSet来比较
	d := c.compare()
	if d > 0 {
		lset, chks, c.intervals = c.b.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.bok = c.b.Next()
	} else if d < 0 {
		lset, chks, c.intervals = c.a.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.aok = c.a.Next()
	} else {
		// Both sets contain the current series. Chain them into a single one.
		l, ca, ra := c.a.At()
		_, cb, rb := c.b.At()

		for _, r := range rb {
			ra = ra.add(r)
		}

		c.l = append(c.l[:0], l...)
		c.c = append(append(c.c[:0], ca...), cb...)
		c.intervals = ra

		c.aok = c.a.Next()
		c.bok = c.b.Next()
	}

	return true
}

func (c *compactionMerger) Err() error {
	if c.a.Err() != nil {
		return c.a.Err()
	}
	return c.b.Err()
}

func (c *compactionMerger) At() (labels.Labels, []chunks.Meta, Intervals) {
	return c.l, c.c, c.intervals
}
