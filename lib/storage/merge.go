package storage

import (
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// mergeBlockStreams merges bsrs into bsw and updates ph.
//
// mergeBlockStreams returns immediately if stopCh is closed.
//
// rowsMerged is atomically updated with the number of merged rows during the merge.
func mergeBlockStreams(ph *partHeader, bsw *blockStreamWriter, bsrs []*blockStreamReader, stopCh <-chan struct{},
	retentionDeadline int64, rowsMerged, rowsDeleted *uint64) error {
	ph.Reset()

	bsm := bsmPool.Get().(*blockStreamMerger)
	bsm.Init(bsrs)
	err := mergeBlockStreamsInternal(ph, bsw, bsm, stopCh, retentionDeadline, rowsMerged, rowsDeleted)
	bsm.reset()
	bsmPool.Put(bsm)
	bsw.MustClose()
	if err == nil {
		return nil
	}
	return fmt.Errorf("cannot merge %d streams: %s: %w", len(bsrs), bsrs, err)
}

var bsmPool = &sync.Pool{
	New: func() interface{} {
		return &blockStreamMerger{}
	},
}

var errForciblyStopped = fmt.Errorf("forcibly stopped")

func mergeBlockStreamsInternal(ph *partHeader, bsw *blockStreamWriter, bsm *blockStreamMerger, stopCh <-chan struct{},
	retentionDeadline int64, rowsMerged, rowsDeleted *uint64) error {
	// Search for the first block to merge
	var pendingBlock *Block
	for bsm.NextBlock() {
		select {
		case <-stopCh:
			return errForciblyStopped
		default:
		}
		if bsm.Block.bh.MaxTimestamp < retentionDeadline {
			// Skip blocks out of the given retention.
			*rowsDeleted += uint64(bsm.Block.bh.RowsCount)
			continue
		}
		pendingBlock = getBlock()
		pendingBlock.CopyFrom(bsm.Block)
		break
	}
	if pendingBlock != nil {
		defer putBlock(pendingBlock)
	}

	// Merge blocks.
	tmpBlock := getBlock()
	defer putBlock(tmpBlock)
	for bsm.NextBlock() {
		select {
		case <-stopCh:
			return errForciblyStopped
		default:
		}
		if bsm.Block.bh.MaxTimestamp < retentionDeadline {
			// skip blocks out of the given retention.
			*rowsDeleted += uint64(bsm.Block.bh.RowsCount)
			continue
		}

		// Verify whether pendingBlock may be merged with bsm.Block (the current block).
		if !pendingBlock.bh.TraceID.Equals(bsm.Block.bh.TraceID) {
			// Fast path - blocks belong to distinct time series.
			// Write the pendingBlock and then deal with bsm.Block.
			if bsm.Block.bh.TraceID.Less(pendingBlock.bh.TraceID) {
				logger.Panicf("BUG: the next TraceID=%+v is smaller than the current TraceID=%+v", &bsm.Block.bh.TraceID, &pendingBlock.bh.TraceID)
			}
			bsw.WriteExternalBlock(pendingBlock, ph, rowsMerged)
			pendingBlock.CopyFrom(bsm.Block)
			continue
		}
		if pendingBlock.tooBig() && pendingBlock.bh.MaxTimestamp <= bsm.Block.bh.MinTimestamp {
			// Fast path - pendingBlock is too big and it doesn't overlap with bsm.Block.
			// Write the pendingBlock and then deal with bsm.Block.
			bsw.WriteExternalBlock(pendingBlock, ph, rowsMerged)
			pendingBlock.CopyFrom(bsm.Block)
			continue
		}

		// Slow path - pendingBlock and bsm.Block belong to the same time series,
		// so they must be merged.
		if err := unmarshalAndCalibrateScale(pendingBlock, bsm.Block); err != nil {
			return fmt.Errorf("cannot unmarshal and calibrate scale for blocks to be merged: %w", err)
		}
		tmpBlock.Reset()
		tmpBlock.bh.TraceID = bsm.Block.bh.TraceID
		mergeBlocks(tmpBlock, pendingBlock, bsm.Block)
		if len(tmpBlock.timestamps) <= maxRowsPerBlock {
			// More entries may be added to tmpBlock. Swap it with pendingBlock,
			// so more entries may be added to pendingBlock on the next iteration.
			tmpBlock.fixupTimestamps()
			pendingBlock, tmpBlock = tmpBlock, pendingBlock
			continue
		}

		// Write the first len(maxRowsPerBlock) of tmpBlock.timestamps to bsw,
		// leave the rest in pendingBlock.
		tmpBlock.nextIdx = maxRowsPerBlock
		pendingBlock.CopyFrom(tmpBlock)
		pendingBlock.fixupTimestamps()
		tmpBlock.nextIdx = 0
		tmpBlock.timestamps = tmpBlock.timestamps[:maxRowsPerBlock]
		tmpBlock.values = tmpBlock.values[:maxRowsPerBlock]
		tmpBlock.fixupTimestamps()
		bsw.WriteExternalBlock(tmpBlock, ph, rowsMerged)
	}
	if err := bsm.Error(); err != nil {
		return fmt.Errorf("cannot read block to be merged: %w", err)
	}
	if pendingBlock != nil {
		bsw.WriteExternalBlock(pendingBlock, ph, rowsMerged)
	}
	return nil
}

// mergeBlocks merges ib1 and ib2 to ob.
func mergeBlocks(ob, ib1, ib2 *Block) {
	ib1.assertMergeable(ib2)
	ib1.assertUnmarshaled()
	ib2.assertUnmarshaled()

	if ib1.bh.MaxTimestamp < ib2.bh.MinTimestamp {
		// Fast path - ib1 values have smaller timestamps than ib2 values.
		appendRows(ob, ib1)
		appendRows(ob, ib2)
		return
	}
	if ib2.bh.MaxTimestamp < ib1.bh.MinTimestamp {
		// Fast path - ib2 values have smaller timestamps than ib1 values.
		appendRows(ob, ib2)
		appendRows(ob, ib1)
		return
	}
	if ib1.nextIdx >= len(ib1.timestamps) {
		appendRows(ob, ib2)
		return
	}
	if ib2.nextIdx >= len(ib2.timestamps) {
		appendRows(ob, ib1)
		return
	}
	for {
		i := ib1.nextIdx
		ts2 := ib2.timestamps[ib2.nextIdx]
		for i < len(ib1.timestamps) && ib1.timestamps[i] <= ts2 {
			i++
		}
		ob.timestamps = append(ob.timestamps, ib1.timestamps[ib1.nextIdx:i]...)
		ob.values = append(ob.values, ib1.values[ib1.nextIdx:i]...)
		ib1.nextIdx = i
		if ib1.nextIdx >= len(ib1.timestamps) {
			appendRows(ob, ib2)
			return
		}
		ib1, ib2 = ib2, ib1
	}
}

func appendRows(ob, ib *Block) {
	ob.timestamps = append(ob.timestamps, ib.timestamps[ib.nextIdx:]...)
	ob.values = append(ob.values, ib.values[ib.nextIdx:]...)
}

func unmarshalAndCalibrateScale(b1, b2 *Block) error {
	if err := b1.UnmarshalData(); err != nil {
		return err
	}
	if err := b2.UnmarshalData(); err != nil {
		return err
	}
	return nil
}

func minUint8(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}
