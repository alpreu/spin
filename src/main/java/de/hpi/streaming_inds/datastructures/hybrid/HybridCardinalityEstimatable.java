package de.hpi.streaming_inds.datastructures.hybrid;

import de.hpi.streaming_inds.datastructures.CardinalityEstimatable;
import de.hpi.streaming_inds.datastructures.chll.CountingHyperLogLog;
import de.hpi.streaming_inds.datastructures.hs.CountingHashSet;

import java.util.Map;


public class HybridCardinalityEstimatable implements CardinalityEstimatable {
    private final int threshold;
    private final int log2m;
    private final int hashSize;
    private boolean isUsingCountingSet = true;
    private CountingHashSet chs;
    private CountingHyperLogLog chll = null;


    private HybridCardinalityEstimatable(HybridCardinalityEstimatable other) {
        this.threshold = other.threshold;
        this.log2m = other.log2m;
        this.hashSize = other.hashSize;
        this.isUsingCountingSet = other.isUsingCountingSet;
        //only copy over the structures in use
        this.chs = other.isUsingCountingSet ? (CountingHashSet) other.chs.deepCopy() : null;
        this.chll = other.isUsingCountingSet ? null : (CountingHyperLogLog) other.chll.deepCopy();
    }

    public HybridCardinalityEstimatable(int threshold, int log2m, int hashSize) {
        this.threshold = threshold;
        this.log2m = log2m;
        this.hashSize = hashSize;
        this.chs = new CountingHashSet(threshold);
    }


    @Override
    public boolean deleteWouldChange(long hash) {
        if (isUsingCountingSet) {
            return this.chs.deleteWouldChange(hash);
        } else {
            return this.chll.deleteWouldChange(hash);
        }
    }

    @Override
    public boolean delete(long hash) {
        if (isUsingCountingSet) {
            return this.chs.delete(hash);
        } else {
            return this.chll.delete(hash);
        }
    }

    @Override
    public boolean putWouldChange(long hash) {
        if (isUsingCountingSet) {
            return this.chs.putWouldChange(hash);
        } else {
            return this.chll.putWouldChange(hash);
        }
    }

    @Override
    public boolean put(long hash) {
        if (isUsingCountingSet) {
            if (this.chs.getCardinality() == threshold) {
                switchToUsingCHLL();
                return true;
            } else {
                return this.chs.put(hash);
            }
        } else {
            return this.chll.put(hash);
        }
    }

    private void switchToUsingCHLL() {
        this.chll = new CountingHyperLogLog(this.log2m, this.hashSize);
        for (Map.Entry<Long, Long> entry: this.chs.getEntries()) {
            long entryHash = entry.getKey();
            long entryCount = entry.getValue();
            for (int i = 0; i < entryCount; i++) {
                this.chll.put(entryHash);
            }
        }
        this.isUsingCountingSet = false;
    }

    @Override
    public boolean containsAll(CardinalityEstimatable other) {
        HybridCardinalityEstimatable otherHCE = (HybridCardinalityEstimatable) other;

        if (this.isUsingCountingSet) {
            if (otherHCE.isUsingCountingSet) return this.chs.containsAll(otherHCE.chs);

            return false; //there are more distinct elements in CHLL, therefore it cannot be included
        } else {
            if (!otherHCE.isUsingCountingSet) return this.chll.containsAll(otherHCE.chll);
            //could possibly improve this by first estimating hll cardinality
            for(long hash: otherHCE.chs.getHashes()) {
                if (this.putWouldChange(hash)) return false;
            }
            return true;

        }
    }

    @Override
    public CardinalityEstimatable deepCopy() {
        return new HybridCardinalityEstimatable(this);
    }
}
