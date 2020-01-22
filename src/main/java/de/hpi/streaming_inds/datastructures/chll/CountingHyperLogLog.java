package de.hpi.streaming_inds.datastructures.chll;

import akka.japi.Pair;
import com.google.common.annotations.VisibleForTesting;
import de.hpi.streaming_inds.datastructures.CardinalityEstimatable;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Reduced implementation of HyperLogLog (HLL) algorithm based on clearspring.analytics streamlib
 */
@NoArgsConstructor(force = true)
public class CountingHyperLogLog implements Serializable, CardinalityEstimatable {
    private static final long serialVersionUID = 2247928718406874368L;
    public final int hashSize;
    public final CountingRegisterSet registerSet;
    public final int log2m;


    @Override
    public boolean deleteWouldChange(long hash) {
        //would only change if result after decrementing is zero
        if (hashSize == Integer.SIZE) {
            int hashedValue = (int) hash;
            Pair<Integer, Integer> indices = getIndices(hashedValue);
            int counter = registerSet.get(indices.first(), indices.second());
            return counter == 1;
        } else {
            Pair<Integer, Integer> indices = getIndices(hash);
            int counter = registerSet.get(indices.first(), indices.second());
            return counter == 1;
        }
    }

    @Override
    public boolean delete(long hash) {
        return this.deleteHashed(hash);
    }

    @Override
    public boolean putWouldChange(long hash) {
        if (hashSize == Integer.SIZE) {
            int hashedValue = (int) hash;
            Pair<Integer, Integer> indices = getIndices(hashedValue);
            int counter = registerSet.get(indices.first(), indices.second());
            return counter == 0;
        } else {
            Pair<Integer, Integer> indices = getIndices(hash);
            int counter = registerSet.get(indices.first(), indices.second());
            return counter == 0;
        }
    }

    @Override
    public boolean put(long hash) {
        return this.offerHashed(hash);
    }

    @Override
    public boolean containsAll(CardinalityEstimatable other) {
        return this.includes((CountingHyperLogLog) other);
    }

    @Override
    public CardinalityEstimatable deepCopy() {
        return new CountingHyperLogLog(this);
    }



    private CountingHyperLogLog(CountingHyperLogLog other) {
        this.log2m = other.log2m;
        this.hashSize = other.hashSize;
        this.registerSet = other.registerSet.deepCopy();
    }

    public CountingHyperLogLog(double rsd, int hashSize) {
        this(log2m(rsd), hashSize);
    }

    public CountingHyperLogLog(int log2m, int hashSize) {
        if (hashSize != 32 && hashSize != 64) throw new IllegalArgumentException("hashSize " + hashSize + " is neither 32 nor 64");
        if (log2m < 0 || log2m > 30) throw new IllegalArgumentException("log2m of " + log2m + " is outside the allowed range [0, 30]");
        this.log2m = log2m;
        this.hashSize = hashSize;
        this.registerSet = new CountingRegisterSet(log2m, this.hashSize);
    }

    private Pair<Integer, Integer> getIndices(long hashedValue) {
        final int i = (int) (hashedValue >>> (Long.SIZE - log2m)); //bucket index is first b bits (b is log2m)
        final int z = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1); //number of zeros in remaining bits
        return new Pair<>(i, z);
    }

    private Pair<Integer, Integer> getIndices(int hashedValue) {
        final int i = hashedValue >>> (Integer.SIZE - log2m); //bucket index is first b bits (b is log2m)
        final int z = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1); //number of zeros in remaining bits
        return new Pair<>(i, z);
    }

    private boolean offerHashed(int hashedValue) {
        Pair<Integer, Integer> indices = getIndices(hashedValue);
        return registerSet.pIncrement(indices.first(), indices.second());
    }

    private boolean deleteHashed(int hashedValue) {
        Pair<Integer, Integer> indices = getIndices(hashedValue);
        return registerSet.pDecrement(indices.first(), indices.second());
    }

    public boolean offerHashed(long hashedValue) {
        if (hashSize == Integer.SIZE) {
           return offerHashed((int) hashedValue);
        } else {
            Pair<Integer, Integer> indices = getIndices(hashedValue);
            return registerSet.pIncrement(indices.first(), indices.second());
        }
    }

    public boolean deleteHashed(long hashedValue) {
        if (hashSize == Integer.SIZE) {
           return deleteHashed((int) hashedValue);
        } else {
            Pair<Integer, Integer> indices = getIndices(hashedValue);
            return registerSet.pDecrement(indices.first(), indices.second());
        }
    }


    public boolean includes(CountingHyperLogLog other) {
        short[][] bM = this.registerSet.M;
        short[][] aM = other.registerSet.M;

        for (int bucket = 0; bucket < bM[0].length; ++bucket) {
            for (int leadingZeroes = (bM.length - 1); leadingZeroes >= 0; --leadingZeroes) {
                //find the highest number of leading zeros in A and check if B has that 'count bucket' also incremented
                if (aM[leadingZeroes][bucket] != 0) {
                    if (bM[leadingZeroes][bucket] == 0) return false;
                }
            }
        }

        return true;
    }

    @VisibleForTesting
    public int get(long hash) {
        if (hashSize == Integer.SIZE) {
            int hashedValue = (int) hash;
            Pair<Integer, Integer> indices = getIndices(hashedValue);
            return registerSet.get(indices.first(), indices.second());
        } else {
            Pair<Integer, Integer> indices = getIndices(hash);
            return registerSet.get(indices.first(), indices.second());
        }
    }


    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

}