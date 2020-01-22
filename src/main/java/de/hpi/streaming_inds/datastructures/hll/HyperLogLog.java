package de.hpi.streaming_inds.datastructures.hll;

import de.hpi.streaming_inds.datastructures.CardinalityEstimatable;
import lombok.NoArgsConstructor;

import java.io.*;

/**
 * Reduced implementation of HyperLogLog (HLL) algorithm based on clearspring.analytics streamlib
 * This is, just like the original implementation, completely broken
 */
@NoArgsConstructor(force = true)
public class HyperLogLog implements Serializable, CardinalityEstimatable {
    private static final long serialVersionUID = 2247928718406874368L;

    public final RegisterSet registerSet;
    public final int log2m;
    public final double alphaMM;


    @Override
    public boolean deleteWouldChange(long hash) {
        throw new UnsupportedOperationException("HyperLogLog does not support deletions");
    }

    @Override
    public boolean delete(long hash) {
        throw new UnsupportedOperationException("HyperLogLog does not support deletions");
    }

    @Override
    public boolean putWouldChange(long hash) {
        return true; //does not matter because the HLL implementation is completely broken.
    }

    @Override
    public boolean put(long hash) {
        return !this.offerHashed(hash);
    }

    @Override
    public boolean containsAll(CardinalityEstimatable other) {
        return this.includes((HyperLogLog) other);
    }

    @Override
    public CardinalityEstimatable deepCopy() {
        return new HyperLogLog(this);
    }


    /**
     * Create a new HyperLogLog instance by deep-copying another one.
     * @param other - the HyperLogLog instance to copy
     */
    public HyperLogLog(HyperLogLog other) {
        this.log2m = other.log2m;
        this.alphaMM = other.alphaMM;
        this.registerSet = new RegisterSet(other.registerSet);
    }

    /**
     * Create a new HyperLogLog instance using the specified standard deviation.
     *
     * @param rsd - the relative standard deviation for the counter.
     *            smaller values create counters that require more space.
     */
    public HyperLogLog(double rsd) {
        this(log2m(rsd));
    }


    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m - the number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m) {
        this(log2m, new RegisterSet(1 << log2m));
    }

    /**
     * Creates a new HyperLogLog instance using the given registers.
     *
     * @param registerSet - the initial values for the register set
     */
    @Deprecated
    public HyperLogLog(int log2m, RegisterSet registerSet) {
        if (log2m < 0 || log2m > 30) {
            throw new IllegalArgumentException("log2m argument is "
                    + log2m + " and is outside the range [0, 30]");
        }
        this.registerSet = registerSet;
        this.log2m = log2m;
        int m = 1 << this.log2m;

        alphaMM = getAlphaMM(log2m, m);
    }


    public boolean offerHashed(long hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = (int) (hashedValue >>> (Long.SIZE - log2m));
        final int r = Long.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return !registerSet.updateIfGreater(j, r);
    }

    @SuppressWarnings("unused")
    public boolean offerHashed(int hashedValue) {
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = hashedValue >>> (Integer.SIZE - log2m);
        final int r = Integer.numberOfLeadingZeros((hashedValue << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
        return !registerSet.updateIfGreater(j, r);
    }

    public boolean includes(HyperLogLog other) {
        int[] bBits = this.registerSet.readOnlyBits();
        int[] aBits = other.registerSet.readOnlyBits();
        for (int bucket = 0; bucket < bBits.length; ++bucket) {
            int aBit = aBits[bucket];
            int bBit = bBits[bucket];
            for (int j = 0; j < RegisterSet.LOG2_BITS_PER_WORD; ++j) {
                int mask = 31 << (RegisterSet.REGISTER_SIZE * j);
                int aVal = aBit & mask;
                int bVal = bBit & mask;
                if (bVal < aVal) {
                    return false;
                }
            }
        }
        return true;
    }

    public int sizeof() {
        return registerSet.size * 4;
    }

    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    private static double getAlphaMM(final int p, final int m) {
        // See the paper.
        switch (p) {
            case 4:
                return 0.673 * m * m;
            case 5:
                return 0.697 * m * m;
            case 6:
                return 0.709 * m * m;
            default:
                return (0.7213 / (1 + 1.079 / m)) * m * m;
        }
    }

}