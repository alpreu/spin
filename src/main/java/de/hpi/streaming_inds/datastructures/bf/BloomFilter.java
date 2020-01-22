package de.hpi.streaming_inds.datastructures.bf;

import de.hpi.streaming_inds.datastructures.CardinalityEstimatable;
import lombok.NoArgsConstructor;

/**
 * Reduced Bloom filter implementation for FAIDACore.
 */
@NoArgsConstructor(force = true)
public class BloomFilter implements CardinalityEstimatable {

    public long[] bits;
    public final long bitCapacity;

    @Override
    public boolean deleteWouldChange(long hash) {
        throw new UnsupportedOperationException("BloomFilter does not support deletions");
    }

    @Override
    public boolean delete(long hash) {
        throw new UnsupportedOperationException("BloomFilter does not support deletions");
    }

    @Override
    public boolean putWouldChange(long hash) {
        return !this.containsHash(hash);
    }

    @Override
    public boolean put(long hash) {
        boolean changed = !this.containsHash(hash);
        this.setHash(hash);
        return changed;
    }

    @Override
    public boolean containsAll(CardinalityEstimatable other) {
        return this.containsAll((BloomFilter) other);
    }

    @Override
    public CardinalityEstimatable deepCopy() {
        return new BloomFilter(this);
    }

    private BloomFilter(BloomFilter other) {
        this.bitCapacity = other.bitCapacity;

        long[] temp = new long[other.bits.length];
        System.arraycopy(other.bits, 0, temp, 0, other.bits.length);
        this.bits = temp;
    }

    public BloomFilter(int capacityInBytes) {
        this.bits = new long[(capacityInBytes + 7) / 8];
        this.bitCapacity = capacityInBytes * 8L;
    }

    private void setHash(long hash) {
        hash = Math.abs(hash);
        this.setBit(hash % this.bitCapacity);
    }

    private void setBit(long position) {
        int field = (int) (position / 64);
        int offset = (int) (position % 64);
        this.bits[field] |= (1L << offset);
    }

    private boolean containsAll(BloomFilter that) {
        if (this.bitCapacity != that.bitCapacity) throw new IllegalArgumentException();

        for (int i = 0; i < this.bits.length; i++) {
            if ((that.bits[i] & ~this.bits[i]) != 0L) return false;
        }
        return true;
    }

    // modifying original implementation
    private boolean containsHash(long hash) {
        hash = Math.abs(hash);
        long position = hash % this.bitCapacity;
        int field = (int) (position / 64);
        return this.bits[field] != 0L;
    }

}