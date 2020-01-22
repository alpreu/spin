package de.hpi.streaming_inds.datastructures.chll;


import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Random;

@NoArgsConstructor(force = true)
public class CountingRegisterSet {
    public final int hashSize;
    public final int b;
    public final int numberOfBuckets;
    public final short[][] M;
    private final Random r;


    private CountingRegisterSet(CountingRegisterSet other) {
        this.b = other.b;
        this.hashSize = other.hashSize;
        this.numberOfBuckets = other.numberOfBuckets;
        this.M = Arrays.stream(other.M).map(short[]::clone).toArray($ -> other.M.clone()); //deepcopy 2d matrix (very slow)
        this.r = other.r;
    }

    public CountingRegisterSet(int b, int hashSize) {
        this.b = b;
        this.hashSize = hashSize;
        this.numberOfBuckets = 1 << b; //m = 2^b
        this.M = new short[this.hashSize - b + 1][numberOfBuckets];
        this.r = new Random();
    }

    public CountingRegisterSet deepCopy() {
        return new CountingRegisterSet(this);
    }





    private void incrementWithProbability(int i, int z) {
        double probability = Math.pow(0.5, (this.M[z][i] - 128));
        if (r.nextDouble() <= probability) {
            this.M[z][i]++;
        }
    }

    private void decrementWithProbability(int i, int z) {
        double probability = Math.pow(0.5, (this.M[z][i] - 128));
        if (r.nextDouble() <= probability) {
            this.M[z][i]--;
        }
    }

    public int get(int i, int z) {
        return this.M[z][i];
    }

    boolean pIncrement(int i, int z) {
        int oldVal = this.M[z][i];
        if (oldVal <= 128) {
            this.M[z][i]++;
            if (oldVal == 0) {
                return true;
            }
        } else {
            incrementWithProbability(i, z);
        }
        return false;
    }

    boolean pDecrement(int i, int z) {
        int oldVal = this.M[z][i];
        if (oldVal == 0) return false;
        if (oldVal <= 128) {
            this.M[z][i]--;
            if (this.M[z][i] == 0) {
                return true;
            }
        } else {
            decrementWithProbability(i, z);
        }
        return false;
    }


    @Override
    public String toString() {
        return Arrays.toString(M);
    }


}
