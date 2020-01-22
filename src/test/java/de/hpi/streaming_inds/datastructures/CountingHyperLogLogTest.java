package de.hpi.streaming_inds.datastructures;

import de.hpi.streaming_inds.datastructures.chll.CountingHyperLogLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class CountingHyperLogLogTest extends CardinalityEstimatableTest {
    CountingHyperLogLog cHll;
    CountingHyperLogLog cHll2;

    @BeforeEach
    void setup() {
        cE = cHll = new CountingHyperLogLog(6, Long.SIZE);
        cE2 = cHll2 = new CountingHyperLogLog(6, Long.SIZE);
    }

    @Test
    void shouldInsertAndDelete32Bit() {
        cE = cHll = new CountingHyperLogLog(6, Integer.SIZE);
        int myHash = 0b00001100000000000000000000000000;
        int bucketIdx = 3; // == first 6 bits of hash (0b101000)
        int counterIdx = 26; // == number of leading zeros after the first 6 bits

        assert cHll.registerSet.M[counterIdx][bucketIdx] == 0;
        cHll.offerHashed(myHash);
        assert cHll.registerSet.M[counterIdx][bucketIdx] == 1;
        cHll.deleteHashed(myHash);
        assert cHll.registerSet.M[counterIdx][bucketIdx] == 0;
    }

    @Test
    void shouldInsertAndDeleteInCorrectBucketIfLeadingZeroesMaximal() {
        cE = cHll = new CountingHyperLogLog(6, Long.SIZE);
        long myHash = 0b0000110000000000000000000000000000000000000000000000000000000000L;
        int bucketIdx = 3; // == first 6 bits of hash (0b101000)
        int counterIdx = 58; // == number of leading zeros after the first 6 bits

        assert cHll.registerSet.M[counterIdx][bucketIdx] == 0;
        cHll.offerHashed(myHash);
        assert cHll.registerSet.M[counterIdx][bucketIdx] == 1;
        cHll.deleteHashed(myHash);
        assert cHll.registerSet.M[counterIdx][bucketIdx] == 0;
    }

    @Test
    void shouldDeleteInsertedValues() {
        cE = cHll = new CountingHyperLogLog(6, Long.SIZE);
        long myHash = 0b1010001001111000010010110001001000110111111000100111111011010011L;
        int bucketIdx = 40; // == first 6 bits of hash (0b101000)
        int counterIdx = 0; // == number of leading zeros after the first 6 bits

        assert cHll.registerSet.M[counterIdx][bucketIdx] == 0;
        cHll.offerHashed(myHash);
        assert cHll.registerSet.M[counterIdx][bucketIdx] == 1;
        cHll.deleteHashed(myHash);
        assert cHll.registerSet.M[counterIdx][bucketIdx] == 0;
    }

    @Test
    void shouldNotDecrementOnDeleteIfCounterZero() {
        cE = cHll = new CountingHyperLogLog(6, Long.SIZE);
        long myHash = 0b1010001001111000010010110001001000110111111000100111111011010011L;
        int bucketIdx = 40; // == first 6 bits of hash (0b101000)
        int counterIdx = 0; // == number of leading zeros after the first 6 bits

        assert cHll.registerSet.M[bucketIdx][counterIdx] == 0;
        cHll.deleteHashed(myHash);
        assert cHll.registerSet.M[bucketIdx][counterIdx] == 0;
    }

    @Test
    void shouldChangeOnDeletionIfCountIsOne() {
        cHll.offerHashed(hash.apply(1));
        cHll.offerHashed(hash.apply(2));
        assert cHll.deleteHashed(hash.apply(1));
        assert cHll.deleteHashed(hash.apply(2));
    }

    @Test
    void shouldNotChangeOnDeletionIfCountIsBig() {
        for (int i = 0; i < 100000; i++) {
            cHll.offerHashed(hash.apply(1));
            cHll.offerHashed(hash.apply(2));
        }
        assert !cHll.deleteHashed(hash.apply(1));
        assert !cHll.deleteHashed(hash.apply(2));
    }

    @Test
    void shouldNotChangeOnDeletionIfCountIsZero() {
        assert !cHll.deleteHashed(hash.apply(1));
        assert !cHll.deleteHashed(hash.apply(2));
    }

    @Test
    void shouldContainRelationAfterDeletion() {
        cHll.put(hash.apply(1));
        cHll.put(hash.apply(2));
        assert cHll.containsAll(cHll2);
        assert !cHll2.containsAll(cHll);

        cHll2.put(hash.apply(1));
        cHll2.put(hash.apply(2));
        cHll2.put(hash.apply(3));
        cHll2.put(hash.apply(4));
        assert !cHll.containsAll(cHll2);
        assert cHll2.containsAll(cHll);

        cHll2.deleteHashed(hash.apply(3));
        cHll2.deleteHashed(hash.apply(4));
        assert cHll.containsAll(cHll2);
        assert cHll2.containsAll(cHll);
    }

    @Test
    void deleteWouldChangeWorkingCorrectly() {
        cE.put(1);
        cE.put(2);
        cE.put(2);
        cE.put(2);

        assert cE.deleteWouldChange(1);
        assert !cE.deleteWouldChange(2);
    }

    @Test
    void deleteWorkingCorrectly() {
        cE.put(1);
        cE.put(2);
        cE.put(2);
        cE.put(2);
        CountingHyperLogLog chll = (CountingHyperLogLog) cE;

        assertThat(chll.get(1)).isEqualTo(1);
        assertThat(chll.get(2)).isEqualTo(3);
        cE.delete(1);
        cE.delete(2);
        cE.delete(2);
        assertThat(chll.get(1)).isEqualTo(0);
        assertThat(chll.get(2)).isEqualTo(1);
        cE.delete(2);
        assertThat(chll.get(2)).isEqualTo(0);


    }






}
