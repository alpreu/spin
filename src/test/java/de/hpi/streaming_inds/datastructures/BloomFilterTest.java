package de.hpi.streaming_inds.datastructures;

import de.hpi.streaming_inds.datastructures.bf.BloomFilter;
import org.junit.jupiter.api.BeforeEach;

public class BloomFilterTest extends CardinalityEstimatableTest {
    @BeforeEach
    void setup() {
        cE = new BloomFilter(32*1024);
        cE2 = new BloomFilter(32*1024);
    }
}
