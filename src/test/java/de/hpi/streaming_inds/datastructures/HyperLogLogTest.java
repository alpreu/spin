package de.hpi.streaming_inds.datastructures;

import de.hpi.streaming_inds.datastructures.hll.HyperLogLog;
import org.junit.jupiter.api.BeforeEach;

public class HyperLogLogTest extends CardinalityEstimatableTest {

    @BeforeEach
    void setup() {
        cE = new HyperLogLog(0.01);
        cE2 = new HyperLogLog(0.01);
    }

}
