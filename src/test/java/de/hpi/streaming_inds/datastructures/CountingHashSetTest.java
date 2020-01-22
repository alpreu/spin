package de.hpi.streaming_inds.datastructures;

import static org.assertj.core.api.Assertions.entry;
import de.hpi.streaming_inds.datastructures.hs.CountingHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;

public class CountingHashSetTest extends CardinalityEstimatableTest {

    @BeforeEach
    void setup() {
        cE = new CountingHashSet(1024);
        cE2 = new CountingHashSet(1024);
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
        CountingHashSet chs = (CountingHashSet) cE;

        assertThat(chs.getEntries()).containsExactlyInAnyOrder(entry(1L,1L), entry(2L, 3L));
        cE.delete(1);
        cE.delete(2);
        cE.delete(2);
        assertThat(chs.getEntries()).containsExactlyInAnyOrder(entry(2L, 1L));
        cE.delete(2);
        assertThat(chs.getEntries()).isEmpty();

    }
}
