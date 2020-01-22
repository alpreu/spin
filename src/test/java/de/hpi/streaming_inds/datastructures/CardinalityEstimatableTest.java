package de.hpi.streaming_inds.datastructures;

import com.clearspring.analytics.hash.MurmurHash;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

abstract class CardinalityEstimatableTest {
    Function<Object, Long> hash = MurmurHash::hash64;
    CardinalityEstimatable cE;
    CardinalityEstimatable cE2;

    @Test
    void shouldChangeOnInsertion() {
        assert cE.put(hash.apply(1));
        assert cE.put(hash.apply(2));
        assert cE.put(hash.apply(3));
        assert cE.put(hash.apply(4));
    }

    @Test
    void shouldNotChangeOnReinsertion() {
        cE.put(hash.apply(1));
        cE.put(hash.apply(2));
        assert !cE.put(hash.apply(1));
        assert !cE.put(hash.apply(2));
    }

    @Test
    void shouldContainAllInsertedElements() {
        cE.put(hash.apply(1));
        cE.put(hash.apply(2));
        cE.put(hash.apply(3));
        cE.put(hash.apply(4));

        cE2.put(hash.apply(1));
        cE2.put(hash.apply(2));

        assert cE.containsAll(cE2);
    }

    @Test
    void shouldNotContainNonInsertedElements() {
        cE.put(hash.apply(1));
        cE.put(hash.apply(2));
        cE.put(hash.apply(3));
        cE.put(hash.apply(4));

        cE2.put(hash.apply(5));
        cE2.put(hash.apply(6));
        assert !cE.containsAll(cE2);
        cE2.put(hash.apply(1));
        cE2.put(hash.apply(2));
        cE2.put(hash.apply(3));
        cE2.put(hash.apply(4));
        assert !cE.containsAll(cE2);

    }

    @Test
    void deepCopyShouldBeIndependent() { //depends on working containsAll() implementation
        cE.put(hash.apply(1));
        cE.put(hash.apply(2));
        cE2 = cE.deepCopy();

        cE.put(hash.apply(3));
        assert cE.containsAll(cE2);
        assert !cE2.containsAll(cE);

    }
}