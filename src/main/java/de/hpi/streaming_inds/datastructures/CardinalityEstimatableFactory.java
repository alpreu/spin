package de.hpi.streaming_inds.datastructures;

import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.datastructures.bf.BloomFilter;
import de.hpi.streaming_inds.datastructures.chll.CountingHyperLogLog;
import de.hpi.streaming_inds.datastructures.hll.HyperLogLog;
import de.hpi.streaming_inds.datastructures.hybrid.HybridCardinalityEstimatable;

public class CardinalityEstimatableFactory {

    public static CardinalityEstimatable createCardinalityEstimatable(ProbabilisticDatastructureType type) {

        if (type == ProbabilisticDatastructureType.HYPERLOGLOG) {
            return new HyperLogLog(ConfigurationSingleton.get().getCHllLog2M());
        } else if (type == ProbabilisticDatastructureType.BLOOMFILTER) {
            return new BloomFilter(ConfigurationSingleton.get().getBfCapacity());
        } else if (type == ProbabilisticDatastructureType.COUNTINGHYPERLOGLOG) {
            return new CountingHyperLogLog(ConfigurationSingleton.get().getCHllLog2M(), ConfigurationSingleton.get().getHashSize());
        } else if (type == ProbabilisticDatastructureType.HYBRID) {
            return new HybridCardinalityEstimatable(ConfigurationSingleton.get().getHybridThreshold(), ConfigurationSingleton.get().getCHllLog2M(), ConfigurationSingleton.get().getHashSize());
        }

        throw new IllegalArgumentException("ProbabilisticDatastructureType " + type + " unknown to Factory");
    }


}


