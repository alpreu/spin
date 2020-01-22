package de.hpi.streaming_inds.datastructures;

public interface CardinalityEstimatable {

    boolean deleteWouldChange(long hash);

    boolean delete(long hash);

    boolean putWouldChange(long hash);

    boolean put(long hash);

    boolean containsAll(CardinalityEstimatable other);

    CardinalityEstimatable deepCopy();


}
