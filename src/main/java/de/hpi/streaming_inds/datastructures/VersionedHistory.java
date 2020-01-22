package de.hpi.streaming_inds.datastructures;

import java.util.ArrayDeque;
import java.util.HashMap;

//array of pairs finding element is O(n)
//array or pairs adding element if we maintain pointer is O(1)

//treemap finding is O(log n)
//treemap insert is O(log n) + deletion of O(log n)

//hashmap finding is O(1)
//hashmap insert is O(1) + deletion of O(1)

public class VersionedHistory {
    private final int MAX_SIZE;
    private HashMap<Long, CardinalityEstimatable> history;
    private ArrayDeque<Long> versions;
    private final ProbabilisticDatastructureType datastructureType;


    public VersionedHistory(ProbabilisticDatastructureType datastructureType, int size) {
        this.MAX_SIZE = size;
        this.history = new HashMap<>();
        this.versions = new ArrayDeque<>(MAX_SIZE);
        this.datastructureType = datastructureType;
    }

    public boolean updateAndReturnIfChanged(Long version, long hash, boolean isDelete) {
        if (history.size() == 0) { //there is no previous version
            CardinalityEstimatable ce = CardinalityEstimatableFactory.createCardinalityEstimatable(datastructureType);
            if (isDelete) {
                ce.delete(hash); //does not really make sense that row 0 is a delete...
            } else {
                ce.put(hash);
            }
            this.put(version, ce);
            return true;
        } else {
            CardinalityEstimatable previous = this.get(version - 1);
            if (isDelete) {
                if (previous.deleteWouldChange(hash)) {
                    CardinalityEstimatable clone = previous.deepCopy();
                    clone.delete(hash);
                    this.put(version, clone);
                    return true;
                } else {
                    previous.delete(hash);
                    this.put(version, previous); //just copy the reference
                    return false;
                }
            } else {
                if (previous.putWouldChange(hash)) {
                    CardinalityEstimatable clone = previous.deepCopy();
                    clone.put(hash);
                    this.put(version, clone);
                    return true;
                } else {
                    previous.put(hash);
                    this.put(version, previous); //just copy the reference
                    return false;
                }
            }
        }
    }

    private void put(Long version, CardinalityEstimatable datastructure) {
        if (history.size() == MAX_SIZE) {
            Long oldestVersion = versions.removeFirst();
            history.remove(oldestVersion);
        }
        versions.add(version);
        history.put(version, datastructure);
    }

    public CardinalityEstimatable get(Long version) {
        return history.get(version);
    }

    @Override
    public String toString() {
        return history.toString();
    }
}
