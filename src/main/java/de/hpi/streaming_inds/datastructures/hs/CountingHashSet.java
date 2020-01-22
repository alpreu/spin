package de.hpi.streaming_inds.datastructures.hs;

import de.hpi.streaming_inds.datastructures.CardinalityEstimatable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

//Performance might improve by providing a custom map implementation that NOPs the default hash function
public class CountingHashSet implements CardinalityEstimatable {
    private HashMap<Long, Long> map;

    private CountingHashSet(CountingHashSet other) {
        this.map = new HashMap<>(other.map);
    }

    public CountingHashSet(int capacityRequest) {
        this.map = new HashMap<>(capacityRequest);
    }


    @Override
    public boolean deleteWouldChange(long hash) {
        Long currentValue = this.map.get(hash);
        if (currentValue == null) return false; //for robustness in unusual test-cases
        return currentValue == 1; //would only change if result after decrementing is zero
    }

    @Override
    public boolean delete(long hash) {
        Long currentValue = this.map.get(hash);
        if (currentValue == null) return false; //for robustness in unusual test-cases
        if (currentValue == 1) {
            this.map.remove(hash); //if result after decrementing would be zero we remove the mapping
            return true;
        } else {
            this.map.put(hash, --currentValue); //just decrement the current count
            return false;
        }
    }

    @Override
    public boolean putWouldChange(long hash) {
       return !this.map.containsKey(hash);
    }

    @Override
    public boolean put(long hash) {
        long newValue = map.merge(hash, 1L, Long::sum);
        return newValue == 1L;
    }

    @Override
    public boolean containsAll(CardinalityEstimatable other) {
        CountingHashSet otherEhs = (CountingHashSet) other;
        return this.map.keySet().containsAll(otherEhs.map.keySet());
    }

    @Override
    public CardinalityEstimatable deepCopy() {
        return new CountingHashSet(this);
    }


    public int getCardinality() {
        return this.map.size();
    }

    public Set<Long> getHashes() {
        return this.map.keySet();
    }

    public Set<Map.Entry<Long, Long>> getEntries() {
        return this.map.entrySet();
    }

}
