package de.hpi.streaming_inds.datastructures.model;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

@EqualsAndHashCode
@NoArgsConstructor(force = true)
public class ColumnCombination {

    @Getter
    private final Integer[] ids;

    public ColumnCombination(int... ids) {
        Integer[] givenIds = Arrays.stream(ids).boxed().toArray(Integer[]::new);
        if (ids.length != new HashSet<>(Arrays.asList(givenIds)).size()) {
            throw new RuntimeException("Cannot instantiate ColumnCombination with duplicate value");
        }
        this.ids = givenIds;
    }

    public String identifier() {
        String repr = "";
        for (int i = 0; i < ids.length; i++) {
            repr += ids[i];
            if (i + 1 == ids.length) {

            } else {
                repr += ".";
            }
        }
        return repr;
    }

    @Override
    public String toString() {
        String repr = "{";
        for (int i = 0; i < ids.length; i++) {
            repr += ids[i];
            if (i + 1 == ids.length) {
                repr += "}";
            } else {
                repr += ",";
            }
        }
        return repr;
    }

    public int size() {
        return ids.length;
    }

    public static boolean areDisjoint(ColumnCombination lhs, ColumnCombination rhs) {
        for (int lhsId:lhs.getIds()) {
            for (int rhsId:rhs.getIds()) {
                if (lhsId == rhsId) {
                    return false;
                }
            }
        }
        return true;
    }

    public static final Comparator<ColumnCombination> comparator = Comparator
            .comparingInt(ColumnCombination::size)
            .thenComparing(c -> c.getIds()[0]); //works only for unary INDs
}
