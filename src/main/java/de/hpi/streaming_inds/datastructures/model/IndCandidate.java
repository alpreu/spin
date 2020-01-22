package de.hpi.streaming_inds.datastructures.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

@EqualsAndHashCode
@NoArgsConstructor(force = true)
public class IndCandidate {
    @Getter
    private ColumnCombination lhs;
    @Getter
    private ColumnCombination rhs;

    public IndCandidate(int lhsId, int rhsId) {
        this(new ColumnCombination(lhsId), new ColumnCombination(rhsId));
    }

    public IndCandidate(ColumnCombination lhs, ColumnCombination rhs) {
        if (lhs.size() != rhs.size()) {
            throw new RuntimeException("Cannot instantiate IndCandidate, size of lhs and rhs do not match");
        }
        if (!ColumnCombination.areDisjoint(lhs, rhs)) {
            throw new RuntimeException("Cannot instantiate IndCandidate, lhs and rhs are not disjoint");
        }
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public int size() {
        return lhs.size();
    }

    public String identifier() {
        return lhs.identifier() + "-=" + rhs.identifier();
    }


    @Override
    public String toString() {
        return lhs + " ⊆ " + rhs;
    }

    public static List<IndCandidate> generateUnaryCandidates(int numberOfColumns) {
        ArrayList<IndCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < numberOfColumns; i++) {
            for (int j = 0; j < numberOfColumns; j++) {
                if (i==j) continue;
                ColumnCombination lhs = new ColumnCombination(i);
                ColumnCombination rhs = new ColumnCombination(j);
                IndCandidate candidate = new IndCandidate(lhs, rhs);
                candidates.add(candidate);
            }
        }
        return candidates.stream().sorted(comparator).collect(Collectors.toList());
    }

    public IndCandidate getReverseCandidate() {
        return new IndCandidate(this.rhs, this.lhs);
    }

    public static List<IndCandidate> generateUnaryCandidatesWithLhs(int columnInd, int numberOfColumns) {
        ArrayList<IndCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < numberOfColumns; i++) {
            if (i == columnInd) continue;
            ColumnCombination lhs = new ColumnCombination(columnInd);
            ColumnCombination rhs = new ColumnCombination(i);
            IndCandidate candidate = new IndCandidate(lhs, rhs);
            candidates.add(candidate);
        }
        return candidates.stream().sorted(comparator).collect(Collectors.toList());
    }

    public static final Comparator<IndCandidate> comparator = Comparator
            .comparing(IndCandidate::getLhs, ColumnCombination.comparator)
            .thenComparing(IndCandidate::getRhs, ColumnCombination.comparator);
}
