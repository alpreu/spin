package de.hpi.streaming_inds.datastructures.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class Row {
    public final long id;
    public final long[] columnHashes;
    public final boolean isDelete;
    public final boolean isLastRow;
}
