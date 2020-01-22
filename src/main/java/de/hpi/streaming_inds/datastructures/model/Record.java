package de.hpi.streaming_inds.datastructures.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor @NoArgsConstructor(force = true)
public class Record {
    public final long rowId;
    public final long columnId;
    public final long hash;
    public final boolean isLastRecord;
}
