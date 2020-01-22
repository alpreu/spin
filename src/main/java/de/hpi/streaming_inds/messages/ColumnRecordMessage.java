package de.hpi.streaming_inds.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class ColumnRecordMessage implements Serializable {
    private static final long serialVersionUID = 3759578103596250017L;
    private final long rowId;
    private final long hash;
    private final boolean isDelete;
    private final boolean isLastRowInBatch;
}
