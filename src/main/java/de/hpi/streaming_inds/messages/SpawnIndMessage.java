package de.hpi.streaming_inds.messages;

import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class SpawnIndMessage implements Serializable {
    private static final long serialVersionUID = 7029160899337915713L;
    private final long rowId;
    private final IndCandidate candidate;
    private final boolean isValid;

}
