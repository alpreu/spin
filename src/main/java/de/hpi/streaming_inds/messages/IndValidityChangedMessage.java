package de.hpi.streaming_inds.messages;

import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class IndValidityChangedMessage implements Serializable {
    private static final long serialVersionUID = 4866907287967397944L;
    final long rowId;
    final IndCandidate candidate;
    boolean isValid;


}
