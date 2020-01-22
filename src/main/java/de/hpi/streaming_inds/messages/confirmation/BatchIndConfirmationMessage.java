package de.hpi.streaming_inds.messages.confirmation;

import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class BatchIndConfirmationMessage implements Serializable {
    private static final long serialVersionUID = -1512901976930832243L;
    private final int columnId;
    private final ArrayList<IndCandidate> finishedCandidates;

}
