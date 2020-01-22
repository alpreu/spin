package de.hpi.streaming_inds.messages.registration;

import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class UnaryCandidateRegistrationMessage implements Serializable {
    private static final long serialVersionUID = 3393954150762121785L;
    final IndCandidate candidate;
}
