package de.hpi.streaming_inds.messages;

import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class IndRecheckedMessage implements Serializable {
    private static final long serialVersionUID = 5341143856734494613L;
    final long rowId;
    final IndCandidate candidate;
    final boolean isValid;
    final boolean isGeneralRecheck;
}
