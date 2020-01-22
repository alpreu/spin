package de.hpi.streaming_inds.messages;

import de.hpi.streaming_inds.datastructures.model.IndSide;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class DatastructureResponseMessage implements Serializable {
    private static final long serialVersionUID = -5196339074838986494L;
    final IndSide indSide;
    Object datastructure;
}
