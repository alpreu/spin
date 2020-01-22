package de.hpi.streaming_inds.messages;

import de.hpi.streaming_inds.datastructures.model.IndSide;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class DatastructureRequestMessage implements Serializable {
    private static final long serialVersionUID = -2068170507314607338L;
    final long rowId;
    final IndSide indSide;
}
