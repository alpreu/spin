package de.hpi.streaming_inds.messages;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class NumberOfColumnsMessage implements Serializable {
    private static final long serialVersionUID = 8344088616648833049L;
    final int numberOfColumns;
}
