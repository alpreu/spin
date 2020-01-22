package de.hpi.streaming_inds.messages;

import de.hpi.streaming_inds.datastructures.model.Row;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class BatchMessage implements Serializable {
    private static final long serialVersionUID = 7112146248979285119L;
    private ArrayList<Row> rows;
    private final boolean isLastBatch;
}
