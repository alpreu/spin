package de.hpi.streaming_inds.messages.confirmation;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;

@Data
@AllArgsConstructor
public class DatasetConfirmationMessage implements Serializable {
    private static final long serialVersionUID = 7371218638027793819L;
    private HashMap<String, String> metrics;
}
