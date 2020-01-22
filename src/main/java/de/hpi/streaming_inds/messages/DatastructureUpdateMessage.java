package de.hpi.streaming_inds.messages;


import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class DatastructureUpdateMessage implements Serializable {
    private static final long serialVersionUID = -5165507446483038576L;
    private final long rowId;
    private final boolean isChanged;
    private final boolean isLastRowInBatch;
    private final ActorRef sender;
}

