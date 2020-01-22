package de.hpi.streaming_inds.messages;

import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class RecheckIndMessage implements Serializable {
    private static final long serialVersionUID = 406860356583711946L;
    final long rowId;
    final ActorRef sender;
    final boolean isGeneralRecheck;
}
