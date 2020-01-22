package de.hpi.streaming_inds.messages;


import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class OrchestratorConfigMessage implements Serializable {
    private static final long serialVersionUID = 4674318630178818808L;
    final ArrayList<Integer> assignedColumnIds;
    final int totalNumberOfColumns;
    final ActorRef globalRegisterActor;
    final ActorRef indValidityActor;


}
