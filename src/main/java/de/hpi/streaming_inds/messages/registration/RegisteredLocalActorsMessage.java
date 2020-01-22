package de.hpi.streaming_inds.messages.registration;

import akka.actor.ActorRef;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class RegisteredLocalActorsMessage implements Serializable {
    private static final long serialVersionUID = 4163067599452705932L;
    final HashMap<Integer, ActorRef> columnActors;
    final HashMap<Integer, ActorRef> columnObserverActors;
    final HashMap<Integer, ActorRef> columnDatastructureActors;
    final HashMap<IndCandidate, ActorRef> unaryCandidateActors;
}
