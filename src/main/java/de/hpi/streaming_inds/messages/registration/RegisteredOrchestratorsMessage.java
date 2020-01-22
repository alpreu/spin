package de.hpi.streaming_inds.messages.registration;

import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class RegisteredOrchestratorsMessage implements Serializable {
    private static final long serialVersionUID = 4163067599452705932L;
    final ArrayList<ActorRef> orchestrators;
}
