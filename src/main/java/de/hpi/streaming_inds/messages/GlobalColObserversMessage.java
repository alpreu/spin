package de.hpi.streaming_inds.messages;

import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class GlobalColObserversMessage implements Serializable {
    private static final long serialVersionUID = 4163067599452705932L;
    final HashMap<Integer, ActorRef> colObservers;
}
