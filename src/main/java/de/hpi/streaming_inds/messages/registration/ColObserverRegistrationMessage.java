package de.hpi.streaming_inds.messages.registration;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class ColObserverRegistrationMessage implements Serializable {
    private static final long serialVersionUID = 4163067599452705932L;
    final int columnId;

}
