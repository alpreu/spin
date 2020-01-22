package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.messages.GlobalColObserversMessage;
import de.hpi.streaming_inds.messages.readiness.OrchestratorReadyMessage;
import de.hpi.streaming_inds.messages.readiness.ClusterReadyMessage;
import de.hpi.streaming_inds.messages.registration.LocalColObserversRegistrationMessage;
import de.hpi.streaming_inds.messages.registration.OrchestratorRegistrationMessage;
import de.hpi.streaming_inds.messages.registration.RegisteredOrchestratorsMessage;

import java.util.ArrayList;
import java.util.HashMap;

public class GlobalRegisterActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "GlobalRegisterActor";
    private ArrayList<ActorRef> orchestrators = new ArrayList<>();
    private final int expectedOrchestrators;
    private HashMap<Integer, ActorRef> colObservers = new HashMap<>();
    private final int expectedColObservers;
    private int orchestratorsReadyCount = 0;




    public static Props props(int expectedOrchestrators, int numberOfColumns) {
        return Props.create(GlobalRegisterActor.class, expectedOrchestrators, numberOfColumns);
    }

    public GlobalRegisterActor(int expectedOrchestrators, int numberOfColumns) {
        this.expectedOrchestrators = expectedOrchestrators;
        this.expectedColObservers = numberOfColumns;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(OrchestratorRegistrationMessage.class, this::handle)
                .match(LocalColObserversRegistrationMessage.class, this::handle)
                .match(OrchestratorReadyMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(OrchestratorRegistrationMessage msg) {
        orchestrators.add(getSender());
        if (orchestrators.size() == expectedOrchestrators) {
            getContext().parent().tell(new RegisteredOrchestratorsMessage(orchestrators), getSelf());
        }
    }

    private void handle(LocalColObserversRegistrationMessage msg) {
            colObservers.putAll(msg.getColObserverActors());
        if (colObservers.size() == expectedColObservers) {
            orchestrators.forEach(o -> o.tell(new GlobalColObserversMessage(colObservers), getSelf()));
        }
    }

    private void handle(OrchestratorReadyMessage msg) {
        orchestratorsReadyCount++;
        if (orchestratorsReadyCount == expectedOrchestrators) {
            getContext().parent().tell(new ClusterReadyMessage(), getSelf());
        }
    }

}

