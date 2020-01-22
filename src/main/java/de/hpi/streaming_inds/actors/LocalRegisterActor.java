package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.messages.readiness.ColObserverReadyMessage;
import de.hpi.streaming_inds.messages.readiness.ColDatastructureReadyMessage;
import de.hpi.streaming_inds.messages.readiness.ColumnReadyMessage;
import de.hpi.streaming_inds.messages.readiness.LocalActorsReadyMessage;
import de.hpi.streaming_inds.messages.readiness.UnaryCandidateReadyMessage;
import de.hpi.streaming_inds.messages.registration.*;

import java.util.HashMap;

public class LocalRegisterActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "LocalRegisterActor";
    private final int numberOfLocalColumns;
    private final int totalNumberOfColumns;
    private final ActorRef globalRegisterActor;
    private HashMap<Integer, ActorRef> columnActors = new HashMap<>();
    private boolean columnsRegistered = false;
    private int columnsReadyCount = 0;
    private boolean columnsReady = false;
    private HashMap<Integer, ActorRef> columnObserverActors = new HashMap<>();
    private boolean columnObserversRegistered = false;
    private int columnObserversReadyCount = 0;
    private boolean columnObserversReady = false;
    private HashMap<Integer, ActorRef> columnDatastructureActors = new HashMap<>();
    private boolean columnDatastructuresRegistered = false;
    private int columnDatastructuresReadyCount = 0;
    private boolean columnDatastructuresReady = false;
    private HashMap<IndCandidate, ActorRef> unaryCandidateActors = new HashMap<>();
    private boolean unaryCandidatesRegistered = false;
    private int unaryCandidatesReadyCount = 0;
    private boolean unaryCandidatesReady = false;




    public static Props props(int numberOfLocalColumns, int totalNumberOfColumns, ActorRef globalRegisterActor) {
        return Props.create(LocalRegisterActor.class, numberOfLocalColumns, totalNumberOfColumns, globalRegisterActor);
    }

    public LocalRegisterActor(int numberOfLocalColumns, int totalNumberOfColumns, ActorRef globalRegisterActor) {
        this.numberOfLocalColumns = numberOfLocalColumns;
        this.totalNumberOfColumns = totalNumberOfColumns;
        this.globalRegisterActor = globalRegisterActor;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(ColumnRegistrationMessage.class, this::handle)
                .match(ColObserverRegistrationMessage.class, this::handle)
                .match(ColDatastructureRegistrationMessage.class, this::handle)
                .match(UnaryCandidateRegistrationMessage.class, this::handle)
                .match(ColumnReadyMessage.class, this::handle)
                .match(ColDatastructureReadyMessage.class, this::handle)
                .match(ColObserverReadyMessage.class, this::handle)
                .match(UnaryCandidateReadyMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(ColumnRegistrationMessage msg) {
        columnActors.put(msg.getColumnId(), getSender());
        if (columnActors.size() == totalNumberOfColumns) {
            columnsRegistered = true;
            tellOrchestratorIfEverythingRegistered();
        }
    }

    private void handle(ColDatastructureRegistrationMessage msg) {
        columnDatastructureActors.put(msg.getColumnId(), getSender());
        if (columnDatastructureActors.size() == totalNumberOfColumns) {
            columnDatastructuresRegistered = true;
            tellOrchestratorIfEverythingRegistered();
        }
    }

    private void handle(ColObserverRegistrationMessage msg) {
        columnObserverActors.put(msg.getColumnId(), getSender());
        if (columnObserverActors.size() == numberOfLocalColumns) {
            columnObserversRegistered = true;
            tellOrchestratorIfEverythingRegistered();
        }
    }

    private void handle(UnaryCandidateRegistrationMessage msg) {
        if (unaryCandidatesRegistered) { //this happens when we move candidates around during the IND checking
            sender().tell(new RegisteredLocalActorsMessage(
                    columnActors,
                    columnObserverActors,
                    columnDatastructureActors,
                    unaryCandidateActors),
                    getSelf()
            );
        } else {
            unaryCandidateActors.put(msg.getCandidate(), getSender());
            if (unaryCandidateActors.size() == numberOfLocalColumns * (totalNumberOfColumns - 1)) {
                unaryCandidatesRegistered = true;
                tellOrchestratorIfEverythingRegistered();
            }
        }
    }

    private void tellOrchestratorIfEverythingRegistered() {
        if (columnsRegistered && columnObserversRegistered && columnDatastructuresRegistered && unaryCandidatesRegistered) {
            getContext().parent().tell(new RegisteredLocalActorsMessage(
                    columnActors,
                    columnObserverActors,
                    columnDatastructureActors,
                    unaryCandidateActors),
                    getSelf()
            );
        }
    }

    private void handle(ColumnReadyMessage msg) {
        columnsReadyCount++;
        if (columnsReadyCount == totalNumberOfColumns) {
            columnsReady = true;
            tellOrchestratorIfEverythingReady();
        }
    }

    private void handle(ColDatastructureReadyMessage msg) {
        columnDatastructuresReadyCount++;
        if (columnDatastructuresReadyCount == totalNumberOfColumns) {
            columnDatastructuresReady = true;
            tellOrchestratorIfEverythingReady();
        }
    }

    private void handle(ColObserverReadyMessage msg) {
        columnObserversReadyCount++;
        if (columnObserversReadyCount == numberOfLocalColumns) {
            columnObserversReady = true;
            tellOrchestratorIfEverythingReady();
        }
    }

    private void handle(UnaryCandidateReadyMessage msg) {
        unaryCandidatesReadyCount++;
        if (unaryCandidatesReadyCount == numberOfLocalColumns * (totalNumberOfColumns - 1)) {
            unaryCandidatesReady = true;
            tellOrchestratorIfEverythingReady();
        }
    }

    private void tellOrchestratorIfEverythingReady() {
        if (columnsReady && columnDatastructuresReady && columnObserversReady && unaryCandidatesReady) {
            getContext().parent().tell(new LocalActorsReadyMessage(), getSelf());
        }
    }

}