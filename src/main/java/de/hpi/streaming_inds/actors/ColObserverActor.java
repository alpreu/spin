package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.Pair;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.messages.*;
import de.hpi.streaming_inds.messages.confirmation.BatchIndConfirmationMessage;
import de.hpi.streaming_inds.messages.confirmation.RecheckConfirmationMessage;
import de.hpi.streaming_inds.messages.readiness.ColObserverReadyMessage;
import de.hpi.streaming_inds.messages.registration.ColObserverRegistrationMessage;
import de.hpi.streaming_inds.messages.registration.RegisteredLocalActorsMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class ColObserverActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "ColumnObserverActor";
    private final int colId;
    private final ActorRef localRegisterActor;
    private final ActorRef indValidityActor;

    private TreeMap<Long, ArrayList<Pair<IndCandidate, ActorRef>>> rowsToCheck = new TreeMap<>();
    private HashMap<Long, Boolean> updateByRowId = new HashMap<>();
    private HashMap<Integer, ActorRef> globalColObservers;

    private long receivedRechecks;
    private long expectedRechecks;

    private boolean isRechecking = false;
    private boolean isDoneWithCurrentBatch = false;

    private long generalRecheckRowId = -1;
    private long idOfLastRowInBatch = -1;

    private ArrayDeque<DatastructureUpdateMessage> updateMessageBuffer = new ArrayDeque<>();
    private boolean bufferWasEmpty = false;


    public static Props props(int colId, ActorRef localRegisterActor, ActorRef indValidityActor) {
        return Props.create(ColObserverActor.class, colId, localRegisterActor, indValidityActor);
    }

    public ColObserverActor(int colId, ActorRef localRegisterActor, ActorRef indValidityActor) {
        this.colId = colId;
        this.localRegisterActor = localRegisterActor;
        this.indValidityActor = indValidityActor;

        localRegisterActor.tell(new ColObserverRegistrationMessage(colId), getSelf());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(RegisteredLocalActorsMessage.class, this::handle)
                .match(GlobalColObserversMessage.class, this::handle)
                .match(DatastructureUpdateMessage.class, this::handle)
                .match(IndRecheckedMessage.class, this::handle)
                .match(SpawnIndMessage.class, this::handle)
                .match(GeneralRecheckMessage.class, this::handle)
                .match(RecheckConfirmationMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }



    @Data @AllArgsConstructor
    private static class GeneralRecheckMessage {
        final long rowId;
    }

    private void handle(RegisteredLocalActorsMessage msg) {
        //Note: we assign the candidates to its lhs-col at the beginning (essentially assuming their validity even though
        // most will be invalid at that time. This is okay because they will probably switch sides anyway and it just seems easier for now.
        msg.getUnaryCandidateActors().entrySet().stream()
                .filter(entry -> entry.getKey().getLhs().getIds()[0] == colId)
                .forEach(entry -> rowsToCheck
                        .computeIfAbsent(0L, k -> new ArrayList<>())
                        .add(new Pair<IndCandidate, ActorRef>(entry.getKey(), entry.getValue())));

    }

    private void handle(GlobalColObserversMessage msg) {
        this.globalColObservers = msg.getColObservers();
        localRegisterActor.tell(new ColObserverReadyMessage(), getSelf());
    }

    private void handle(DatastructureUpdateMessage msg) {
        updateMessageBuffer.add(msg);

        if (msg.isLastRowInBatch()) {
            this.idOfLastRowInBatch = msg.getRowId();
        }

        if ((msg.getRowId() % ConfigurationSingleton.get().getBatchSize()) == 0 || bufferWasEmpty) {
            bufferWasEmpty = false;
            processNextMessage();
        }
    }

    private void processNextMessage() {
        DatastructureUpdateMessage msg = updateMessageBuffer.pollFirst();

        if ((msg.getRowId() % ConfigurationSingleton.get().getBatchSize()) == 0) {
            if (!rowsToCheck.isEmpty() && ((rowsToCheck.firstKey() % ConfigurationSingleton.get().getBatchSize()) != 0)) {
                throw new RuntimeException(colId + ": rowsToCheck.firstKey() is not first row on reaching next batch in row " + msg.getRowId() + ", " + rowsToCheck.keySet());
            }

            this.log().info(colId + ": " + " reset state on row " + msg.getRowId());
            updateByRowId.clear();
            this.idOfLastRowInBatch = -1;
            this.isDoneWithCurrentBatch = false;
        }

        updateByRowId.put(msg.getRowId(), msg.isChanged());
        self().tell(new GeneralRecheckMessage(msg.getRowId()), getSelf()); //TODO: not sure if this can cause race condition
    }

    private void handle(GeneralRecheckMessage msg) {
        if (this.isRechecking) {
            this.log().info(colId + ": was rechecking, rescheduling general recheck for row " + msg.getRowId());
            context().system().scheduler().scheduleOnce(Duration.ofMillis(100), self(), msg, context().system().dispatcher(), sender());
        } else {
            this.generalRecheckRowId = msg.getRowId();
            triggerGeneralRecheck(msg.getRowId());
        }
    }

    private ArrayList<IndCandidate> getFinishedCandidates(long nextBatchRowId) {
        ArrayList<Pair<IndCandidate, ActorRef>> candidatePairs = rowsToCheck.get(nextBatchRowId);
        if (candidatePairs == null) {
            return new ArrayList<>(); //could also consider adding null to the msg (faster serialization?)
        } else {
            return (ArrayList<IndCandidate>) candidatePairs.stream().map(Pair::first).collect(Collectors.toList());
        }
    }

    private void triggerGeneralRecheck(long rowId) {
        this.isRechecking = true;
        ArrayList<Pair<IndCandidate, ActorRef>> candidates = rowsToCheck.remove(rowId);

        if (candidates == null) { //no local candidates
            onRecheckFinish(rowId);
        } else {
            Boolean rowHasChange = updateByRowId.get(rowId);
            recheck(true, rowId, candidates, rowHasChange);
        }
    }

    private void handle(IndRecheckedMessage msg) {
        this.receivedRechecks++;

        if (!msg.isValid()) {
            Integer rhsColId = msg.getCandidate().getRhs().getIds()[0];
            assignCandidateToObserver(rhsColId, msg, sender(), msg.isValid());
        } else {
            Integer lhsColId = msg.getCandidate().getLhs().getIds()[0];
            assignCandidateToObserver(lhsColId, msg, sender(), msg.isValid());
        }

        if (this.receivedRechecks == this.expectedRechecks) {
            onRecheckFinish(msg.getRowId());
        }
    }



    private boolean hasPendingRechecksFromSpawnedInd() {
        return !rowsToCheck.isEmpty() && (rowsToCheck.firstKey() <= generalRecheckRowId);
    }

    private boolean hasBufferedMessages() {
        return !updateMessageBuffer.isEmpty();
    }

    private void processPendingRechecksOrNextMessage() {
        if (isRechecking) {
            this.log().info(colId + ": FIXME: Still rechecking while being told to process pending or next message");
            return;
        }

        if (hasPendingRechecksFromSpawnedInd()) {
            triggerPendingRechecks();
        } else {
            if (hasBufferedMessages()) {
                processNextMessage();
            } else {
                this.bufferWasEmpty = true;
            }
        }
    }

    private void informIfDone(long rowId) {
        if (rowId == this.idOfLastRowInBatch) {
            this.isDoneWithCurrentBatch = true;
            getContext().parent().tell(new BatchIndConfirmationMessage(colId, getFinishedCandidates(rowId + 1)), getSelf());
        }
    }

    private void onRecheckFinish(long rowId) {
        this.isRechecking = false;
        informIfDone(rowId);
        self().tell(new RecheckConfirmationMessage(), getSelf());
    }

    private void handle(RecheckConfirmationMessage msg) {
        this.processPendingRechecksOrNextMessage();
    }

    private void recheck(boolean isGeneralRecheck, long rowId, ArrayList<Pair<IndCandidate, ActorRef>> candidates, boolean rowHasChange) {
        if (!rowHasChange) {
            rowsToCheck.computeIfAbsent(rowId + 1, k -> new ArrayList<>())
                    .addAll(candidates); //move all candidates to the next rowId
            onRecheckFinish(rowId);
        } else {
            this.expectedRechecks = candidates.size();
            this.receivedRechecks = 0;
            for (Pair<IndCandidate, ActorRef> candidate : candidates) {
                candidate.second().tell(new RecheckIndMessage(rowId, getSelf(), isGeneralRecheck), getSelf());
            }
        }
    }

    private void triggerPendingRechecks() {
        this.isRechecking = true;

        Map.Entry<Long, ArrayList<Pair<IndCandidate, ActorRef>>> firstEntry = rowsToCheck.pollFirstEntry();
        long rowId = firstEntry.getKey();
        ArrayList<Pair<IndCandidate, ActorRef>> candidates = firstEntry.getValue();
        Boolean rowHasChange = updateByRowId.get(rowId);

        recheck(false, rowId, candidates, rowHasChange);
    }


    private void assignCandidateToObserver(Integer colObserverId, IndRecheckedMessage msg,  ActorRef candidateActor, boolean indIsValid) {
        if (this.colId == colObserverId) { //do not send away, the candidate is already assigned to that side
            rowsToCheck.computeIfAbsent(msg.getRowId() + 1, k -> new ArrayList<>())
                    .add(new Pair<>(msg.getCandidate(), candidateActor));
        } else {
            //needs to be poisonpill instead of stop(), in case the actor still has messages to process
            candidateActor.tell(PoisonPill.getInstance(), getSelf());
            ActorRef otherHandSideColObserver = globalColObservers.get(colObserverId);
            otherHandSideColObserver.tell(new SpawnIndMessage(msg.getRowId(), msg.getCandidate(), indIsValid), getSelf());
        }
    }

    private void handle(SpawnIndMessage msg) {
        ActorRef newCandidate = context().actorOf(UnaryCandidateActor.props(msg.getCandidate(), localRegisterActor, indValidityActor, msg.isValid()),
                UnaryCandidateActor.DEFAULT_NAME + "_" + msg.getCandidate().identifier());

        rowsToCheck.computeIfAbsent(msg.getRowId() + 1, k -> new ArrayList<>()).add(new Pair<>(msg.getCandidate(), newCandidate));

        if (isDoneWithCurrentBatch) { //the spawned ind wont get automatically checked through the normal mechanism
            //FIXME: we sometimes tell it to process pending or next message while still rechecking? might not be problematic tho?
            if (msg.getRowId() == this.idOfLastRowInBatch) {
                informIfDone(msg.getRowId());
            } else {
                processPendingRechecksOrNextMessage();
            }
        }
    }


}