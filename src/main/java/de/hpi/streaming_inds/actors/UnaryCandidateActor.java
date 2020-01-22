package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.datastructures.*;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.datastructures.model.IndSide;
import de.hpi.streaming_inds.messages.*;
import de.hpi.streaming_inds.messages.readiness.UnaryCandidateReadyMessage;
import de.hpi.streaming_inds.messages.registration.RegisteredLocalActorsMessage;
import de.hpi.streaming_inds.messages.registration.UnaryCandidateRegistrationMessage;

import java.time.Duration;
import java.util.ArrayDeque;

public class UnaryCandidateActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "UnaryCandidateActor";
    private final ActorRef localRegisterActor;
    private final ActorRef indValidityActor;
    private IndCandidate indCandidate;
    private ActorRef lhsDatastructureActor;
    private ActorRef rhsDatastructureActor;

    ArrayDeque<RecheckIndMessage> recheckMessageBuffer = new ArrayDeque<>();
    private ActorRef currentRecheckSender;
    private long currentRecheckRowId =-1;
    private boolean isRechecking = false;

    private CardinalityEstimatable lhsStructure;
    private CardinalityEstimatable rhsStructure;
    private int datastructureCount = 0;

    private boolean hasDatastructureActors = false;
    private boolean currentRecheckIsGeneralRecheck;

    private boolean isValid = false;


    public static Props props(IndCandidate candidate, ActorRef localRegisterActor, ActorRef indValidityActor, boolean isValid) {
        return Props.create(UnaryCandidateActor.class, candidate, localRegisterActor, indValidityActor, isValid);
    }

    public UnaryCandidateActor(IndCandidate candidate, ActorRef localRegisterActor, ActorRef indValidityActor, boolean isValid) {
        this.indCandidate = candidate;
        this.localRegisterActor = localRegisterActor;
        this.indValidityActor = indValidityActor;
        this.isValid = isValid;

        localRegisterActor.tell(new UnaryCandidateRegistrationMessage(indCandidate), getSelf());
    }



    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(RegisteredLocalActorsMessage.class, this::handle)
                .match(RecheckIndMessage.class, this::handle)
                .match(DatastructureResponseMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(RegisteredLocalActorsMessage msg) {
        this.hasDatastructureActors = true;
        this.lhsDatastructureActor = msg.getColumnDatastructureActors().get(indCandidate.getLhs().getIds()[0]);
        this.rhsDatastructureActor = msg.getColumnDatastructureActors().get(indCandidate.getRhs().getIds()[0]);

        localRegisterActor.tell(new UnaryCandidateReadyMessage(), getSelf());
    }

    private boolean canRecheck() {
        return !isRechecking && recheckMessageBuffer.size() > 0;
    }

    private void handle(RecheckIndMessage msg) {
        if (!hasDatastructureActors) {
            context().system().scheduler().scheduleOnce(Duration.ofMillis(10), self(), msg, context().system().dispatcher(), sender());
        } else {
            recheckMessageBuffer.add(msg);

            if (canRecheck()) {
                recheckNextMessage();
            }
        }
    }

    private void recheckNextMessage() {
        this.isRechecking = true;

        RecheckIndMessage msg = recheckMessageBuffer.removeFirst();
        this.currentRecheckRowId = msg.getRowId();
        this.currentRecheckSender = msg.getSender();
        this.currentRecheckIsGeneralRecheck = msg.isGeneralRecheck();

        lhsDatastructureActor.tell(new DatastructureRequestMessage(currentRecheckRowId, IndSide.LHS), getSelf());
        rhsDatastructureActor.tell(new DatastructureRequestMessage(currentRecheckRowId, IndSide.RHS), getSelf());
    }

    private void handle(DatastructureResponseMessage msg) {
        datastructureCount++;
        boolean wasValid = this.isValid;

        if (msg.getIndSide() == IndSide.LHS) {
            this.lhsStructure = (CardinalityEstimatable) msg.getDatastructure();
        }
        if (msg.getIndSide() == IndSide.RHS) {
            this.rhsStructure = (CardinalityEstimatable) msg.getDatastructure();
        }

        if (datastructureCount == 2) {
            datastructureCount = 0;

            if (this.lhsStructure == null) {
                this.log().error(indCandidate.toString() + " datastructure is null for lhs");
            }
            if (this.rhsStructure == null) {
                this.log().error(indCandidate.toString() + " datastructure is null for rhs");
            }

            this.isValid = rhsStructure.containsAll(lhsStructure);

            if ((currentRecheckRowId == 0 ) || (isValid != wasValid)) {
                indValidityActor.tell(new IndValidityChangedMessage(currentRecheckRowId, indCandidate, isValid), getSelf());
            }
            this.currentRecheckSender.tell(new IndRecheckedMessage(this.currentRecheckRowId, this.indCandidate, this.isValid, this.currentRecheckIsGeneralRecheck), getSelf());

            this.isRechecking = false;
            if (canRecheck()) {
                recheckNextMessage();
            }
        }

    }



}