package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.configuration.Configuration;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.datastructures.CardinalityEstimatable;
import de.hpi.streaming_inds.datastructures.ProbabilisticDatastructureType;
import de.hpi.streaming_inds.datastructures.VersionedHistory;
import de.hpi.streaming_inds.messages.ColumnRecordMessage;
import de.hpi.streaming_inds.messages.DatastructureResponseMessage;
import de.hpi.streaming_inds.messages.DatastructureRequestMessage;
import de.hpi.streaming_inds.messages.DatastructureUpdateMessage;
import de.hpi.streaming_inds.messages.readiness.ColDatastructureReadyMessage;
import de.hpi.streaming_inds.messages.registration.ColDatastructureRegistrationMessage;
import de.hpi.streaming_inds.messages.registration.RegisteredLocalActorsMessage;

import java.time.Duration;

public class ColDatastructureActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "ColDatastructureActor";
    private final ActorRef localRegisterActor;
    private final boolean isPassive;
    private int columnId;
    private ActorRef colObserver;
    private VersionedHistory versionedHistory;
    private ActorRef colActor;

    public static Props props(int columnId, ActorRef localRegisterActor, boolean isPassive) {
        return Props.create(ColDatastructureActor.class, columnId, localRegisterActor, isPassive);
    }

    public ColDatastructureActor(int columnId, ActorRef localRegisterActor, boolean isPassive) {
        this.columnId = columnId;
        this.isPassive = isPassive;
        Configuration config = ConfigurationSingleton.get();
        this.versionedHistory = new VersionedHistory(ProbabilisticDatastructureType.valueOf(config.getProbabilisticDatastructureType()), config.getBatchSize());
        this.localRegisterActor = localRegisterActor;

        localRegisterActor.tell(new ColDatastructureRegistrationMessage(columnId), getSelf());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(RegisteredLocalActorsMessage.class, this::handle)
                .match(ColumnRecordMessage.class, this::handle)
                .match(DatastructureRequestMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }


    private void handle(RegisteredLocalActorsMessage msg) {
        if (!isPassive) {
            this.colObserver = msg.getColumnObserverActors().get(columnId);
        }
        this.colActor = msg.getColumnActors().get(columnId);
        localRegisterActor.tell(new ColDatastructureReadyMessage(), getSelf());
    }

    private void handle(ColumnRecordMessage msg) {
        boolean wasChanged = versionedHistory.updateAndReturnIfChanged(msg.getRowId(), msg.getHash(), msg.isDelete());

        if (!isPassive) {
            colObserver.tell(new DatastructureUpdateMessage(msg.getRowId(), wasChanged, msg.isLastRowInBatch(), getSelf()), getSelf());
        }
    }

    private void handle(DatastructureRequestMessage msg) {
        CardinalityEstimatable requested = versionedHistory.get(msg.getRowId());
        if (requested == null) { //we have not seen the row yet
            this.log().info(columnId + " has not seen " + msg.getRowId() + " yet");
            context().system().scheduler().scheduleOnce(Duration.ofMillis(10), self(), msg, context().system().dispatcher(), sender());
        } else {
            sender().tell(new DatastructureResponseMessage(msg.getIndSide(), requested), getSelf());
        }
    }


}