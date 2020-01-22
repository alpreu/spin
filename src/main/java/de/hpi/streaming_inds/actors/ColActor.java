package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.datastructures.model.Row;
import de.hpi.streaming_inds.messages.*;
import de.hpi.streaming_inds.messages.readiness.ColumnReadyMessage;
import de.hpi.streaming_inds.messages.registration.ColumnRegistrationMessage;
import de.hpi.streaming_inds.messages.registration.RegisteredLocalActorsMessage;

import java.util.ArrayList;


public class ColActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "ColumnActor";
    private final int columnId;
    private ActorRef localRegisterActor;
    private ActorRef colDatastructureActor;


    public static Props props(int columnId, ActorRef localRegisterActor) {
        return Props.create(ColActor.class, columnId, localRegisterActor);
    }

    public ColActor(int columnId, ActorRef localRegisterActor) {
        this.columnId = columnId;
        this.localRegisterActor = localRegisterActor;

        localRegisterActor.tell(new ColumnRegistrationMessage(columnId), getSelf());
    }



    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(RegisteredLocalActorsMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }


    private void handle(RegisteredLocalActorsMessage msg) {
        this.colDatastructureActor = msg.getColumnDatastructureActors().get(columnId);
        localRegisterActor.tell(new ColumnReadyMessage(), getSelf());
    }

    private void handle(BatchMessage msg) {
        ArrayList<Row> rows = msg.getRows();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            colDatastructureActor.tell(new ColumnRecordMessage(row.id, row.columnHashes[columnId], row.isDelete, (i == (rows.size() - 1))), getSelf());
        }
    }

}
