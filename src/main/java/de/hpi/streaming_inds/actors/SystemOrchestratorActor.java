package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.MasterSystem;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.messages.*;
import de.hpi.streaming_inds.messages.confirmation.BatchIndConfirmationMessage;
import de.hpi.streaming_inds.messages.readiness.LocalActorsReadyMessage;
import de.hpi.streaming_inds.messages.readiness.OrchestratorReadyMessage;
import de.hpi.streaming_inds.messages.registration.*;

import java.util.ArrayList;
import java.util.HashMap;

public class SystemOrchestratorActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "SystemOrchestratorActor";
    private final Cluster cluster = Cluster.get(this.context().system());
    private ActorRef masterActor;
    private ActorRef localRegisterActor;
    private HashMap<Integer, ActorRef> columnActors = new HashMap<>();
    private HashMap<Integer, ActorRef> colObserverActors;
    private int totalNumberOfColumns;
    private ArrayList<Integer> assignedColumnIds;
    private ActorRef globalRegisterActor;
    private ActorRef batchDistributorActor;
    private ActorRef indValidityActor;


    public static Props props() {
        return Props.create(SystemOrchestratorActor.class);
    }

    public SystemOrchestratorActor() {
    }

    @Override
    public void preStart() {
        this.cluster.subscribe(this.self(), ClusterEvent.MemberUp.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(ClusterEvent.CurrentClusterState.class, this::handle)
                .match(ClusterEvent.MemberUp.class, this::handle)
                .match(OrchestratorConfigMessage.class, this::handle)
                .match(RegisteredLocalActorsMessage.class, this::handle)
                .match(GlobalColObserversMessage.class, this::handle)
                .match(LocalActorsReadyMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(BatchIndConfirmationMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(OrchestratorConfigMessage msg) {
        this.log().info("received my config message");
        this.log().info(msg.toString());
        this.masterActor = getSender();
        this.assignedColumnIds = msg.getAssignedColumnIds();
        this.totalNumberOfColumns = msg.getTotalNumberOfColumns();
        this.globalRegisterActor = msg.getGlobalRegisterActor();
        this.indValidityActor = msg.getIndValidityActor();

        this.localRegisterActor = getContext().actorOf(LocalRegisterActor.props(assignedColumnIds.size(), totalNumberOfColumns, globalRegisterActor));


        for (int i = 0; i < totalNumberOfColumns; i++) {
            ActorRef colActor = getContext().actorOf(ColActor.props(i, localRegisterActor),
                    ColActor.DEFAULT_NAME + "_" + i);

            ActorRef colDatastructureActor = getContext().actorOf(ColDatastructureActor.props(i, localRegisterActor, !assignedColumnIds.contains(i)),
                    ColDatastructureActor.DEFAULT_NAME + "_" + i);
        }

        for (int columnId: assignedColumnIds) {
            ActorRef colObserverActor = getContext().actorOf(ColObserverActor.props(columnId, localRegisterActor, indValidityActor),
                    ColObserverActor.DEFAULT_NAME + "_" + columnId);

            for(IndCandidate candidate: IndCandidate.generateUnaryCandidatesWithLhs(columnId, totalNumberOfColumns)) {
                ActorRef unaryCandidateActor = getContext().actorOf(UnaryCandidateActor.props(candidate, localRegisterActor, indValidityActor, false),
                        UnaryCandidateActor.DEFAULT_NAME + "_" + candidate.identifier());
            }
        }
    }


    private void handle(RegisteredLocalActorsMessage msg) {
        this.columnActors = msg.getColumnActors();
        this.colObserverActors = msg.getColumnObserverActors();
        msg.getColumnActors().values().forEach(a -> a.tell(msg, getSelf()));
        msg.getColumnObserverActors().values().forEach(a -> a.tell(msg, getSelf()));
        msg.getColumnDatastructureActors().values().forEach(a -> a.tell(msg, getSelf()));
        msg.getUnaryCandidateActors().values().forEach(a -> a.tell(msg, getSelf()));

        globalRegisterActor.tell(new LocalColObserversRegistrationMessage(msg.getColumnObserverActors()), getSelf());
    }

    private void handle(GlobalColObserversMessage msg) {
        colObserverActors.values().forEach(co -> co.tell(msg, getSelf()));
    }

    private void handle(LocalActorsReadyMessage msg) {
        this.log().info("All local actors are ready");
        globalRegisterActor.tell(new OrchestratorReadyMessage(), getSelf());
    }

    private void handle(BatchMessage msg) {
        this.batchDistributorActor = getSender();
        columnActors.values().forEach(a -> a.tell(msg, getSelf()));
    }

    private void handle(BatchIndConfirmationMessage msg) {
        batchDistributorActor.tell(msg, getSelf());
    }

    private void handle(ClusterEvent.CurrentClusterState msg) {
        msg.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(ClusterEvent.MemberUp msg) {
        this.register(msg.member());
    }

    private void register(Member member) {
        if (member.hasRole(MasterSystem.MASTER_ROLE))
            this.getContext()
            .actorSelection(member.address() + "/user/" + MasterActor.DEFAULT_NAME + "/" + GlobalRegisterActor.DEFAULT_NAME)
            .tell(new OrchestratorRegistrationMessage(), this.self());
    }
}