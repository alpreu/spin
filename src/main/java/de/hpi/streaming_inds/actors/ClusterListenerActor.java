package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.japi.pf.ReceiveBuilder;

public class ClusterListenerActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "ClusterListenerActor";

    public static Props props() {
        return Props.create(ClusterListenerActor.class);
    }

    private final Cluster cluster = Cluster.get(this.context().system());

    @Override
    public void preStart() {
        this.cluster.subscribe(this.self(), MemberEvent.class, UnreachableMember.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(self());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(CurrentClusterState.class, state -> this.log().info("Current members: {}", state.members()))
                .match(MemberUp.class, mUp -> this.log().info("Member is Up: {}", mUp.member()))
                .match(UnreachableMember.class, mUnreachable -> getContext().getSystem().terminate())
                .match(MemberRemoved.class, mRemoved -> this.log().info("Member is Removed: {}", mRemoved.member()))
                .match(MemberEvent.class, message -> {}) //ignore
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }
}