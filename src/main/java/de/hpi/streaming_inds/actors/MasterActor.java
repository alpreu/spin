package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.Util;
import de.hpi.streaming_inds.configuration.Configuration;
import de.hpi.streaming_inds.messages.*;
import de.hpi.streaming_inds.messages.confirmation.DatasetConfirmationMessage;
import de.hpi.streaming_inds.messages.readiness.ClusterReadyMessage;
import de.hpi.streaming_inds.messages.registration.RegisteredOrchestratorsMessage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

public class MasterActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "MasterActor";
    private final Configuration config;
    private final int requiredNumberOfSlaves;
    private int numberOfColumns = -1;
    private ActorRef globalRegisterActor;
    private ArrayList<ActorRef> orchestrators;
    private ActorRef datasetReaderActor;
    private ActorRef batchDistributorActor;
    private long startTime;
    private ActorRef indValidityActor;


    public static Props props(Configuration config) {
        return Props.create(MasterActor.class, config);
    }

    public MasterActor(Configuration config) {
        this.config = config;
        this.requiredNumberOfSlaves = config.getNumberOfSlaves();

    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(NumberOfColumnsMessage.class, this::handle)
                .match(RegisteredOrchestratorsMessage.class, this::handle)
                .match(ClusterReadyMessage.class, this::handle)
                .match(DatasetConfirmationMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(NumberOfColumnsMessage msg) {
        this.log().info("received number of columns msg: " + msg.getNumberOfColumns());
        this.datasetReaderActor = getSender();
        this.numberOfColumns = msg.getNumberOfColumns();
        this.indValidityActor = getContext().actorOf(IndValidityActor.props(), IndValidityActor.DEFAULT_NAME);
        this.globalRegisterActor = getContext().actorOf(GlobalRegisterActor.props(requiredNumberOfSlaves, numberOfColumns), GlobalRegisterActor.DEFAULT_NAME);
    }

    private void handle(RegisteredOrchestratorsMessage msg) {
        this.log().info("Orchestrators registered, assigning columns");
        this.orchestrators = msg.getOrchestrators();
        assignColumns();

    }

    private void assignColumns() {
        List<List<Integer>> columnDistribution = Util.calculateColumnDistribution(numberOfColumns, orchestrators.size());

        for (int i = 0; i < orchestrators.size(); i++) {
            orchestrators.get(i).tell(new OrchestratorConfigMessage(new ArrayList<>(columnDistribution.get(i)), numberOfColumns, globalRegisterActor, indValidityActor), getSelf());
        }
    }

    private void handle(ClusterReadyMessage msg) {
        this.log().info("Whole cluster is ready, starting BatchDistributor");
        startTime = System.currentTimeMillis();
        this.batchDistributorActor = getContext().actorOf(BatchDistributorActor.props(datasetReaderActor, orchestrators, numberOfColumns), BatchDistributorActor.DEFAULT_NAME);
    }

    private void handle(DatasetConfirmationMessage msg) {
        long endTime = System.currentTimeMillis();
        long elapsed = endTime - startTime;
        HashMap<String, String> metrics = msg.getMetrics();


        metrics.put("zConfig", config.toImportantConfigString());

        metrics.put("runtime", String.valueOf(elapsed));
        this.log().info("completed processing of " + metrics.get("totalBatches") + " batches (" + metrics.get("totalRows") + " rows) in "+ elapsed +"ms");

        double throughput = (double) Long.parseLong(metrics.get("totalRows")) / (elapsed / (double) 1000);
        String throughput3Dec = String.format(Locale.US,"%.3f", throughput);
        metrics.put("throughput", throughput3Dec);
        this.log().info("throughput " + throughput3Dec + " rows/sec");
        this.writeMetricsCsv(metrics);
        indValidityActor.tell(msg, getSelf());
        this.log().info("everything finished, shutting down...");
        getContext().system().terminate();
    }

    private void writeMetricsCsv(HashMap<String, String> metrics) {
        TreeMap<String, String> sortedMetrics = new TreeMap<>(metrics);

        StringBuilder builder = new StringBuilder();

        int count = 1;
        for(String key : sortedMetrics.keySet()) {
            builder.append(key);
            if (count != sortedMetrics.keySet().size()) builder.append(",");
            count++;
        }
        builder.append("\n");
        count = 1;
        for(String value : sortedMetrics.values()) {
            builder.append(value);
            if (count != sortedMetrics.values().size()) builder.append(",");
            count++;
        }
        builder.append("\n");

        String csvString = builder.toString();
        String cwd = new File("").getAbsolutePath();
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(cwd +  "/result/metrics.csv"));
            writer.write(csvString);
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }



}