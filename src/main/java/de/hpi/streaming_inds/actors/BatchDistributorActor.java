package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.Util;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.datastructures.model.Row;
import de.hpi.streaming_inds.messages.*;
import de.hpi.streaming_inds.messages.confirmation.BatchIndConfirmationMessage;
import de.hpi.streaming_inds.messages.confirmation.DatasetConfirmationMessage;
import de.hpi.streaming_inds.messages.readiness.DistributorReadyMessage;

import java.util.*;
import java.util.stream.Collectors;


public class BatchDistributorActor extends AbstractLoggingActor {
    public static String DEFAULT_NAME = "BatchDistributorActor";
    private ActorRef datasetReaderActor;
    private ArrayList<ActorRef> orchestrators;
    private int numberOfColumns;
    private long firstBatchReceivalTime;
    private boolean distributedLastBatch = false;
    private long batchCount = 0;
    private long rowCount = 0;

    private long batchTimeStart;
    private final ArrayDeque<Long> batchTimesMillis = new ArrayDeque<>();


    private HashSet<IndCandidate> finishedCandidatesInBatch = new HashSet<>();


    private ArrayDeque<BatchMessage> bufferedBatches = new ArrayDeque<>();

    public static Props props(ActorRef datasetReaderActor, ArrayList<ActorRef> orchestrators, int numberOfColumns) {
        return Props.create(BatchDistributorActor.class, datasetReaderActor, orchestrators, numberOfColumns);
    }

    public BatchDistributorActor(ActorRef datasetReaderActor, ArrayList<ActorRef> orchestrators, int numberOfColumns) {
        this.datasetReaderActor = datasetReaderActor;
        this.numberOfColumns = numberOfColumns;
        this.orchestrators = orchestrators;

        datasetReaderActor.tell(new DistributorReadyMessage(), getSelf());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(BatchMessage.class, this::handle)
                .match(BatchIndConfirmationMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(BatchIndConfirmationMessage msg) {
        finishedCandidatesInBatch.addAll(msg.getFinishedCandidates());

        if (finishedCandidatesInBatch.size() == (numberOfColumns * (numberOfColumns - 1))) {
            long batchTimeDiff = System.currentTimeMillis() - this.batchTimeStart;
            this.batchTimesMillis.add(batchTimeDiff);
            finishedCandidatesInBatch.clear();
            sendNextBatchOrRequestBatchIfEmpty();
        }
    }

    private void handle(BatchMessage msg) {
        ArrayList<Row> rows = msg.getRows();
        if (rows.get(0).id == 0) this.firstBatchReceivalTime = System.currentTimeMillis();
        if (msg.isLastBatch()) {
            long endTime = System.currentTimeMillis();
            long elapsed = endTime - this.firstBatchReceivalTime;
            this.log().info("received last batch from datasetreader after " + elapsed + " ms.");
        }

        this.bufferedBatches.add(msg);
        sendNextBatch();
    }

    private void sendNextBatchOrRequestBatchIfEmpty() {
        if (bufferedBatches.size() > 0) {
            sendNextBatch();
        } else {
            if (distributedLastBatch) {
                HashMap<String, String> metrics = this.getBatchMetrics();
                metrics.put("totalBatches", String.valueOf(this.batchCount));
                metrics.put("totalRows", String.valueOf(this.rowCount));
                getContext().parent().tell(new DatasetConfirmationMessage(metrics), getSelf());

            } else {
                datasetReaderActor.tell(new DistributorReadyMessage(), getSelf());
            }
        }
    }

    private void sendNextBatch() {
        this.batchTimeStart = System.currentTimeMillis();
        batchCount++;
        this.log().info("sending batch " + batchCount);
        BatchMessage batchMsg = bufferedBatches.removeFirst();
        for (ActorRef orchestrator: orchestrators) {
            orchestrator.tell(batchMsg, getSelf());
        }

        if (batchMsg.isLastBatch()) this.distributedLastBatch = true;
        this.rowCount += batchMsg.getRows().size();
    }

    private HashMap<String, String> getBatchMetrics() {
        HashMap<String, String> metrics = new HashMap<>();
        List<Long> sortedTimes = batchTimesMillis.stream().sorted().collect(Collectors.toList());
        double avgTime = sortedTimes.stream().mapToDouble(e -> e).average().getAsDouble();
        long p99 = Util.getPercentile(sortedTimes, 99);
        long p95 = Util.getPercentile(sortedTimes, 95);
        long p90 = Util.getPercentile(sortedTimes, 90);
        long p75 = Util.getPercentile(sortedTimes, 75);
        long p50 = Util.getPercentile(sortedTimes, 50);

        metrics.put("batchTimeAvg", String.valueOf(String.format(Locale.US,"%.3f", avgTime)));
        metrics.put("batchTimeP99", String.valueOf(p99));
        metrics.put("batchTimeP95", String.valueOf(p95));
        metrics.put("batchTimeP90", String.valueOf(p90));
        metrics.put("batchTimeP75", String.valueOf(p75));
        metrics.put("batchTimeP50", String.valueOf(p50));

        this.log().info("sorted batch times (ms): \n" + sortedTimes.toString() + "\n" +
                "percentile 99: " + p99 + "ms, percentile 95: " + p95 + "ms, percentile 90: " + p90 + "ms, percentile 75: " + p75 + "ms");
        return metrics;
    }



}
