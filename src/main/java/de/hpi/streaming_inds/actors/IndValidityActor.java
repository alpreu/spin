package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.japi.Pair;
import akka.japi.pf.ReceiveBuilder;
import de.hpi.streaming_inds.datastructures.model.IndCandidate;
import de.hpi.streaming_inds.messages.IndValidityChangedMessage;
import de.hpi.streaming_inds.messages.confirmation.DatasetConfirmationMessage;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndValidityActor extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "IndValidityActor";
    private HashMap<IndCandidate, Pair<Long, Boolean>> indValidity = new HashMap<>();
    private BufferedWriter metadataChangelogWriter;

    public static Props props() {
        return Props.create(IndValidityActor.class);
    }

    public IndValidityActor() {
        String cwd = new File("").getAbsolutePath();
        try {
            this.metadataChangelogWriter = new BufferedWriter(new FileWriter(cwd +  "/result/result.changelog"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(IndValidityChangedMessage.class, this::handle)
                .match(DatasetConfirmationMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(IndValidityChangedMessage msg) {
        this.log().info(msg.toString());
        indValidity.merge(msg.getCandidate(),
                new Pair<>(msg.getRowId(), msg.isValid()),
                (currentPair, newPair) -> newPair.first() > currentPair.first() ? newPair : currentPair);

        try {
            metadataChangelogWriter.write(msg.getRowId() + ", " + msg.getCandidate()+ ", " + msg.isValid());
            metadataChangelogWriter.newLine();
        } catch (IOException e) {
            this.log().error(e.toString());
        }
    }

    private void handle(DatasetConfirmationMessage msg) {
        this.log().info("Received DatasetConfirmation, printing valid candidates");
        prettyPrintValidInds();
    }

    private void prettyPrintValidInds() {
        List<String> validCandidates = indValidity.entrySet().stream()
                .filter(e -> e.getValue().second())
                .map(Map.Entry::getKey)
                .sorted(IndCandidate.comparator)
                .map(IndCandidate::toString)
                .collect(Collectors.toList());

        StringBuilder builder = new StringBuilder();
        for (String candidate: validCandidates) {
            builder.append(candidate);
            builder.append("\n");
        }
        builder.append("Found " + validCandidates.size() + " INDs");
        builder.append("\n");
        String resultString = builder.toString();

        String cwd = new File("").getAbsolutePath();
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(cwd +  "/result/result.unaries"));
            writer.write(resultString);
            writer.close();
            this.metadataChangelogWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        this.log().info(resultString);
        this.log().info("Found " + validCandidates.size() + " INDs");
    }


}