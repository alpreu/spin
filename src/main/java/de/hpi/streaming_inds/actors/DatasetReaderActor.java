package de.hpi.streaming_inds.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.clearspring.analytics.hash.MurmurHash;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.datastructures.model.Row;
import de.hpi.streaming_inds.messages.BatchMessage;
import de.hpi.streaming_inds.messages.NumberOfColumnsMessage;
import de.hpi.streaming_inds.messages.readiness.DistributorReadyMessage;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class DatasetReaderActor extends AbstractLoggingActor {
    public static String DEFAULT_NAME = "DatasetReaderActor";
    private final Function<Object, Long> hashFunction;

    private final int BATCH_SIZE = ConfigurationSingleton.get().getBatchSize();
    private ArrayList<Row> rows;
    private long rowIdCount = 0;
    private List<RelationalInput> relationalInputReaders;
    private boolean isLastBatch = false;
    private int numberOfColumns = 0;


    public static Props props(List<RelationalInputGenerator> inputGenerators, ActorRef masterActor) {
        return Props.create(DatasetReaderActor.class, inputGenerators, masterActor);
    }

    public DatasetReaderActor(List<RelationalInputGenerator> inputGenerators, ActorRef masterActor) throws AlgorithmExecutionException {
        if (ConfigurationSingleton.get().getHashSize() == Integer.SIZE) {
            this.log().info("using 32bit hashes");
            this.hashFunction = DatasetReaderActor::murmurHash32;
        } else if (ConfigurationSingleton.get().getHashSize() == Long.SIZE){
            this.log().info("using 64bit hashes");
            this.hashFunction = MurmurHash::hash64;
        } else {
            throw new RuntimeException("Unexpected hashSize: " + ConfigurationSingleton.get().getHashSize());
        }

        this.relationalInputReaders = new ArrayList<>();
        for (RelationalInputGenerator gen: inputGenerators) {
            relationalInputReaders.add(gen.generateNewCopy());
        }

        this.readBatch();
        this.numberOfColumns = this.rows.get(0).columnHashes.length; //the metadata column has already been converted

        masterActor.tell(new NumberOfColumnsMessage(numberOfColumns), getSelf());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(DistributorReadyMessage.class, this::handle)
                .matchAny(o -> this.log().info("Received unknown message: {}", o.toString()))
                .build();
    }

    private void handle(DistributorReadyMessage msg) throws InputIterationException {
        if (this.rows.size() != 0) {
            this.sender().tell(new BatchMessage(this.rows, this.isLastBatch), getSelf());
            this.readBatch();
        } else {
            this.log().info("rowbuffer was empty");
        }
    }

    private boolean atLeastOneInputReaderStillHasRows() throws InputIterationException {
        for (RelationalInput reader: this.relationalInputReaders) {
            if (reader.hasNext()) return true;
        }
        return false;
    }

    private void readBatch() throws InputIterationException {
        this.rows = new ArrayList<>(this.BATCH_SIZE);
        while(atLeastOneInputReaderStillHasRows() && this.rows.size() < this.BATCH_SIZE) {

            List<String> columns = new ArrayList<>();
            for (RelationalInput reader: this.relationalInputReaders) {
                List<String> columnsOfReader = reader.next();
                columns.addAll(columnsOfReader);
            }

            Row row = mapToRowType(columns);

            if((numberOfColumns != 0) && (row.columnHashes.length != numberOfColumns)) {
                throw new RuntimeException("Number of columns does not match, " + row.columnHashes.length + " is not " + numberOfColumns + " for row id " + row.id);
            }

            this.rows.add(row);
            rowIdCount++;
        }
        if (!atLeastOneInputReaderStillHasRows()) {
            this.isLastBatch = true;
        }
    }

    private boolean isDelete(String metadataColumnRecord) {
        return metadataColumnRecord.equals("delete") || metadataColumnRecord.equals("\"delete\"") || metadataColumnRecord.equals("\'delete\'");
    }

    private Row mapToRowType(List<String> columns) throws InputIterationException {
        String metadataColumn = columns.get(0);
        List<String> dataColumns = columns.subList(1, columns.size());
        return new Row(rowIdCount, hashColumns(dataColumns), isDelete(metadataColumn), !atLeastOneInputReaderStillHasRows());
    }

    private long[] hashColumns(List<String> columns) {
        long[] hashedColumns = new long[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            hashedColumns[i] = this.hashFunction.apply(columns.get(i));
        }
        return hashedColumns;
    }

    private static long murmurHash32(Object o) { //for wrapping 32bit int in long type
        return MurmurHash.hash(o);
    }


}
