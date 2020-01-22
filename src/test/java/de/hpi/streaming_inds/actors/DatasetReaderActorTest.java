package de.hpi.streaming_inds.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.clearspring.analytics.hash.MurmurHash;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.io.CSVFileInputGenerator;
import de.hpi.streaming_inds.messages.BatchMessage;
import de.hpi.streaming_inds.messages.NumberOfColumnsMessage;
import de.hpi.streaming_inds.messages.readiness.DistributorReadyMessage;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetReaderActorTest {
    private ActorSystem system;
    private TestKit probe;
    private ActorRef actor;

    @BeforeEach
    void setup() {
        ConfigurationSingleton.get().setBatchSize(10);
        this.system = ActorSystem.create("test-system");
        this.probe = new TestKit(system);
    }

    @AfterEach
    void teardown() {
        system.stop(probe.getRef());
        system.stop(actor);
        TestKit.shutdownActorSystem(this.system);
    }

    static long[] getHashes(List<String> raw) {
        return raw.stream().map(MurmurHash::hash).mapToLong(Integer::longValue).toArray();
    }

    @Test
    void readSingleSourceWithPrefixCorrectly() throws IOException {
        RelationalInputGenerator inputGenerator = new CSVFileInputGenerator("src/test/resources/table1.csv", ',', false, true, false, false);
        actor = system.actorOf(DatasetReaderActor.props(Collections.singletonList(inputGenerator), probe.getRef()));

        NumberOfColumnsMessage msg = probe.expectMsgClass(NumberOfColumnsMessage.class);
        assertThat(msg.getNumberOfColumns()).isEqualTo(3);

        probe.send(actor, new DistributorReadyMessage());
        BatchMessage batchMsg = probe.expectMsgClass(BatchMessage.class);

        assertThat(batchMsg.isLastBatch()).isTrue();
        assertThat(batchMsg.getRows().get(0).isDelete()).isFalse();
        assertThat(batchMsg.getRows().get(0).isLastRow()).isFalse();
        assertThat(batchMsg.getRows().get(2).isLastRow()).isTrue();
        long[] hashed = getHashes(Arrays.asList("39", " State-gov", " 77516"));
        assertThat(batchMsg.getRows().get(0).columnHashes).containsExactly(hashed);
    }

    @Test
    void readMultipleSourcesCorrectly() throws IOException {
        actor = system.actorOf(DatasetReaderActor.props(
                Arrays.asList(
                        new CSVFileInputGenerator("src/test/resources/table1.csv", ',', false, true, false, false),
                        new CSVFileInputGenerator("src/test/resources/table2.csv", ',', false, false, false, false)
                ), probe.getRef()));

        NumberOfColumnsMessage msg = probe.expectMsgClass(NumberOfColumnsMessage.class);
        assertThat(msg.getNumberOfColumns()).isEqualTo(6);

        probe.send(actor, new DistributorReadyMessage());
        BatchMessage batchMsg = probe.expectMsgClass(BatchMessage.class);

        assertThat(batchMsg.isLastBatch()).isTrue();
        assertThat(batchMsg.getRows().get(0).isDelete()).isFalse();
        assertThat(batchMsg.getRows().get(0).isLastRow()).isFalse();
        assertThat(batchMsg.getRows().get(2).isLastRow()).isTrue();
        long[] hashed = getHashes(Arrays.asList("39", " State-gov", " 77516", "53", " Private", " 321865"));
        assertThat(batchMsg.getRows().get(0).columnHashes).containsExactly(hashed);
    }


    @Test
    void readMultipleSourcesWithDifferingLengthsCorrectly() throws IOException {
        ConfigurationSingleton.get().setBatchSize(3);

        actor = system.actorOf(DatasetReaderActor.props(
                Arrays.asList(
                        new CSVFileInputGenerator("src/test/resources/table1.csv", ',', false, true, false, false),
                        new CSVFileInputGenerator("src/test/resources/table2.csv", ',', false, false, false, false),
                        new CSVFileInputGenerator("src/test/resources/table3.csv", ',', false, false, false, false)
                ), probe.getRef()));

        NumberOfColumnsMessage msg = probe.expectMsgClass(NumberOfColumnsMessage.class);
        assertThat(msg.getNumberOfColumns()).isEqualTo(3+3+6);

        probe.send(actor, new DistributorReadyMessage());
        BatchMessage batchMsg = probe.expectMsgClass(BatchMessage.class);

        assertThat(batchMsg.isLastBatch()).isFalse();
        assertThat(batchMsg.getRows().get(0).isDelete()).isFalse();
        assertThat(batchMsg.getRows().get(0).isLastRow()).isFalse();
        assertThat(batchMsg.getRows().get(2).isLastRow()).isFalse();
        long[] hashed = getHashes(Arrays.asList("39", " State-gov", " 77516", "53", " Private", " 321865", "Male", " 0", " 0", " 11", " Taiwan", " <=50K"));
        assertThat(batchMsg.getRows().get(0).columnHashes).containsExactly(hashed);

        probe.send(actor, new DistributorReadyMessage());
        BatchMessage lastBatchMsg = probe.expectMsgClass(BatchMessage.class);

        assertThat(lastBatchMsg.isLastBatch()).isTrue();
        assertThat(lastBatchMsg.getRows().get(0).isDelete()).isFalse();
        assertThat(lastBatchMsg.getRows().get(0).isLastRow()).isFalse();
        assertThat(lastBatchMsg.getRows().get(1).isLastRow()).isTrue();
        long[] lastHashed0 = getHashes(Arrays.asList("38"," Private"," 215646"," HS-grad", "27"," Private"," 257302", "Female"," 0"," 0"," 38"," United-States"," <=50K"));
        long[] lastHashed1 = getHashes(Arrays.asList("38"," Private"," 215646"," HS-grad", "27"," Private"," 257302", "Male"," 0"," 0"," 40"," United-States"," >50K"));
        assertThat(lastBatchMsg.getRows().get(0).columnHashes).containsExactly(lastHashed0);
        assertThat(lastBatchMsg.getRows().get(1).columnHashes).containsExactly(lastHashed1);
    }

    @Test
    void readMultipleSourcesWithDifferingLengthsCorrectlyWithoutPrefix() throws IOException {
        ConfigurationSingleton.get().setBatchSize(3);

        actor = system.actorOf(DatasetReaderActor.props(
                Arrays.asList(
                        new CSVFileInputGenerator("src/test/resources/table1.csv", ',', false, false, false, false),
                        new CSVFileInputGenerator("src/test/resources/table2.csv", ',', false, false, false, false),
                        new CSVFileInputGenerator("src/test/resources/table3.csv", ',', false, false, false, false)
                ), probe.getRef()));

        NumberOfColumnsMessage msg = probe.expectMsgClass(NumberOfColumnsMessage.class);
        assertThat(msg.getNumberOfColumns()).isEqualTo(2+3+6);

        probe.send(actor, new DistributorReadyMessage());
        BatchMessage batchMsg = probe.expectMsgClass(BatchMessage.class);

        assertThat(batchMsg.isLastBatch()).isFalse();
        assertThat(batchMsg.getRows().get(0).isDelete()).isFalse();
        assertThat(batchMsg.getRows().get(0).isLastRow()).isFalse();
        assertThat(batchMsg.getRows().get(2).isLastRow()).isFalse();
        long[] hashed = getHashes(Arrays.asList(" State-gov", " 77516", "53", " Private", " 321865", "Male", " 0", " 0", " 11", " Taiwan", " <=50K"));
        assertThat(batchMsg.getRows().get(0).columnHashes).containsExactly(hashed);

        probe.send(actor, new DistributorReadyMessage());
        BatchMessage lastBatchMsg = probe.expectMsgClass(BatchMessage.class);

        assertThat(lastBatchMsg.isLastBatch()).isTrue();
        assertThat(lastBatchMsg.getRows().get(0).isDelete()).isFalse();
        assertThat(lastBatchMsg.getRows().get(0).isLastRow()).isFalse();
        assertThat(lastBatchMsg.getRows().get(1).isLastRow()).isTrue();
        long[] lastHashed0 = getHashes(Arrays.asList(" Private"," 215646"," HS-grad", "27"," Private"," 257302", "Female"," 0"," 0"," 38"," United-States"," <=50K"));
        long[] lastHashed1 = getHashes(Arrays.asList(" Private"," 215646"," HS-grad", "27"," Private"," 257302", "Male"," 0"," 0"," 40"," United-States"," >50K"));
        assertThat(lastBatchMsg.getRows().get(0).columnHashes).containsExactly(lastHashed0);
        assertThat(lastBatchMsg.getRows().get(1).columnHashes).containsExactly(lastHashed1);
    }




}
