package de.hpi.streaming_inds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.event.slf4j.Logger;
import com.typesafe.config.Config;
import de.hpi.streaming_inds.actors.ClusterListenerActor;
import de.hpi.streaming_inds.actors.DatasetReaderActor;
import de.hpi.streaming_inds.actors.MasterActor;
import de.hpi.streaming_inds.configuration.Configuration;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.io.CSVFileInputGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;

import java.io.IOException;
import java.util.ArrayList;


public class MasterSystem extends IndSystem {
    public static final String MASTER_ROLE = "master";



    public static void start() {
        final ActorSystem system;
        final Configuration c = ConfigurationSingleton.get();
        final Config systemConfig = createConfig(c.systemName, MASTER_ROLE, c.getMasterHost(), c.getMasterPort(), c.getMasterHost(), c.getMasterPort());

        Logger.root().info(c.toString());
        system = createSystem(c.systemName, systemConfig);
        ActorRef masterActor = system.actorOf(MasterActor.props(c), MasterActor.DEFAULT_NAME);
        try {
            ArrayList<RelationalInputGenerator> inputGenerators = new ArrayList<>();
            for (int i = 0; i < c.getFilenames().size(); i++) {
                inputGenerators.add(new CSVFileInputGenerator(
                        c.getFilenames().get(i),
                        c.getSeparator(),
                        c.isSkipFirstLine(),
                        (i == 0 && c.isNeedsPrefixing()),
                        c.isIgnoreSurroundingSpaces(),
                        c.isTrim()
                ));
            }
            ActorRef datasetReaderActor = system.actorOf(DatasetReaderActor.props(inputGenerators, masterActor), DatasetReaderActor.DEFAULT_NAME);
        } catch (IOException e) {
            system.terminate();
            throw new RuntimeException(e);
        }

        Cluster.get(system).registerOnMemberUp(() -> { //only waits for the specified systems to become available, does not wait for the actors of each system...
            system.actorOf(ClusterListenerActor.props(), ClusterListenerActor.DEFAULT_NAME);
        });
    }

}
