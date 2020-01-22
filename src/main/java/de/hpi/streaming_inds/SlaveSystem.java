package de.hpi.streaming_inds;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.event.slf4j.Logger;
import com.typesafe.config.Config;
import de.hpi.streaming_inds.actors.ClusterListenerActor;
import de.hpi.streaming_inds.actors.SystemOrchestratorActor;
import de.hpi.streaming_inds.configuration.Configuration;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;

public class SlaveSystem extends IndSystem {
    public static final String SLAVE_ROLE = "slave";



    public static void start() {
        final ActorSystem system;
        final Configuration c = ConfigurationSingleton.get();
        final Config systemConfig = createConfig(c.systemName, SLAVE_ROLE, c.getHost(), c.getPort(), c.getMasterHost(), c.getMasterPort());

        Logger.root().info(c.toString());
        system = createSystem(c.systemName, systemConfig);

        Cluster.get(system).registerOnMemberUp(() -> { //only waits for the specified systems to become available, does not wait for the actors of each system...
            system.actorOf(ClusterListenerActor.props(), ClusterListenerActor.DEFAULT_NAME);
            system.actorOf(SystemOrchestratorActor.props(), SystemOrchestratorActor.DEFAULT_NAME);
        });
    }
}
