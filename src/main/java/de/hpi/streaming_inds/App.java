package de.hpi.streaming_inds;


import com.beust.jcommander.JCommander;
import de.hpi.streaming_inds.configuration.ConfigurationSingleton;
import de.hpi.streaming_inds.configuration.MasterCommand;
import de.hpi.streaming_inds.configuration.SlaveCommand;



public class App {

    public static void main(String[] args) {
        MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
                .addCommand(MasterSystem.MASTER_ROLE, masterCommand)
                .addCommand(SlaveSystem.SLAVE_ROLE, slaveCommand)
                .acceptUnknownOptions(true)
                .build();

        jCommander.parse(args);

        if (jCommander.getParsedCommand() == null) {
            throw new RuntimeException("No parameters specified");
        }

        switch (jCommander.getParsedCommand()) {
            case MasterSystem.MASTER_ROLE:
                ConfigurationSingleton.get().update(masterCommand);
                MasterSystem.start();
                break;
            case SlaveSystem.SLAVE_ROLE:
                ConfigurationSingleton.get().update(slaveCommand);
                SlaveSystem.start();
                break;
            default:
                throw new RuntimeException("No valid system role specified");
        }
    }
}