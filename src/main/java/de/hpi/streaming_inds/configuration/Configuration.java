package de.hpi.streaming_inds.configuration;


import lombok.Data;
import lombok.ToString;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

@Data @ToString
public class Configuration {
    public static final String systemName = "indstream";
    private String host = getLocalhost();
    private String masterHost = getLocalhost();
    private int port = defaultPort();
    private int masterPort = defaultPort();
    private int numberOfSlaves;
    private int batchSize;
    private int hashSize = defaultHashSize();
    private List<String> filenames;
    private char separator;
    private boolean skipFirstLine;
    private boolean needsPrefixing;
    private boolean ignoreSurroundingSpaces;
    private boolean trim;
    private String probabilisticDatastructureType;
    private int bfCapacity;
    private int cHllLog2M;
    private int hybridThreshold;


    public void update(Command cmd) {
        this.host = cmd.host;
        this.port = cmd.port;
        this.masterHost = cmd.masterhost;
        this.masterPort = cmd.masterport;
        this.numberOfSlaves = cmd.numberOfSlaves;
        this.batchSize = cmd.batchSize;
        this.hashSize = cmd.hashSize;
        this.filenames = cmd.filenames;
        this.separator = cmd.separator;
        this.skipFirstLine = cmd.skipFirstLine;
        this.needsPrefixing = cmd.needsPrefixing;
        this.ignoreSurroundingSpaces = cmd.ignoreSurroundingSpaces;
        this.trim = cmd.trim;
        this.probabilisticDatastructureType = cmd.probabilisticDatastructureType;
        this.bfCapacity = cmd.bfCapacity;
        this.cHllLog2M = cmd.chllLog2M;
        this.hybridThreshold = cmd.hybridThreshold;
    }

    public void update(MasterCommand masterCommand) {
        this.update((Command) masterCommand);
    }

    public void update(SlaveCommand slaveCommand) {
        this.update((Command) slaveCommand);
    }

    private static int defaultHashSize() {
        return 32; //note that BF and HLL only really support 64bit
    }

    private static int defaultPort() {
        return 3000;
    }

    private static String getLocalhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    public String toImportantConfigString() {
        return "Config("
                +"slaves="+numberOfSlaves
                +"batchSize="+batchSize
                +"ds="+probabilisticDatastructureType
                +"log2m="+cHllLog2M
                +"thresh="+hybridThreshold
                +"bfcap="+bfCapacity
                +")";
    }


}
