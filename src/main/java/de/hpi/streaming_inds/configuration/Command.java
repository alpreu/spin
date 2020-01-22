package de.hpi.streaming_inds.configuration;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.ArrayList;
import java.util.List;

@Parameters(commandDescription = "starts a ind streams system", separators = "=")
public abstract class Command {
    @Parameter(names = { "-mh", "--masterhost" }, description = "host name or IP of the master", required = true)
    String masterhost;

    @Parameter(names = { "-mp", "--masterport" }, description = "port of the master", required = false)
    int masterport = ConfigurationSingleton.get().getMasterPort();

    @Parameter(names = { "-h", "--host" }, description = "this machine's host name or IP to bind against", required = false)
    String host = ConfigurationSingleton.get().getHost();

    @Parameter(names = { "-p", "--port" }, description = "port to bind against", required = false)
    int port = ConfigurationSingleton.get().getPort();

    @Parameter(names = { "-bs", "--batch-size" }, description = "batch size", required = true)
    int batchSize;

    @Parameter(names = { "-hs", "--hash-size" }, description = "size of hashfunction to use (32 or 64bit)", required = false)
    int hashSize = ConfigurationSingleton.get().getHashSize();

    @Parameter(names = { "-s", "--slaves"}, description = "number of slave systems", required = true)
    int numberOfSlaves;

    @Parameter(names = { "-f", "--file-name" }, description = "path of dataset", required = true)
    List<String> filenames;

    @Parameter(names = {"-sep", "--separator" }, converter = CharConverter.class, description = "default separator", required = true)
    char separator;

    @Parameter(names = {"-sf", "--skip-first" }, description = "skip first line", required = true, arity = 1)
    boolean skipFirstLine;

    @Parameter(names = {"-np", "--needs-prefix"}, description = "prefix data with 'insert' metadata column", required = true, arity = 1)
    boolean needsPrefixing;

    @Parameter(names = {"-iss", "--ignore-surrounding-spaces"}, description = "ignore spaces surrounding record values", required = false, arity = 1)
    boolean ignoreSurroundingSpaces = false;

    @Parameter(names = {"-tr", "--trim"}, description = "trim leading and trailing whitespace", required = false, arity = 1)
    boolean trim = false;

    @Parameter(names = {"-dt", "--datastructure-type"}, description = "type of datastructure to use for set estimation (HYBRID, HYPERLOGLOG, BLOOMFILTER, COUNTINGHYPERLOGLOG)", required = true)
    String probabilisticDatastructureType;

    @Parameter(names = {"-bfc", "--bf-capacity"}, description = "capacity of bloomfilter", required = true)
    int bfCapacity;

    @Parameter(names = {"-l2m", "--log2m"}, description = "bits to use for HLL", required = true)
    int chllLog2M;

    @Parameter(names = {"-ht", "--hybrid-threshold"}, description = "threshold where hybrid datastructure estimates", required = true)
    int hybridThreshold;


    private static class CharConverter implements IStringConverter<Character> {
        @Override
        public Character convert(String value) {
            return value.charAt(0);
        }
    }




}
