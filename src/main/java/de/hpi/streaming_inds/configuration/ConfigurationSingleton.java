package de.hpi.streaming_inds.configuration;

public class ConfigurationSingleton {
    private static final Configuration config =  new Configuration();

    public static Configuration get() {
        return config;
    }
}
