# SPIN - Inclusion Dependency Discovery on Streaming Data

## Build

Build the project using `mvn clean package`


## Configuration

To configure the algorithm you have two options: providing every argument on the command line or creating a `run.params` file containing the config which will be passed as an argument.


An exemplary `run.params` config is the following:
```
--batch-size=250
--hash-size=32
--masterhost=<MASTERHOST>
--slaves=3
--file-name=<FILEPATH TO DATASET>
--separator=<CSV SEPARATOR> 
--skip-first=<CSV SKIP FIRST LINE>
--needs-prefix=<TRUE IF DATA HAS NO UPDATE TYPE METADATA AS FIRST COLUMN> 
--datastructure-type=<PROBABILISTIC DATASTRUCTURE TO USE>
--bf-capacity=1024
--log2m=16
--hybrid-threshold=1000
```

In case your data set spans multiple files just keep repeating the `--file-name` argument for every file of the data set.

On invalid configuration or missing arguments the CLI will also provide help.


## Running

To run the SPIN algorithm you need to have one instance of the master system running, and at least one instance of the worker system. Ideally these run on different machines in the same network, if not one has to provide `--port` and `--master-port` arguments in the `run.params` file as well so the systems can connect.

To start the master system use:

`java -jar <JARNAME> master @run.params`

To start the worker system use:

`java -jar <JARNAME> slave @run.params`