# flink-query

## Table of Content

- scripts: The scripts to run the programs, process the output, and making the plots
- java/org/apache/flink:
  - generator: the java object that will be generated in the program
  - metrics: different ways to output the metrics
  - sink: sink operator in flink, in charge of getting metrics
  - source: source operator, read data
  - util/event_gens: control the emit rate of the data
  - Parallel.java: the program where we run our experiments

## How to run the program

### Environments

- Java: jdk-11
- Python
- Flink: 1.16.0

### Steps

1. Build the java project, get the `jar` of Parallel.java
2. Once you get the `jar`, run the script

  $ python3 scripts/experiments_run.py flink-query.jar --Parallelism 5
  
3. Metrics of throughput and latency will be stored in the metrics_logs directory, then run the script

  $ python3 scripts/data_processing.py
  
4. Then you will get the processed `.csv` files
5. For metrics of CPU and Memory Usage, you need to trace the java process id on the 
