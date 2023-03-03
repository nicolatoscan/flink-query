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

1. 
