[![Build Status](https://travis-ci.org/SolaceProducts/solace-apache-beam.svg?branch=master)](https://travis-ci.org/SolaceProducts/solace-apache-beam)

# Solace PubSub+ Connector for Beam: I/O

## Overview

This project provides an I/O component which allows for Apache Beam applications running in Google Cloud Dataflow to connect to and receive data from a Solace Event Mesh.

## Table of contents
* [Synopsis](#synopsis)
* [Design](#design)
* [Usage](#usage)
* [Sample Walkthrough](#sample-walkthrough)
* [Contributing](#contributing)
* [Authors](#authors)
* [License](#license)
* [Resources](#resources)
---

## Synopsis

Consider the following diagram:

![Architecture Overview](images/Overview.png)

Using the Beam I/O Connector, Apache Beam applications can receive messages from a Solace PubSub+ broker (appliance, software, or Solace Cloud messaging service) regardless of how messages were initially sent to the broker – whether it be REST POST, AMQP, JMS, or MQTT messages. And by using an Apache Beam data runner, these applications can be deployed onto various services, from which, the data they receive can be further processed by other I/O connectors to interact with other technologies such as Cloud BigQuery, Apache Cassandra, etc.

The Solace Event Mesh is a clustered group of Solace PubSub+ Brokers that transparently, in real-time, route data events to any Service that is part of the Event Mesh.  Solace PubSub+ Brokers are connected to each other as a multi-connected mesh that to individual services (consumers or producers of data events) appears to be a single Event Broker. Event messages are seamlessly transported within the entire Solace Event Mesh regardless of where the event is created and where the process exists that has registered interested in consuming the event. You can read move about the advantages of a Solace event mesh [here](https://cloud.solace.com/learn/group_howto/ght_event_mesh.html).

Simply by having a Beam-connected Solace PubSub+ broker added to the Event Mesh, the entire Event Mesh becomes aware of the data registration request and will know how to securely route the appropriate events to and from Beam-connected services.

To understand the Beam architecture of source and sink I/O connectors and runners please review [Beam Documentation](https://beam.apache.org/documentation/).

To understand the capabilities of different Beam runners please review the [runner compatibility matrix](https://beam.apache.org/documentation/runners/capability-matrix/). 

This Beam integration solution allows any event from any service in the Solace Event Mesh [to be captured in](https://beam.apache.org/documentation/io/built-in/):
* Google BigQuery
* Google Cloud Bigtable
* Google Cloud Datastore
* Google Cloud Spanner
* Apache Cassandra
* Apache Hadoop InputFormat
* Apache HBase
* Apache Hive (HCatalog)
* Apache Kudu
* Apache Solr
* Elasticsearch (v2.x, v5.x, v6.x)
* JDBC
* MongoDB
* Redis
* Amazon Kinesis
* File
* Avro Format

It does not matter which service in the Event Mesh created the event, the events are all potentially available to Beam-connected services. There is no longer a requirement to code end applications to reach individual data services.

![Event Mesh](images/EventMesh_Beam.png)

## Design

The Beam I/O Connector is an UnboundedSource connector providing an unbounded data stream.  The connector will connect to a single Solace PubSub+ broker and will read data from a provided list of [Solace Queues](https://docs.solace.com/Features/Endpoints.htm#Queues).  Each queue binding is contained within their own slice of the Beam runner, from which, each will poll and read from their assigned queues in parallel.  Messages within checkpoints are acknowledged (client acks) and deleted from Solace in batches as each checkpoint is committed by Apache Beam.

## Usage

### Video Tutorial

[Getting Started With PubSub+ Connector for Beam: I/O](https://solace.com/resources/hybrid-cloud/getting-started-pubsub-connector-beam)

### Updating Your Build

The releases from this project are hosted in [Maven Central](https://mvnrepository.com/artifact/com.solace.connector.beam/beam-sdks-java-io-solace).

Here is how to include the Beam I/O Connector in your project using Gradle and Maven.

#### Using it with Gradle
```groovy
// Solace PubSub+ Connector for Beam: I/O
compile("com.solace.connector.beam:beam-sdks-java-io-solace:1.0.+")
```

#### Using it with Maven
```xml
<!-- Solace PubSub+ Connector for Beam: I/O -->
<dependency>
  <groupId>com.solace.connector.beam</groupId>
  <artifactId>beam-sdks-java-io-solace</artifactId>
  <version>1.0.+</version>
</dependency>
```


### Reading from Solace PubSub+
To instantiate a Beam I/O Connector, a PCollection must be created within the context of a pipeline.  Beam [programming-guide](https://beam.apache.org/documentation/programming-guide/) explains this concept.

```java
Pipeline pipeline = Pipeline.create(PipelineOptions options);
PCollection<SolaceTextRecord> input = pipeline.apply(
        SolaceIO.read(JCSMPProperties jcsmpProperties, List<String> queues, Coder<T> coder, SolaceIO.InboundMessageMapper<T> mapper)
                .withUseSenderTimestamp(boolean useSenderTimestamp)
                .withAdvanceTimeoutInMillis(int advanceTimeoutInMillis)
                .withMaxNumRecords(long maxNumRecords)
                .withMaxReadTime(Duration maxReadTime));
```

**Note:** This connector does not come with any de-duplication features. It is up to the application developer to design their own duplicate message filtering mechanism.

### Configuration

| Parameter              | Default          | Description  |
|------------------------|------------------|--------------|
| jcsmpProperties        | _Requires input_ | Solace PubSub+ connection config. |
| queues                 | _Requires input_ | List of queue names to consume from. These queues must be pre-configured. Add duplicate entries to this list to have multiple consumers reading from the same queue. |
| inboundMessageMapper   | _Requires input_ | The mapper object used for converting Solace messages to elements of the resulting PCollection. |
| coder                  | _Requires input_ | Defines how to encode and decode the elements of the resulting PCollection. |
| useSenderTimestamp     | false            | Use sender timestamps to determine the freshness of data, otherwise use the time at which Beam receives the data. |
| advanceTimeoutInMillis | 100              | Message poll timeout. If the poll timeout is exceeded, then it will be treated as if no messages were available at that time. |
| maxNumRecords          | Long.MAX_VALUE   | Max number of records received by the SolaceIO.Read. When this max number of records is lower than Long.MAX_VALUE, the SolaceIO.Read will provide a bounded PCollection. |
| maxReadTime            | null             | Max read time (duration) for which the SolaceIO.Read will receive messages. When this max read time is not null, the SolaceIO.Read will provide a bounded PCollection. |


### PubSub+ Broker Configuration for Apache Beam

#### Allow Apache Beam to Detect Message Backlog to Scale Workers

Apache Beam uses the message backlog as one of its parameters to determine whether or not to scale its workers. To detect the amount of backlog that exists for a particular queue, the Beam I/O Connector sends a SEMP-over-the-message-bus request to the broker. But for it to be able to do this, [show commands for SEMP-over-the-message-bus must be enabled](https://docs.solace.com/SEMP/Using-Legacy-SEMP.htm#Configur).

#### Get More Accurate Latency Measurements

By default, the latency measurement for a particular message is taken at the time that the message is read by the Beam I/O Connector. It does not take into account the time that this message is sitting in a Solace queue waiting to be processed.

But if messages are published with sender timestamps and useSenderTimestamp is enabled for the Beam I/O Connector, then end-to-end latencies will be used and reported. For publishing java clients, the JCSMP property, [GENERATE_SEND_TIMESTAMPS](https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/com/solacesystems/jcsmp/JCSMPProperties.html#GENERATE_SEND_TIMESTAMPS), will ensure that each outbound message is sent with a timestamp.

#### Get a More Consistent Message Consumption Rate

Messages consumed by Apache Beam are acknowledged when the Beam runner decides that it is safe to do so. This has potential to cause message consumption rates to stagger if the queues' maximum delivered unacknowledged messages per flow is too low. To prevent this, this setting should be configured to be equal to at least your required nominal message rate multiplied by your pipeline's window size.

Here is the link to the instructions for configuring a queue's maximum delivered unacknowledged messages per flow:

https://docs.solace.com/Configuring-and-Managing/Configuring-Queues.htm#managing_guaranteed_messaging_1810020758_455709

## Sample Walkthrough

### Acquire a Solace PubSub+ Service

To run the samples you will need a Solace PubSub+ Event Broker.
Here are two ways to quickly get started if you don't already have a PubSub+ instance:

1. Get a free Solace PubSub+ event broker cloud instance
    * Visit https://solace.com/products/event-broker/cloud/
    * Create an account and instance for free
1. Run the Solace PubSub+ event broker locally
    * Visit https://solace.com/downloads/
    * A variety of download options are available to run the software locally
    * Follow the instructions for whatever download option you choose

### Configure a Solace PubSub+ VM for Apache Beam

1. Enable show commands for SEMP-over-the-message-bus
1. Create your queues
    * For the sake of this tutorial, lets say you created two queues: `Q/fx-001` and `Q/fx-002`

### Populate the Solace PubSub+ Queues

Skip this section if you will be running the [SolaceProtoBuffRecordTest](#solaceprotobuffrecordtest) sample.

1. Download [SDKPerf](https://solace.com/downloads/#other-software-other) and extract the archive
    * For the sake of this tutorial, lets say you downloaded C SDKPerf
1. Load 100 test messages onto your queues:
    ```shell script
    sdkperf_c -cip=${SOLACE_URI} -cu="${USERNAME}@${SOLACE_VPN}" -cp=${PASSWORD} -mt=persistent -mn=100 -mr=10 -pfl=README.md -pql=Q/fx-001,Q/fx-002
    ```

For [SolaceBigQuery](#solacebigquery) sample, to be able to use it as it is, you need to use payload of a certain schema such as:
```
[{"date":"2020-06-07","sym":"DUMMY","time":"22:58","lowAskSize":20,"highAskSize":790,"lowBidPrice":43.13057,"highBidPrice":44.95833,"lowBidSize":60,"highBidSize":770,"lowTradePrice":43.51274,"highTradePrice":45.41246,"lowTradeSize":0,"highTradeSize":480,"lowAskPrice":43.67592,"highAskPrice":45.86658,"vwap":238.0331}]
```


### Run a Sample

#### SolaceRecordTest

The [SolaceRecordTest](solace-apache-beam-samples/src/main/java/com/solace/connector/beam/examples/SolaceRecordTest.java) example counts the number of each word in the received Solace message payloads and outputs the results as log messages.

1. Run the SolaceRecordTest sample on a local Apache Beam runner to consume messages:
    ```shell script
    mvn -e compile exec:java \
       -Dexec.mainClass=com.solace.connector.beam.examples.SolaceRecordTest \
       -Dexec.args="--sql=Q/fx-001,Q/fx-002 --cip=${SOLACE_URI} --cu=${SOLACE_USERNAME} --cp=${SOLACE_PASSWORD} --vpn=${SOLACE_VPN}" \
       > build.log 2> output.log &
    ```
1. Verify the messages were received and acknowledged:
    ```shell script
    grep -E "SolaceRecordTest - \*\*\*CONTRIBUTING. [0-9]+" output.log
    ```

#### WindowedWordCountSolace

The [WindowedWordCountSolace](solace-apache-beam-samples/src/main/java/com/solace/connector/beam/examples/WindowedWordCountSolace.java) example counts the number of each word in the received Solace message payloads and outputs the results to Google Cloud Storage.

1. Follow the [Before you Begin section in Google's Apache Beam Quickstart](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven#before-you-begin) to setup your GCP environment for this sample
1. Run the WindowedWordCountSolace sample in Google Dataflow:
    ```shell script
    mvn compile exec:java \
       -Pdataflow-runner \
       -Dexec.mainClass=com.solace.connector.beam.examples.WindowedWordCountSolace \
       -Dexec.args="--runner=DataflowRunner --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=2 --sql=Q/fx-001,Q/fx-002 --project=${GCP_PROJECT} --gcpTempLocation=${GOOGLE_STORAGE_TMP} --stagingLocation=${GOOGLE_STORAGE_STAGING} --output=${GOOGLE_STORAGE_OUTPUT} --cip=${SOLACE_URI} --cu=${SOLACE_USERNAME} --cp=${SOLACE_PASSWORD} --vpn=${SOLACE_VPN}"
    ```
1. Verify that the messages were received and acknowledged by going to `$GOOGLE_STORAGE_OUTPUT` and verify that files were outputted into there.

#### SolaceProtoBuffRecordTest

The [SolaceProtoBuffRecordTest](solace-apache-beam-samples/src/main/java/com/solace/connector/beam/examples/SolaceProtoBuffRecordTest.java) example sends and receives [Protocol Buffer](https://google.github.io/proto-lens/installing-protoc.html) generated messages. These messages are sent using JCSMP and received and outputted as log messages by an Apache Beam pipeline.

1. Run the SolaceProtoBuffRecordTest sample on a local Apache Beam runner to send and consume messages:
    ```shell script
    mvn -e compile exec:java \
       -Dexec.mainClass=com.solace.connector.beam.examples.SolaceProtoBuffRecordTest \
       -Dexec.args="--sql=Q/fx-001,Q/fx-002 --cip=${SOLACE_URI} --cu=${SOLACE_USERNAME} --cp=${SOLACE_PASSWORD} --vpn=${SOLACE_VPN}"
    ```
1. Verify that the messages are being outputted in the log.

#### SolaceBigQuery

The [SolaceBigQuery](solace-apache-beam-samples/src/main/java/com/solace/connector/beam/examples/SolaceBigQuery.java) example receives a message from PubSub+ queue, transforms it, and then writes it to a BigQuery table. 

1. Create a BigQuery table with appropriate schema:
```
[
 {
 
   "name": "date",
   "type": "STRING",
   "mode": "NULLABLE"
 },
 {
   "name": "sym",
   "type": "STRING",
   "mode": "NULLABLE"
 },
 {
   "name": "time",
   "type": "STRING",
   "mode": "NULLABLE"
 },
 {
   "name": "lowAskSize",
   "type": "INTEGER",
   "mode": "NULLABLE"
 },
 {
   "name": "highAskSize",
   "type": "INTEGER",
   "mode": "NULLABLE"
 },
 {
   "name": "lowBidPrice",
   "type": "FLOAT",
   "mode": "NULLABLE"
 },
 {
   "name": "highBidPrice",
   "type": "FLOAT",
   "mode": "NULLABLE"
 },
 {
   "name": "lowBidSize",
   "type": "INTEGER",
   "mode": "NULLABLE"
 },
 {
   "name": "highBidSize",
   "type": "INTEGER",
   "mode": "NULLABLE"
 },
 {
   "name": "lowTradePrice",
   "type": "FLOAT",
   "mode": "NULLABLE"
 },
 {
   "name": "highTradePrice",
   "type": "FLOAT",
   "mode": "NULLABLE"
 },
 {
   "name": "lowTradeSize",
   "type": "INTEGER",
   "mode": "NULLABLE"
 },
 {
   "name": "highTradeSize",
   "type": "INTEGER",
   "mode": "NULLABLE"
 },
 {
   "name": "lowAskPrice",
   "type": "FLOAT",
   "mode": "NULLABLE"
 },
 {
   "name": "highAskPrice",
   "type": "FLOAT",
   "mode": "NULLABLE"
 },
 {
   "name": "vwap",
   "type": "FLOAT",
   "mode": "NULLABLE"
 }
]
```
1. Modify SolaceBigQuery sample to add details about your table (project id, dataset id, and table id) in the following section:
```java
TableReference tableSpec =
                new TableReference()
                        .setProjectId("<project_id>")
                        .setDatasetId("<dataset_id>")
                        .setTableId("<table_id>");
```

1. Run the SolaceBigQuery sample on Google Dataflow:
```shell script
mvn compile exec:java -Dexec.mainClass=com.solace.connector.beam.examples.SolaceBeamBigQuery -Dexec.args="--sql=Q/fx --cip=${SOLACE_URI} --cu=${SOLACE_USERNAME} --cp=${SOLACE_PASSWORD} --vpn=${SOLACE_VPN} --project=<project_name> --tempLocation=gs://<bucket_name>/demo --workerMachineType=n1-standard-2 --runner=DataflowRunner --autoscalingAlgorithm=THROUGHPUT_BASED --maxNumWorkers=4 --stagingLocation=gs://<bucket_name>/staging" -Pdataflow-runner
```

1. Verify that the messages were written to your BigQuery table.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

See the list of [contributors](../../graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.

## Resources

For more information about Solace technology in general please visit these resources:

- The [Solace Developers website](https://www.solace.dev/)
- Understanding [Solace technology]( https://solace.com/products/tech/)
- Ask the [Solace Community]( https://solace.community/)
