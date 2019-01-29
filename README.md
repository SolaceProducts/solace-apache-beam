[![Build Status](https://travis-ci.org/SolaceLabs/solace-beam-integration.svg?branch=master)](https://travis-ci.org/SolaceLabs/solace-beam-integration)

# solace-beam-integration

## Synopsis

This repository provides a solution which allows applications connected to a Solace Event Mesh to interop with Apache Beam.

Consider the following diagram:

![Architecture Overview](images/overview.png)

It does not matter if the application communicates with the Solace PubSub+ broker via a REST POST or AMQP, JMS or MQTT message, it can be sent streaming to Apache Beam for further processing by it's data runners and further on to other IO connectors such as BigQuery or Apache Cassandra, etc.

The Solace Event Mesh is a clustered group of Solace PubSub+ Brokers that transparently, in real-time, route data events to any Service that is part of the Event Mesh. Solace PubSub+ Brokers (Appliances, Software and SolaceCloud) are connected to each other as a multi-connected mesh that to individual services (consumers or producers of data events) appears to be a single Event Broker. Events messages are seamlessly transported within the entire Solace Event Mesh regardless of where the event is created and where the process exists that has registered interested in consuming the event. 

Simply by having a Beam connected Solace PubSub+ broker added to the Event Mesh,the entire Event Mesh becomes aware of the data registration request and will know how to securely route the appropriate events to and from Beam Connected services.
This Beam integration solution allows any event from any service in the Solace Event Mesh to send [to be captured in:](https://beam.apache.org/documentation/io/built-in/)
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
* Avro Fromat

It does not matter which service in the Event Mesh created the event, the events are all potentially available to Beam Connected services. There is no longer a requirement to code end applications to reach individual data services.

![Event Mesh](images/EventMesh.png)

## Design

To understand 

https://beam.apache.org/documentation/runners/capability-matrix/

## Design

The overall design is to tightly couple the message brokers to the apiGateway for security reasons.  The apiGateway is internal in nature and coupled to the message broker by subnet and security group.  If you want to make access to this ApiGateway more open, you can deploy the below cloud formation template then manually open and expand permissions or simply use this repo as inspiration to create your own AWS service interfaces.

### High Level Data Flow

If you look at the following diagram from left to right:

![High Level Data Flow](images/data-view.png)

1. Messages come into the Solace Message broker through various open messaging protocols and are placed into a [Solace Queue](https://docs.solace.com/Features/Endpoints.htm#Queues)

2. A [REST Delivery Endpoint](https://docs.solace.com/Open-APIs-Protocols/Using-REST.htm) within the message broker takes the message and sends it in a HTTP REST POST.

3. The AWS API Gateway has a [Webhook](https://en.wikipedia.org/wiki/Webhook) interface that consumes the REST calls and injects them into the AWS application integration service in natural AWS calls.

This system allows for data retrieval in the reverse direction as well as ability to delete data in AWS.

The advantage of this design is that movement of data from open messaging protocol up to the AWS APIGateway happens all within the Solace Message Broker, no added bridges, gateways, or 3rd party pluggins are required.  So, this data is treated with the same priority, reliability and performance as all Solace enterprise messaging.

### Detailed Topology Example
![Detailed Architecture](images/DetailedArch.png)

Breaking down the above diagram into its component parts:
1. End application - In this example a Spring app communicating with JMS is outside the scope of this solution and is assumed to pre-exist.  It would communicate with the Solace message broker in normal fashion.

2. Solace Message Broker - Can be optionally provided or defined within the solution.  Will terminate the application connection and deliver messages via Rest Delivery Endpoints to the APIGateway.

3. VPC Endpoint - Defined within solution, provides a private interface to the APIGateway from within the defined subnet only.

4. Security Group - Defined within solution.  Allows only the created Message Broker(s) to communicate with the APIGateway.  This is based on security group membership no IPs or other credentials.

5. APIGateway - Defined within solution. Converts the Solace provided message to a signed REST call formatted for the target downstream resource, (SQS, SNS, S3, Lambda, Kinesis).

6. IAM Role - Defined within solution. Allows read/write access to the specific downstream resource, can be across accounts.

7. AWS Resource.  In this example an SQS Queue is outside the scope of this solution and is assumed to pre-exist.  API gateway can write to a specific object or read from it.

## Usage

### Minimum Resource Requirements
Below is the list of 

## Deploying solution
The solution is deployed ...

### Launch option 1: Parameters for deploying only APIGateway

| Parameter label (name)     | Default   | Description                                                        |
|----------------------------|-----------|--------------------------------------------------------------------|
| Stack name                 | Solace-APIGW | Any globally unique name                                           |
| **Resource Parameters** | |                                                                     |
| Type of Resource (ResourceType) | _Requires_ _input_ | One of S3, SQS, SNS, Lambda, Kinesis, from picklist |
| ARN of Resource (ResourceARN) | _Requires_ _input_ | The actual resource ARN being written to and read from, see below for further deployment details |
| **AWS Quick Start Configuration** | |                                                                     |
| Subnet IDs (SubnetID) | _Requires_ _input_ | Choose public subnet IDs in your existing VPC from this list (e.g., subnet-4b8d329f,subnet-bd73afc8,subnet-a01106c2), matching your deployment architecture. |
| VPC ID (VPCID)             | _Requires_ _input_ | Choose the ID of your existing VPC stack - for a value, refer to the `VPCID` in the "VPCStack"'s `Outputs` tab in the AWS CloudFormation view (e.g., vpc-0343606e). This VPC must exist with the proper configuration for Solace cluster access. |
| Quick Start S3 Bucket Name (QSS3BucketName) | solace-products | S3 bucket where the Quick Start templates and scripts are installed. Change this parameter to specify the S3 bucket name you’ve created for your copy of Quick Start assets, if you decide to customize or extend the Quick Start for your own use. |
| Quick Start S3 Key Prefix (QSS3KeyPrefix) | solace-aws-ha-quickstart/latest/ | Specifies the S3 folder for your copy of Quick Start assets. Change this parameter if you decide to customize or extend the Quick Start for your own use. |

### Launch option 2: Parameters for deploying APIGateway and Solace Message Broker

| Parameter label (name)     | Default   | Description                                                        |
|----------------------------|-----------|--------------------------------------------------------------------|
| Stack name                 | Solace-APIGW | Any globally unique name                                           |
| **Solace Parameters**      |           |                                                                    |
| CLI and SEMP SolOS admin password (AdminPassword)| _Requires_ _input_ | Password to allow Solace admin access to configure the message broker instances |
| Key Pair Name (KeyPairName) | _Requires_ _input_ | A new or an existing public/private key pair within the AWS Region, which allows you to connect securely to your instances after launch. |
| Instance Type (NodeInstanceType) | t2.large | The EC2 instance type for the Solace message broker The m series are recommended for production use. <br/> The available CPU and memory of the selected machine type will limit the maximum connection scaling tier for the Solace message broker. For requirements, refer to the [Solace documentation](https://docs.solace.com/Solace-SW-Broker-Set-Up/Setting-Up-SW-Brokers.htm#Compare) |
| Security Groups for external access (SecurityGroupID) | _Requires_ _input_ | The ID of the security group in your existing VPC that is allowed to access the console and Data|

<br/><br/>

Select [next] after completing the parameters form to get to the "Options" screen.

Select [next] on the "Options" screen unless you want to add tags, use specific IAM roles, or blend in custom stacks.

Acknowledge that resources will be created and select [Create] in bottom right corner.

![alt text](/images/capabilities.png "Create Stack")

### Deploying SQS solution
The expected message pattern might be to send messages to a SQS queue and receive messages from an SQS queue.  In reality the message patterns will likely be:
1.  Send messages from Solace event mesh to a SQS queue.
2.  Poll an SQS queue for messages eligible for delivery and receive these messages.
3.  Delete each delivered message that has been successfully consumed.

If a message has been delivered but not yet deleted it moved from eligible for deliver to in-flight.  From here it can either be explicitly deleted with a delete message or moved back to eligible for delivery after delete timeout, which is by default 15 seconds.

When sending and receiving messages to/from an SQS queue, you will need an existing queue.  The ARN for the queue can be found from the AWS console by looking at the SQS queue details and will be of the form:

    arn:aws:sqs:<aws region>:<aws accountId>:<queue name>

When sending messages that would go to the SQS queue, the binary attachment of the message will be sent AWS queue. Custom headers will not be passed to SQS.

When receiving messages from SQS queue, a request/reply pattern should be used if the sending protocol supports it.  The reply will include the message payload, message payload MD5 and the ReceiptHandle.  This ReceiptHandle needs to be the only element in the delete message body and needs to be sent within 15 seconds of the initial delivery or the message will be re-queued within SQS for delivery.  If the receiving protocol does not support request/reply, then a monitor application can async receive data generated from the polling and subsequently delete it.

Here is an example exchange pattern:

    Send Message
    pubSubTools/sdkperf_c -cip="${publicIp}" -ptl=solace-beam-integration/send -mr=1 -mn=1 -pal messageBody

    Receive Message
    pubSubTools/sdkperf_c -cip="${publicIp}" -stl=solace-beam-integration/receive/reply -ptl=solace-beam-integration/receive -prp=/reply -mr=1 -mn=1 -md

    Extract the ReceiptHandle from the received message ReceiptHandle and delete from queue
    pubSubTools/sdkperf_c -cip="${publicIp}" -ptl=solace-beam-integration/delete -mr=1 -mn=1 -pal ReceiptHandle

### Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

See the list of [contributors](../../graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.

## Resources

For more information about Solace technology in general please visit these resources:

- The Solace Developer Portal website at: http://dev.solace.com
- Understanding [Solace technology.](http://dev.solace.com/tech/)
- Ask the [Solace community](http://dev.solace.com/community/).