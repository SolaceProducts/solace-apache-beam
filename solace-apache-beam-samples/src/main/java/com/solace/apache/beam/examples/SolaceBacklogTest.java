package com.solace.apache.beam.examples;

import java.io.ByteArrayInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import com.solacesystems.jcsmp.*;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class SolaceBacklogTest {
    private static final Logger LOG = LoggerFactory.getLogger(SolaceBacklogTest.class);

    private static JCSMPSession session;
    private static Topic sempTopic;

    public interface Options extends WordCount.WordCountOptions {
      @Description("IP and port of the client appliance. (e.g. -cip=192.168.160.101)")
      String getCip();
      void setCip(String value);

      @Description("Client username and optionally VPN name.")
      String getCu();
      void setCu(String value);

      @Description("Client password (default '')")
      @Default.String("")
      String getCp();
      void setCp(String value);

      @Description("List of queues for subscribing")
      String getSql();
      void setSql(String value);

      @Description("Enable reading sender timestamp to deturmine freashness of data")
      @Default.Boolean(false)
      boolean getSts();
      void setSts(boolean value);

      @Description("Enable reading sender MessageId to deturmine duplication of data")
      @Default.Boolean(false)
      boolean getSmi();
      void setSmi(boolean value);

      @Description("The timeout in milliseconds while try to receive a messages from Solace broker")
      @Default.Integer(100)
      int getTimeout();
      void setTimeout(int timeoutInMillis);;
    }

  public static class PrintingPubCallback implements JCSMPStreamingPublishEventHandler {
    public void handleError(String messageId, JCSMPException cause, long timestamp) {
      LOG.error("Error occurred for Solace queue depth request message: " + messageId);
      cause.printStackTrace();
    }

    // This method is only invoked for persistent and non-persistent
    // messages.
    public void responseReceived(String messageId) {
      LOG.error("Unexpected response to Solace queue depth request message: " + messageId);
    }
  }

  static void runBacklogTest(Options options) {
    try {
      final JCSMPProperties properties = new JCSMPProperties();
      properties.setProperty(JCSMPProperties.HOST, options.getCip()); // host:port
      properties.setProperty(JCSMPProperties.USERNAME, options.getCu()); // client-username
      properties.setProperty(JCSMPProperties.PASSWORD, options.getCp()); // client-password
      properties.setProperty(JCSMPProperties.VPN_NAME, "default"); // message-vpn

      session = JCSMPFactory.onlyInstance().createSession(properties);
      session.getMessageProducer(new PrintingPubCallback());
      XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener)null); consumer.start();
      session.connect();

      String routerName = (String) session.getCapability(CapabilityType.PEER_ROUTER_NAME);
      final String sempTopicString = String.format("#SEMP/%s/SHOW", routerName);
      sempTopic = JCSMPFactory.onlyInstance().createTopic(sempTopicString);

      // do NOT provision the queue, so "Unknown Queue" exception will be threw if the
      // queue is not existed already
      final Queue queue = JCSMPFactory.onlyInstance().createQueue(options.getSql());

      // Create a Flow be able to bind to and consume messages from the Queue.
      final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
      flow_prop.setEndpoint(queue);
      flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

      EndpointProperties endpointProps = new EndpointProperties();
      endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
      // bind to the queue, passing null as message listener for no async callback
      FlowReceiver flowReceiver = session.createFlow(null, flow_prop, endpointProps);
      // Start the consumer
      flowReceiver.start();
      long backlog = queryQueueBytes(options.getSql(), "default");
      LOG.info("Backlog = {}", Long.toString(backlog));
    } catch (Exception ex) {
      LOG.error("Initialization Exception: {}", ex.toString());
    }
  }

  public static long queryQueueBytes(String queueName, String vpnName) {
    LOG.info("Enter queryQueueBytes() Queue: [{}] VPN: [{}]",  queueName, vpnName);
    long queueBytes = 0;
    String sempShowQueue = "<rpc><show><queue><name>" + queueName + "</name>";
    sempShowQueue += "<vpn-name>" + vpnName + "</vpn-name></queue></show></rpc>";
    try {
      Requestor requestor = session.createRequestor();
      BytesXMLMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
      requestMsg.writeAttachment(sempShowQueue.getBytes());
      BytesXMLMessage replyMsg = requestor.request(requestMsg, 5000, sempTopic);
      ByteArrayInputStream input;
      byte[] bytes = new byte[replyMsg.getAttachmentContentLength()];
      replyMsg.readAttachmentBytes(bytes);
      input = new ByteArrayInputStream(bytes);
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(input);
      XPath xpath = XPathFactory.newInstance().newXPath();
      String expression = "/rpc-reply/rpc/show/queue/queues/queue/info/current-spool-usage-in-bytes";
      Node node = (Node) xpath.compile(expression).evaluate(doc, XPathConstants.NODE);
      queueBytes = Long.parseLong(node.getTextContent());
    } catch (JCSMPException ex) {
      LOG.error("Encountered a JCSMPException querying queue depth: {}", ex.getMessage());
      return -1;
    } catch (Exception ex) {
      LOG.error("Encountered a Parser Exception querying queue depth: {}", ex.toString());
      return -1;
    }
    return queueBytes;
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    try {
      runBacklogTest(options);
    } catch (Exception e){
      e.printStackTrace();
    }
  }
}