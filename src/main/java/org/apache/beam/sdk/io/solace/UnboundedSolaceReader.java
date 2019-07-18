package org.apache.beam.sdk.io.solace;

import com.google.common.annotations.VisibleForTesting;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

import org.apache.beam.sdk.io.UnboundedSource;

import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;



/**
 * Unbounded Reader to read messages from a Solace Router.
 */
@VisibleForTesting
class UnboundedSolaceReader<T> extends UnboundedSource.UnboundedReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSolaceReader.class);

  // The closed state of this {@link UnboundedSolaceReader}. If true, the reader
  // has not yet been closed,
  private AtomicBoolean active = new AtomicBoolean(true);

  private final UnboundedSolaceSource<T> source;
  private JCSMPSession session;
  private XMLMessageProducer prod;
  private FlowReceiver flowReceiver;
  private boolean isAutoAck;
  private boolean useSenderTimestamp;
  private String clientName;
  private Topic sempTopic;
  private String sempVersion;
  private T current;
  private Instant currentTimestamp;

  /**
   * Queue to place advanced messages before {@link #getCheckpointMark()} be
   * called non concurrent queue, should only be accessed by the reader thread A
   * given {@link UnboundedReader} object will only be accessed by a single thread
   * at once.
   */
  private java.util.Queue<BytesXMLMessage> wait4cpQueue = new LinkedList<BytesXMLMessage>();

  /**
   * Queue to place messages ready to ack, will be accessed by
   * {@link #getCheckpointMark()} and
   * {@link SolaceCheckpointMark#finalizeCheckpoint()} in defferent thread.
   */
  private BlockingQueue<BytesXMLMessage> safe2ackQueue = new LinkedBlockingQueue<BytesXMLMessage>();

  public class PrintingPubCallback implements JCSMPStreamingPublishEventHandler {
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

  public UnboundedSolaceReader(UnboundedSolaceSource<T> source) {
    this.source = source;
    this.current = null;
    sempVersion = "soltr/8_13";
  }

  @Override
  public boolean start() throws IOException {
    LOG.info("Starting UnboundSolaceReader for Solace queue: {} ...", source.getQueueName());
    SolaceIO.Read<T> spec = source.getSpec();
    try {
      SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration();
      final JCSMPProperties properties = new JCSMPProperties();
      properties.setProperty(JCSMPProperties.HOST, cc.getHost()); // host:port
      properties.setProperty(JCSMPProperties.USERNAME, cc.getUsername()); // client-username
      properties.setProperty(JCSMPProperties.PASSWORD, cc.getPassword()); // client-password
      properties.setProperty(JCSMPProperties.VPN_NAME, cc.getVpn()); // message-vpn

      if (cc.getClientName() != null) {
        properties.setProperty(JCSMPProperties.CLIENT_NAME, cc.getClientName()); // message-vpn
      }

      session = JCSMPFactory.onlyInstance().createSession(properties);
      prod = session.getMessageProducer(new PrintingPubCallback());
      clientName = (String) session.getProperty(JCSMPProperties.CLIENT_NAME);
      session.connect();

      String routerName = (String) session.getCapability(CapabilityType.PEER_ROUTER_NAME);
      final String sempTopicString = String.format("#SEMP/%s/SHOW", routerName);
      sempTopic = JCSMPFactory.onlyInstance().createTopic(sempTopicString);

      // do NOT provision the queue, so "Unknown Queue" exception will be threw if the
      // queue is not existed already
      final Queue queue = JCSMPFactory.onlyInstance().createQueue(source.getQueueName());

      // Create a Flow be able to bind to and consume messages from the Queue.
      final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
      flow_prop.setEndpoint(queue);

      isAutoAck = spec.connectionConfiguration().isAutoAck();
      if (isAutoAck) {
        // auto ack the messages
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
      } else {
        // will ack the messages in checkpoint
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
      }

      this.useSenderTimestamp = spec.connectionConfiguration().isSenderTimestamp();

      EndpointProperties endpointProps = new EndpointProperties();
      endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
      // bind to the queue, passing null as message listener for no async callback
      flowReceiver = session.createFlow(null, flow_prop, endpointProps);
      // Start the consumer
      flowReceiver.start();
      LOG.info("Starting Solace session [{}] on queue[{}]...", clientName, source.getQueueName());

      return advance();

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new IOException(ex);
    }
  }

  @Override
  public boolean advance() throws IOException {
    // LOG.debug("Advancing Solace session [{}] on queue[{}]..."
    // , clientName
    // , source.getQueueName()
    // );
    SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration();
    try {
      BytesXMLMessage msg = flowReceiver.receive(cc.getTimeoutInMillis());
      if (msg == null) {
        return false;
      }

      current = this.source.getSpec().messageMapper().mapMessage(msg);

      // if using sender timestamps use them, else use current time.
      if (useSenderTimestamp) {
        Long solaceTime = msg.getSendTimestamp();
        if (solaceTime == null) {
            currentTimestamp = Instant.now();
          } else {
            currentTimestamp = new Instant(solaceTime.longValue());
          }
      } else {
        currentTimestamp = Instant.now();
      }

      // add message to checkpoint ack if not autoack
      if (!isAutoAck) {
        wait4cpQueue.add(msg);
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Close the Solace session [{}] on queue[{}]...", clientName, source.getQueueName());
    active.set(false);
    try {
      if (flowReceiver != null) {
        flowReceiver.close();
      }
      if (session != null) {
        session.closeSession();
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    // TODO: add finally block to close session.
  }

  /**
   * Direct Runner will call this method on every second.
   */
  @Override
  public Instant getWatermark() {
    if (current == null) {
      return Instant.now();
    }
    return currentTimestamp;
  }

  @Override
  public UnboundedSource.CheckpointMark getCheckpointMark() {
    // put all messages in wait4cp to safe2ack
    // and clean the wait4cp queue in the same time
    SolaceCheckpointMark scm = null;
    try {
      BytesXMLMessage msg = wait4cpQueue.peek();
    /*  BytesXMLMessage msg = wait4cpQueue.poll();
      while (msg != null) {
        safe2ackQueue.put(msg);
        msg = wait4cpQueue.poll();
      }
    */
      scm = new SolaceCheckpointMark(this, clientName, msg);
    
    } catch (Exception ex) {
      LOG.error("Got exception while putting into the blocking queue: {}", ex);
    }

    return scm;
  }

  /**
   * Ack all message in the safe2ackQueue called by
   * {@link SolaceCheckpointMark #finalizeCheckpoint()} It's possible for a
   * checkpoint to be taken but never finalized So we simply ack all messages
   * which are read to be ack.
   */
  public void ackMessages(BytesXMLMessage message) throws IOException {
    LOG.debug("try to ack {} messages with {} Session [{}]", safe2ackQueue.size(), 
        active.get() ? "active" : "closed",
        clientName);

    if (!active.get()) {
      return;
    }
    try {
     /*while (safe2ackQueue.size() > 0) {
        BytesXMLMessage msg = safe2ackQueue.poll(0, TimeUnit.NANOSECONDS);
        if (msg != null) {
          msg.ackMessage();
        }
      }
    */
    
    BytesXMLMessage msg = wait4cpQueue.poll();
      while (msg != message) {
        if (msg != null) {
          msg.ackMessage();
        }
        msg = wait4cpQueue.poll();
      }
      msg.ackMessage();
    
    } catch (Exception ex) {
      LOG.error("Got exception while acking the message: {}", ex);
      throw new IOException(ex);
    }
  }

  @Override
  public T getCurrent() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public Instant getCurrentTimestamp() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return currentTimestamp;
  }

  @Override
  public UnboundedSolaceSource<T> getCurrentSource() {
    return source;
  }

  public long queryQueueBytes(String queueName, String vpnName) {
    long queueBytes = 0;
    String sempShowQueue = "<rpc semp-version=\"" + sempVersion + "\">";
    sempShowQueue += "<show><queue><name>" + queueName + "</name>";
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
      String expression = "/rpc-reply/rpc/show/queue/queues/queue/info/num-messages-spooled";
      Node node = (Node) xpath.compile(expression).evaluate(doc, XPathConstants.NODE);
      queueBytes = Long.parseLong(node.getTextContent());
    } catch (JCSMPException ex) {
      LOG.error("Encountered a JCSMPException querying queue depth: {}", ex.getMessage());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    } catch (Exception ex) {
      LOG.error("Encountered a Parser Exception querying queue depth: {}", ex.toString());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }
    return queueBytes;
  }

  @Override
  public long getSplitBacklogBytes() {
    LOG.debug("Enter getSplitBacklogBytes()");
    long backlogBytes = 0;
    long queuBacklog = queryQueueBytes(source.getQueueName(), source.getVpnName());
    if (queuBacklog == UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN) {
      LOG.error("getSplitBacklogBytes() unable to read bytes from: {}", source.getQueueName());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }
    LOG.debug("getSplitBacklogBytes() Reporting backlog bytes of: {} from queue {}", 
        Long.toString(backlogBytes), source.getQueueName());
    return backlogBytes;
  }
}
