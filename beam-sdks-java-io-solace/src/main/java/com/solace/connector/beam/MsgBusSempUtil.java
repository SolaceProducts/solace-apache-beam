package com.solace.connector.beam;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;

class MsgBusSempUtil {
	private static final Logger LOG = LoggerFactory.getLogger(MsgBusSempUtil.class);

	private final JCSMPSession jcsmpSession;
	private final boolean createProducer;
	private final boolean createConsumer;
	private Topic sempShowTopic;
	private XMLMessageProducer producer;
	private XMLMessageConsumer consumer;
	private Requestor requestor;

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

	public MsgBusSempUtil(JCSMPSession jcsmpSession) {
		this(jcsmpSession, true, true);
	}

	public MsgBusSempUtil(JCSMPSession jcsmpSession, boolean createProducer, boolean createConsumer) {
		this.jcsmpSession = jcsmpSession;
		this.createProducer = createProducer;
		this.createConsumer = createConsumer;
	}

	public void start() throws JCSMPException {
		if (createProducer) {
			LOG.info(String.format("Creating %s %s for session %s", Requestor.class.getSimpleName(),
					XMLMessageProducer.class.getSimpleName(), jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));
			producer = jcsmpSession.getMessageProducer(new PrintingPubCallback());
		}

		if (createConsumer) {
			LOG.info(String.format("Creating %s %s for session %s", Requestor.class.getSimpleName(),
					XMLMessageConsumer.class.getSimpleName(), jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));
			consumer = jcsmpSession.getMessageConsumer((XMLMessageListener) null);
			consumer.start();
		}

		LOG.info(String.format("Creating %s for session %s", Requestor.class.getSimpleName(),
				jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));
		requestor = jcsmpSession.createRequestor();

		String routerName = (String) jcsmpSession.getCapability(CapabilityType.PEER_ROUTER_NAME);
		final String sempShowTopicString = String.format("#SEMP/%s/SHOW", routerName);
		sempShowTopic = JCSMPFactory.onlyInstance().createTopic(sempShowTopicString);
	}

	public void close() {
		LOG.info(String.format("Closing %s resources for session %s", Requestor.class.getSimpleName(),
				jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));

		if (producer != null) {
			LOG.info(String.format("Stopping %s %s for session %s", Requestor.class.getSimpleName(),
					XMLMessageProducer.class.getSimpleName(), jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));
			producer.close();
		}

		if (consumer != null) {
			LOG.info(String.format("Stopping %s %s for session %s", Requestor.class.getSimpleName(),
					XMLMessageConsumer.class.getSimpleName(), jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME)));
			consumer.close();
		}
	}

	public BytesXMLMessage queryRouter(String queryString) throws JCSMPException {
		BytesXMLMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
		requestMsg.writeAttachment(queryString.getBytes());
		try {
			return requestor.request(requestMsg, 5000, sempShowTopic);
		} catch (JCSMPException e) {
			if (jcsmpSession.isClosed()) {
				throw e;
			} else {
				LOG.warn(String.format("Failed to execute message bus SEMP request, restarting %s: %s",
						Requestor.class.getSimpleName(), e.getMessage()), e);
				close();
				start();
				return requestor.request(requestMsg, 5000, sempShowTopic);
			}
		}
	}

	public String queryRouter(String queryString, String searchString) throws JCSMPException, ParserConfigurationException, IOException, SAXException, XPathExpressionException, TransformerException {
		ByteArrayInputStream input;

		BytesXMLMessage replyMsg = queryRouter(queryString);
		byte[] bytes = new byte[replyMsg.getAttachmentContentLength()];
		replyMsg.readAttachmentBytes(bytes);

		input = new ByteArrayInputStream(bytes);
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(input);
		XPath xpath = XPathFactory.newInstance().newXPath();
		Node node = (Node) xpath.compile(searchString).evaluate(doc, XPathConstants.NODE);
		if (node == null || node.getTextContent() == null) {
			throw new NullPointerException(String.format("Failed to evaluate %s in %s", searchString, printDocument(doc)));
		}
		return node.getTextContent();
	}

	public static String printDocument(Document doc) throws TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
		transformer.setOutputProperty(OutputKeys.METHOD, "xml");
		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(doc), new StreamResult(writer));
		return writer.getBuffer().toString();
	}
}
