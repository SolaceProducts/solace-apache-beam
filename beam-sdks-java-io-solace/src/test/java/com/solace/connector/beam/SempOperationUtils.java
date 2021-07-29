package com.solace.connector.beam;

import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.action.model.ActionMsgVpnClientDisconnect;
import com.solace.test.integration.semp.v2.action.model.ActionMsgVpnQueueMsgDelete;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnClient;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnClientsResponse;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsg;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @deprecated use {@link com.solace.test.integration.semp.v2.SempV2Api}
 */
@Deprecated
public class SempOperationUtils {
	private static final Logger LOG = LoggerFactory.getLogger(SempOperationUtils.class);

	private final SempV2Api sempV2Api;
	private final MsgBusSempUtil msgBusSempUtil;

	public SempOperationUtils(SempV2Api sempV2Api, JCSMPSession jcsmpSession,
							  boolean createProducer, boolean createConsumer) {
		this.sempV2Api = sempV2Api;
		this.msgBusSempUtil = new MsgBusSempUtil(jcsmpSession, createProducer, createConsumer);
	}

	public void start() throws JCSMPException {
		msgBusSempUtil.start();
	}

	public void close() {
		msgBusSempUtil.close();
	}

	public void disconnectClients(JCSMPProperties jcsmpProperties, Collection<String> ignoreClients) throws
			com.solace.test.integration.semp.v2.monitor.ApiException,
			com.solace.test.integration.semp.v2.action.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		String cursorQuery = null;
		List<String> clients = new ArrayList<>();
		do {
			MonitorMsgVpnClientsResponse response = sempV2Api.monitor().getMsgVpnClients(msgVpn, Integer.MAX_VALUE, cursorQuery, null, null);
			clients.addAll(response.getData()
					.stream()
					.map(MonitorMsgVpnClient::getClientName)
					.filter(c -> !c.startsWith("#") && !ignoreClients.contains(c))
					.collect(Collectors.toList()));
			cursorQuery = response.getMeta().getPaging() != null ? response.getMeta().getPaging().getCursorQuery() : null;
		} while (cursorQuery != null);

		for (String clientName : clients) {
			LOG.info(String.format("Disconnecting client %s", clientName));
			sempV2Api.action().doMsgVpnClientDisconnect(msgVpn, clientName, new ActionMsgVpnClientDisconnect());
		}
	}

	public void updateQueue(JCSMPProperties jcsmpProperties, String queueName, ConfigMsgVpnQueue queuePatch) throws
			com.solace.test.integration.semp.v2.config.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);

		LOG.info(String.format("Updating queue %s:\n %s", queueName, queuePatch));
		shutdownQueueEgress(jcsmpProperties, queueName);
		sempV2Api.config().updateMsgVpnQueue(msgVpn, queueName, queuePatch, null);
		enableQueueEgress(jcsmpProperties, queueName);
	}

	public void shutdownQueueEgress(JCSMPProperties jcsmpProperties, String queueName) throws
			com.solace.test.integration.semp.v2.config.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		ConfigMsgVpnQueue queuePatch = new ConfigMsgVpnQueue().egressEnabled(false);

		LOG.info(String.format("Shutting down egress for queue %s", queueName));
		sempV2Api.config().updateMsgVpnQueue(msgVpn, queueName, queuePatch, null);
	}

	public void enableQueueEgress(JCSMPProperties jcsmpProperties, String queueName) throws
			com.solace.test.integration.semp.v2.config.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		ConfigMsgVpnQueue queuePatch = new ConfigMsgVpnQueue().egressEnabled(true);
		LOG.info(String.format("Enabling egress for queue %s", queueName));
		sempV2Api.config().updateMsgVpnQueue(msgVpn, queueName, queuePatch, null);
	}

	public void drainQueues(JCSMPProperties jcsmpProperties, Collection<String> queues) throws ApiException,
			com.solace.test.integration.semp.v2.action.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		for (String queueName: new HashSet<>(queues)) {
			LOG.info(String.format("Draining queue %s", queueName));
			List<MonitorMsgVpnQueueMsg> messages;
			do {
				messages = sempV2Api.monitor().getMsgVpnQueueMsgs(msgVpn, queueName, Integer.MAX_VALUE, null, null, null).getData();
				for (MonitorMsgVpnQueueMsg message : messages) {
					sempV2Api.action().doMsgVpnQueueMsgDelete(msgVpn, queueName, String.valueOf(message.getMsgId()), new ActionMsgVpnQueueMsgDelete());
				}
			} while (!messages.isEmpty());
		}
	}

	public long getQueueMessageCount(JCSMPProperties jcsmpProperties, String queueName)
			throws SAXException, TransformerException, IOException, XPathExpressionException, JCSMPException, ParserConfigurationException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		String request = String.format("<rpc><show><queue><name>%s</name><vpn-name>%s</vpn-name></queue></show></rpc>", queueName, msgVpn);
		String searchString = "/rpc-reply/rpc/show/queue/queues/queue/info/num-messages-spooled";
		return Long.parseLong(msgBusSempUtil.queryRouter(request, searchString));
	}

	public long getQueueUnackedMessageCount(JCSMPProperties jcsmpProperties, String queueName) throws ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		return sempV2Api.monitor().getMsgVpnQueue(msgVpn, queueName, null).getData().getTxUnackedMsgCount();
	}

	public long getQueueMaxDeliveredUnackedMsgsPerFlow(JCSMPProperties jcsmpProperties, String queueName) throws ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		return sempV2Api.monitor().getMsgVpnQueue(msgVpn, queueName, null).getData().getMaxDeliveredUnackedMsgsPerFlow();
	}

	public boolean isQueueEmpty(JCSMPProperties jcsmpProperties, String queueName) throws ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		return sempV2Api.monitor().getMsgVpnQueue(msgVpn, queueName, null).getData().getMsgSpoolUsage() <= 0;
	}

	public void waitForQueuesEmpty(JCSMPProperties jcsmpProperties, Collection<String> queues, long waitSecs)
			throws InterruptedException, ParserConfigurationException,
			TransformerException, IOException, XPathExpressionException, JCSMPException, SAXException, ApiException {

		for (String queueName : new HashSet<>(queues)) {
			long wait = TimeUnit.SECONDS.toMillis(waitSecs);
			long sleep = Math.min(TimeUnit.SECONDS.toMillis(5), wait);

			while (!isQueueEmpty(jcsmpProperties, queueName)) {
				if (wait > 0) {
					LOG.info(String.format("Waiting for queue %s to become empty - %s sec remaining",
							queueName, TimeUnit.MILLISECONDS.toSeconds(wait)));
					Thread.sleep(sleep);
					wait -= sleep;
				} else {
					throw new IllegalStateException(String.format("Queue %s was not empty, found %s messages",
							queueName, getQueueMessageCount(jcsmpProperties, queueName)));
				}
			}

			LOG.info(String.format("Queue %s became empty", queueName));
		}
	}
}
