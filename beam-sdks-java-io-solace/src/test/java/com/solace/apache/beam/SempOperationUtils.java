package com.solace.apache.beam;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.solace.semp.v2.action.model.MsgVpnClient;
import com.solace.semp.v2.action.model.MsgVpnClientDisconnect;
import com.solace.semp.v2.action.model.MsgVpnClientsResponse;
import com.solace.semp.v2.action.model.MsgVpnQueueMsg;
import com.solace.semp.v2.action.model.MsgVpnQueueMsgDelete;
import com.solace.semp.v2.config.ApiClient;
import com.solace.semp.v2.config.ApiException;
import com.solace.semp.v2.config.api.QueueApi;
import com.solace.semp.v2.config.model.MsgVpnQueue;
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
import java.util.stream.Collectors;

public class SempOperationUtils {
	private static final Logger LOG = LoggerFactory.getLogger(SempOperationUtils.class);

	private String mgmtHost;
	private String mgmtUsername;
	private String mgmtPassword;

	private QueueApi queueConfigApi;
	private com.solace.semp.v2.action.api.ClientApi clientActionApi;
	private com.solace.semp.v2.action.api.QueueApi queueActionApi;

	private MsgBusSempUtil msgBusSempUtil;

	public SempOperationUtils(String mgmtHost, String mgmtUsername, String mgmtPassword, JCSMPSession jcsmpSession,
							  boolean createProducer, boolean createConsumer) {
		this.mgmtHost = mgmtHost;
		this.mgmtUsername = mgmtUsername;
		this.mgmtPassword = mgmtPassword;
		this.msgBusSempUtil = new MsgBusSempUtil(jcsmpSession, createProducer, createConsumer);
	}

	public void start() throws JCSMPException {
		msgBusSempUtil.start();

		LOG.info(String.format("Creating Config API Clients for %s", mgmtHost));
		ApiClient configApiClient = new ApiClient();
		configApiClient.getJSON().getContext(null).configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		configApiClient.setBasePath(String.format("%s/SEMP/v2/config", mgmtHost));
		configApiClient.setUsername(mgmtUsername);
		configApiClient.setPassword(mgmtPassword);

		queueConfigApi = new QueueApi(configApiClient);

		LOG.info(String.format("Creating Action API Clients for %s", mgmtHost));
		com.solace.semp.v2.action.ApiClient actionApiClient = new com.solace.semp.v2.action.ApiClient();
		actionApiClient.getJSON().getContext(null).configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		actionApiClient.setBasePath(String.format("%s/SEMP/v2/action", mgmtHost));
		actionApiClient.setUsername(mgmtUsername);
		actionApiClient.setPassword(mgmtPassword);

		clientActionApi = new com.solace.semp.v2.action.api.ClientApi(actionApiClient);
		queueActionApi = new com.solace.semp.v2.action.api.QueueApi(actionApiClient);
	}

	public void close() {
		if (msgBusSempUtil != null) {
			msgBusSempUtil.close();
		}
	}

	public void disconnectClients(JCSMPProperties jcsmpProperties, Collection<String> ignoreClients) throws com.solace.semp.v2.action.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		String cursorQuery = null;
		List<String> clients = new ArrayList<>();
		do {
			MsgVpnClientsResponse response = clientActionApi.getMsgVpnClients(msgVpn, Integer.MAX_VALUE, cursorQuery, null, null);
			clients.addAll(response.getData()
					.stream()
					.map(MsgVpnClient::getClientName)
					.filter(c -> !c.startsWith("#") && !ignoreClients.contains(c))
					.collect(Collectors.toList()));
			cursorQuery = response.getMeta().getPaging() != null ? response.getMeta().getPaging().getCursorQuery() : null;
		} while (cursorQuery != null);

		for (String clientName : clients) {
			LOG.info(String.format("Disconnecting client %s", clientName));
			clientActionApi.doMsgVpnClientDisconnect(msgVpn, clientName, new MsgVpnClientDisconnect());
		}
	}

	public void updateQueue(JCSMPProperties jcsmpProperties, String queueName, MsgVpnQueue queuePatch) throws ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);

		LOG.info(String.format("Updating queue %s:\n %s", queueName, queuePatch));
		shutdownQueueEgress(jcsmpProperties, queueName);
		queueConfigApi.updateMsgVpnQueue(msgVpn, queueName, queuePatch, null);
		enableQueueEgress(jcsmpProperties, queueName);
	}

	public void shutdownQueueEgress(JCSMPProperties jcsmpProperties, String queueName) throws ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		MsgVpnQueue queuePatch = new MsgVpnQueue().egressEnabled(false);

		LOG.info(String.format("Shutting down egress for queue %s", queueName));
		queueConfigApi.updateMsgVpnQueue(msgVpn, queueName, queuePatch, null);
	}

	public void enableQueueEgress(JCSMPProperties jcsmpProperties, String queueName) throws ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		MsgVpnQueue queuePatch = new MsgVpnQueue().egressEnabled(true);
		LOG.info(String.format("Enabling egress for queue %s", queueName));
		queueConfigApi.updateMsgVpnQueue(msgVpn, queueName, queuePatch, null);
	}

	public void drainQueues(JCSMPProperties jcsmpProperties, Collection<String> queues) throws com.solace.semp.v2.action.ApiException {
		String msgVpn = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		for (String queueName: new HashSet<>(queues)) {
			LOG.info(String.format("Draining queue %s", queueName));
			List<MsgVpnQueueMsg> messages;
			do {
				messages = queueActionApi.getMsgVpnQueueMsgs(msgVpn, queueName, Integer.MAX_VALUE, null, null, null).getData();
				for (MsgVpnQueueMsg message : messages) {
					queueActionApi.doMsgVpnQueueMsgDelete(msgVpn, queueName, String.valueOf(message.getMsgId()), new MsgVpnQueueMsgDelete());
				}
			} while (!messages.isEmpty());
		}
	}

	public boolean isQueueEmpty(JCSMPProperties jcsmpProperties, String queueName)
			throws SAXException, TransformerException, IOException, XPathExpressionException, JCSMPException, ParserConfigurationException {
		String request = String.format("<rpc><show><queue><name>%s</name><vpn-name>%s</vpn-name></queue></show></rpc>",
				queueName, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
		String queryString = "/rpc-reply/rpc/show/queue/queues/queue/info/current-spool-usage-in-bytes";
		String queryResults = msgBusSempUtil.queryRouter(request, queryString);
		return Long.parseLong(queryResults) <= 0;
	}

	public void waitForQueuesEmpty(JCSMPProperties jcsmpProperties, Collection<String> queues, long waitSecs)
			throws SAXException, TransformerException, IOException, XPathExpressionException, JCSMPException, ParserConfigurationException, InterruptedException {
		for (String queueName : new HashSet<>(queues)) {
			boolean isEmpty = false;
			long sleep = 5000;

			for (int i = 0; !isEmpty && i < (waitSecs * 1000) / sleep; i++) {
				LOG.info(String.format("Waiting for queue %s to become empty", queueName));
				isEmpty = isQueueEmpty(jcsmpProperties, queueName);
				Thread.sleep(sleep);
			}

			if (!isEmpty) {
				throw new IllegalStateException(String.format("Queue %s was not empty", queueName));
			} else {
				LOG.info(String.format("Queue %s became empty", queueName));
			}
		}
	}
}
