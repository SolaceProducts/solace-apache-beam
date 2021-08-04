package com.solace.connector.beam;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SolaceIOTest {
	private JCSMPProperties testJcsmpProperties;
	private List<String> testQueues;

	@BeforeEach
	public void setup() {
		testJcsmpProperties = new JCSMPProperties();
		testJcsmpProperties.setProperty(JCSMPProperties.HOST, RandomStringUtils.randomAlphanumeric(10));
		testJcsmpProperties.setProperty(JCSMPProperties.USERNAME, RandomStringUtils.randomAlphanumeric(10));
		testJcsmpProperties.setProperty(JCSMPProperties.PASSWORD, RandomStringUtils.randomAlphanumeric(10));
		testJcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, RandomStringUtils.randomAlphanumeric(10));

		testQueues = new ArrayList<>();
		testQueues.add(UUID.randomUUID().toString());
		testQueues.add(UUID.randomUUID().toString());
	}

	@Test
	public void testReadBuildsCorrectly() {
		SolaceIO.Read<SolaceTestRecord> read = SolaceIO.read(testJcsmpProperties, testQueues,
				SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper());

		assertEquals(testJcsmpProperties.toString(), read.jcsmpProperties().toString());
		assertEquals(testQueues, read.queues());
		assertFalse(read.useSenderTimestamp());
		assertEquals(500, read.advanceTimeoutInMillis());
		assertEquals(Long.MAX_VALUE, read.maxNumRecords());
		assertNull(read.maxReadTime());
		assertEquals(SolaceTestRecord.getCoder(), read.coder());
		assertEquals(SolaceTestRecord.getMapper(), read.inboundMessageMapper());
	}

	@Test
	public void testNullJcsmpProperties() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(null, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()));
		assertThat(exception.getMessage(), containsString("jcsmpProperties"));
	}

	@Test
	public void testNullJcsmpPropertiesOverride() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withJcsmpProperties(null));
		assertThat(exception.getMessage(), containsString("jcsmpProperties"));
	}

	@ParameterizedTest
	@ValueSource(strings = {JCSMPProperties.HOST, JCSMPProperties.USERNAME, JCSMPProperties.PASSWORD, JCSMPProperties.VPN_NAME})
	public void testJcsmpPropertiesWithNullRequiredProperty(String jcsmpProperty) {
		testJcsmpProperties.setProperty(jcsmpProperty, null);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()));
		assertThat(exception.getMessage(), containsString(jcsmpProperty + " cannot be null"));
	}

	@Test
	public void testJcsmpPropertiesWithNonNullClientName() {
		testJcsmpProperties.setProperty(JCSMPProperties.CLIENT_NAME, "dummy");
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()));
		assertThat(exception.getMessage(), containsString(JCSMPProperties.CLIENT_NAME + " must be null"));
	}

	@Test
	public void testNullQueues() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, null, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()));
		assertThat(exception.getMessage(), containsString("queues"));
	}

	@Test
	public void testNullQueuesOverride() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, null, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withQueues(new ArrayList<>()));
		assertThat(exception.getMessage(), containsString("queues"));
	}

	@Test
	public void testEmptyQueues() {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, new ArrayList<>(), SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper()));
		assertThat(exception.getMessage(), containsString("queues cannot be null or empty"));
	}

	@Test
	public void testEmptyQueuesOverride() {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withQueues(new ArrayList<>()));
		assertThat(exception.getMessage(), containsString("queues cannot be null or empty"));
	}

	@Test
	public void testNullCoder() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, null, SolaceTestRecord.getMapper()));
		assertThat(exception.getMessage(), containsString("coder"));
	}

	@Test
	public void testNullCoderOverride() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withCoder(null));
		assertThat(exception.getMessage(), containsString("coder"));
	}

	@Test
	public void testNullInboundMessageMapper() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), null));
		assertThat(exception.getMessage(), containsString("inboundMessageMapper"));
	}

	@Test
	public void testNullInboundMessageMapperOverride() {
		NullPointerException exception = assertThrows(NullPointerException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withInboundMessageMapper(null));
		assertThat(exception.getMessage(), containsString("inboundMessageMapper"));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, -1})
	public void testInvalidAdvanceTimeoutInMillis(int advanceTimeout) {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withAdvanceTimeoutInMillis(advanceTimeout));
		assertThat(exception.getMessage(), containsString("advanceTimeoutInMillis must be greater than 0"));
	}

	@ParameterizedTest
	@ValueSource(longs = {0, -1})
	public void testInvalidMaxNumRecords(long maxNumRecords) {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withMaxNumRecords(maxNumRecords));
		assertThat(exception.getMessage(), containsString("maxNumRecords must be greater than 0"));
	}

	@ParameterizedTest
	@ValueSource(longs = {0, -1})
	public void testInvalidMaxReadTime(long maxReadTime) {
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
				SolaceIO.read(testJcsmpProperties, testQueues, SolaceTestRecord.getCoder(), SolaceTestRecord.getMapper())
						.withMaxReadTime(new Duration(maxReadTime)));
		assertThat(exception.getMessage(), containsString("maxReadTime must be greater than 0"));
	}
}
