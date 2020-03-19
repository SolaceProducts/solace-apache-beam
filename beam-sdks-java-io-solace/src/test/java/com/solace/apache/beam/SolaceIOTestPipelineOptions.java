package com.solace.apache.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

public interface SolaceIOTestPipelineOptions extends TestPipelineOptions {

	@Description("Host name for Solace PubSub+ broker")
	@Default.String("localhost")
	String getSolaceHostName();
	void setSolaceHostName(String value);

	@Description("VPN name for Solace PubSub+ broker")
	@Default.String("default")
	String getSolaceVpnName();
	void setSolaceVpnName(String value);

	@Description("SMF port for Solace PubSub+ broker")
	@Default.Integer(55555)
	Integer getSolaceSmfPort();
	void setSolaceSmfPort(Integer value);

	@Description("Client username for Solace PubSub+ broker")
	String getSolaceUsername();
	void setSolaceUsername(String value);

	@Description("Client password for Solace PubSub+ broker")
	String getSolacePassword();
	void setSolacePassword(String value);

	@Description("SEMP port for Solace PubSub+ broker")
	@Default.Integer(943)
	Integer getSolaceMgmtPort();
	void setSolaceMgmtPort(Integer value);

	@Description("Management username for Solace PubSub+ broker")
	String getSolaceMgmtUsername();
	void setSolaceMgmtUsername(String value);

	@Description("Management password for Solace PubSub+ broker")
	String getSolaceMgmtPassword();
	void setSolaceMgmtPassword(String value);
}
