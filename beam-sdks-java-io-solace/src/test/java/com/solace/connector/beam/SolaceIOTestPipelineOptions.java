package com.solace.connector.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

public interface SolaceIOTestPipelineOptions extends TestPipelineOptions {

	@Description("Host for Solace PubSub+ broker")
	@Default.String("tcp://localhost:55555")
	String getSolaceHost();
	void setSolaceHost(String value);

	@Description("VPN name for Solace PubSub+ broker")
	@Default.String("default")
	String getSolaceVpnName();
	void setSolaceVpnName(String value);

	@Description("Client username for Solace PubSub+ broker")
	@Default.String("default")
	String getSolaceUsername();
	void setSolaceUsername(String value);

	@Description("Client password for Solace PubSub+ broker")
	@Default.String("default")
	String getSolacePassword();
	void setSolacePassword(String value);

	@Description("Management host for Solace PubSub+ broker")
	@Default.String("http://localhost:8080")
	String getSolaceMgmtHost();
	void setSolaceMgmtHost(String value);

	@Description("Management username for Solace PubSub+ broker")
	@Default.String("admin")
	String getSolaceMgmtUsername();
	void setSolaceMgmtUsername(String value);

	@Description("Management password for Solace PubSub+ broker")
	@Default.String("admin")
	String getSolaceMgmtPassword();
	void setSolaceMgmtPassword(String value);
}
