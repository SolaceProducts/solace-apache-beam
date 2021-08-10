package com.solace.connector.beam.examples.test;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface SolaceTestOptions extends PipelineOptions {
	@Description("Host for Solace PubSub+ broker")
	@Default.String("tcp://localhost:55555")
	String getPspHost();
	void setPspHost(String value);

	@Description("VPN name for Solace PubSub+ broker")
	@Default.String("default")
	String getPspVpnName();
	void setPspVpnName(String value);

	@Description("Client username for Solace PubSub+ broker")
	@Default.String("default")
	String getPspUsername();
	void setPspUsername(String value);

	@Description("Client password for Solace PubSub+ broker")
	@Default.String("default")
	String getPspPassword();
	void setPspPassword(String value);

	@Description("Management host for Solace PubSub+ broker")
	@Default.String("http://localhost:8080")
	String getPspMgmtHost();
	void setPspMgmtHost(String value);

	@Description("Management username for Solace PubSub+ broker")
	@Default.String("admin")
	String getPspMgmtUsername();
	void setPspMgmtUsername(String value);

	@Description("Management password for Solace PubSub+ broker")
	@Default.String("admin")
	String getPspMgmtPassword();
	void setPspMgmtPassword(String value);

	@Description("Use Testcontainers to provision Solace PubSub+ broker")
	@Default.Boolean(true)
	boolean getUseTestcontainers();
	void setUseTestcontainers(boolean value);
}
