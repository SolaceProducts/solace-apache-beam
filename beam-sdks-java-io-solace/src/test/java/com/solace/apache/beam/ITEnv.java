package com.solace.apache.beam;

import java.util.Optional;

public final class ITEnv {
	public enum Test implements EnvProperty {
		RUNNER("TEST_RUNNER");

		private String name;
		Test(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return name;
		}
	}

	public enum Dataflow implements EnvProperty {
		PROJECT("DATAFLOW_PROJECT"),
		TMP_ROOT("DATAFLOW_TEMP_ROOT");

		private String name;
		Dataflow(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return name;
		}
	}

	public enum Solace implements EnvProperty {
		HOST("SOLACE_HOST"),
		VPN("SOLACE_VPN_NAME"),
		SMF_PORT("SOLACE_SMF_PORT"),
		USERNAME("SOLACE_USERNAME"),
		PASSWORD("SOLACE_PASSWORD"),
		MGMT_PORT("SOLACE_MGMT_PORT"),
		MGMT_USERNAME("SOLACE_MGMT_USERNAME"),
		MGMT_PASSWORD("SOLACE_MGMT_PASSWORD");

		private String name;
		Solace(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return name;
		}
	}

	private interface EnvProperty {
		default String get() {
			return get(null);
		}

		default String get(String defaultValue) {
			return Optional.ofNullable(System.getenv(getName())).orElse(defaultValue);
		}

		default boolean isPresent() {
			return Optional.ofNullable(System.getenv(getName())).isPresent();
		}

		String getName();
	}
}
