package com.solace.connector.beam.test.util;

import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<T> extends Consumer<T> {
	@Override
	default void accept(T t) {
		try {
			acceptThrows(t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	void acceptThrows(T t) throws Exception;
}
