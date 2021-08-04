package com.solace.connector.beam.examples.test.extension;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;

public class LogCaptorExtension implements AfterEachCallback, ParameterResolver {
	private final Logger logger;

	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(LogCaptorExtension.class);

	public LogCaptorExtension(Class<?> logClass) {
		this.logger = (Logger) LogManager.getLogger(logClass);
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		return BufferedReader.class.isAssignableFrom(parameterContext.getParameter().getType());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		Store store = extensionContext.getStore(NAMESPACE);
		try {
			PipedReader in = store.getOrComputeIfAbsent(getReaderKey(), c -> new PipedReader(), PipedReader.class);
			Appender appender = store.getOrComputeIfAbsent(getAppenderKey(), c ->
			{
				try {
					return WriterAppender.newBuilder()
							.setName(getAppenderKey())
							.setTarget(new PipedWriter(in))
							.setLayout(PatternLayout.createDefaultLayout())
							.build();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}, Appender.class);
			if (!appender.isStarted()) {
				appender.start();
			}
			logger.addAppender(appender);
			return store.getOrComputeIfAbsent(getBufferedReaderKey(), c -> new BufferedReader(in), BufferedReader.class);
		} catch (Exception e) {
			throw new ParameterResolutionException("Failed to resolve parameter" + parameterContext.getParameter().getName(), e);
		}
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {
		Store store = context.getStore(NAMESPACE);
		WriterAppender appender = store.get(getAppenderKey(), WriterAppender.class);
		if (appender != null) {
			logger.removeAppender(appender);
			appender.stop();
		}
		BufferedReader bufferedReader = store.get(getBufferedReaderKey(), BufferedReader.class);
		if (bufferedReader != null) {
			bufferedReader.close();
		}
	}

	private String getReaderKey() {
		return logger.getName() + "-reader";
	}

	private String getBufferedReaderKey() {
		return logger.getName() + "-buffered-reader";
	}

	private String getAppenderKey() {
		return logger.getName() + "-appender";
	}
}
