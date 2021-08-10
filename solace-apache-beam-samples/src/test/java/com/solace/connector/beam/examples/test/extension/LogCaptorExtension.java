package com.solace.connector.beam.examples.test.extension;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class LogCaptorExtension implements AfterTestExecutionCallback, ParameterResolver {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogCaptorExtension.class);
	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(LogCaptorExtension.class);

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		return BufferedReader.class.isAssignableFrom(parameterContext.getParameter().getType())
				&& parameterContext.isAnnotated(LogCaptor.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
		Store store = extensionContext.getStore(NAMESPACE);

		Logger logger = store.getOrComputeIfAbsent(Logger.class, c -> (Logger) LogManager.getLogger(
				parameterContext.findAnnotation(LogCaptor.class)
						.map(LogCaptor::value)
						.orElseThrow(() -> new ParameterResolutionException(String.format("parameter %s is not annotated with %s",
								parameterContext.getParameter().getName(), LogCaptor.class)))),
				Logger.class);
		PipedReader in = store.getOrComputeIfAbsent(PipedReader.class, c -> new PipedReader(), PipedReader.class);
		Appender appender = store.getOrComputeIfAbsent(Appender.class, c ->
		{
			try {
				return WriterAppender.newBuilder()
						.setName(logger.getName() + "-LogCaptor")
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
		LOG.info("Adding appender {} to logger {}", appender.getName(), logger.getName());
		logger.addAppender(appender);
		return store.getOrComputeIfAbsent(BufferedReader.class, c -> new BufferedReader(in), BufferedReader.class);
	}

	@Override
	public void afterTestExecution(ExtensionContext context) throws Exception {
		Store store = context.getStore(NAMESPACE);

		Logger logger = store.get(Logger.class, Logger.class);
		Appender appender = store.get(Appender.class, Appender.class);

		if (logger != null && appender != null) {
			LOG.info("Removing appender {} from logger {}", appender.getName(), logger.getName());
			logger.removeAppender(appender);
		}

		if (appender != null) {
			appender.stop();
		}

		BufferedReader bufferedReader = store.get(BufferedReader.class, BufferedReader.class);
		if (bufferedReader != null) {
			bufferedReader.close();
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.PARAMETER)
	public @interface LogCaptor {
		Class<?> value();
	}
}
