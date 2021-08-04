package com.solace.connector.beam.examples.test.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * <p>Junit 5 extension to auto-create and delete executor services.</p>
 * <p>Can be accessed with test parameters:</p>
 * <pre><code>
 *  static ExecutorServiceExtension executorExtension = new ExecutorServiceExtension(Executors::newCachedThreadPool);
 *
 * {@literal @}Test
 *  public void testMethod(ExecutorService executorService) { // param type can be any subclass of ExecutorService
 *		// Test logic using executor service
 *  }
 * </code></pre>
 */
public class ExecutorServiceExtension implements AfterEachCallback, ParameterResolver {
	private final Supplier<ExecutorService> executorSupplier;

	private static final Namespace NAMESPACE = Namespace.create(ExecutorServiceExtension.class);
	private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceExtension.class);

	public ExecutorServiceExtension(Supplier<ExecutorService> executorSupplier) {
		this.executorSupplier = executorSupplier;
	}

	@Override
	public void afterEach(ExtensionContext context) {
		ExecutorService executorService = context.getStore(NAMESPACE).get(ExecutorService.class, ExecutorService.class);
		if (executorService != null) {
			LOG.info("Shutting down executor service");
			executorService.shutdownNow();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return ExecutorService.class.isAssignableFrom(parameterContext.getParameter().getType());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(ExecutorService.class,
				c -> executorSupplier.get(), ExecutorService.class);
	}
}
