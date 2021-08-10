package com.solace.connector.beam.examples.test.extension;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>Junit 5 extension to auto-create and delete executor services.</p>
 * <p>Can be accessed with test parameters:</p>
 * <pre><code>
 *	{@literal @}ExtendWith(ExecutorServiceExtension.class)
 *	public class Test {
 *		{@literal @}Test
 *		public void testMethod({@literal @}ExecSvc ExecutorService executorService) { // param type can be any subclass of ExecutorService
 *			// Test logic using executor service
 *  	}
 *  }
 * </code></pre>
 */
public class ExecutorServiceExtension implements AfterTestExecutionCallback, ParameterResolver {
	private static final Namespace NAMESPACE = Namespace.create(ExecutorServiceExtension.class);
	private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceExtension.class);

	@Override
	public void afterTestExecution(ExtensionContext context) throws Exception {
		ExecutorService executorService = context.getStore(NAMESPACE).get(ExecutorService.class, ExecutorService.class);
		if (executorService != null) {
			LOG.info("Shutting down executor service");
			executorService.shutdownNow();
			if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
				LOG.error("Could not shutdown executor");
			}
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return ExecutorService.class.isAssignableFrom(parameterContext.getParameter().getType()) &&
				parameterContext.isAnnotated(ExecSvc.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		ExecSvc config = parameterContext.findAnnotation(ExecSvc.class).orElseThrow(() ->
				new ParameterResolutionException(String.format("parameter %s is not annotated with %s",
						parameterContext.getParameter().getName(), ExecSvc.class)));

		return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(ExecutorService.class,
				c -> {
					int poolSize = config.poolSize();
					if (config.scheduled()) {
						if (poolSize < 1) {
							throw new ParameterResolutionException(
									"Pool size must be > 1 for scheduled executor services");
						}
						LOG.info("Creating scheduled thread pool with core pool size {}", poolSize);
						return Executors.newScheduledThreadPool(poolSize);
					} else if (poolSize < 1) {
						LOG.info("Creating cached thread pool");
						return Executors.newCachedThreadPool();
					} else {
						LOG.info("Creating fixed thread pool of size {}", poolSize);
						return Executors.newFixedThreadPool(poolSize);
					}
				}, ExecutorService.class);
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.PARAMETER)
	public @interface ExecSvc {
		int poolSize() default 0;
		boolean scheduled() default false;
	}
}
