package ai.superstream.agent;

import ai.superstream.util.SuperstreamLogger;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;

/**
 * Java agent entry point for the Superstream library.
 */
public class SuperstreamAgent {
    private static final SuperstreamLogger logger = SuperstreamLogger.getLogger(SuperstreamAgent.class);

    /**
     * Premain method, called when the agent is loaded during JVM startup.
     *
     * @param arguments Agent arguments
     * @param instrumentation Instrumentation instance
     */
    public static void premain(String arguments, Instrumentation instrumentation) {
        logger.info("Superstream Agent initialized");
        install(instrumentation);
    }

    /**
     * AgentMain method, called when the agent is loaded after JVM startup.
     *
     * @param arguments Agent arguments
     * @param instrumentation Instrumentation instance
     */
    public static void agentmain(String arguments, Instrumentation instrumentation) {
        logger.info("Superstream Agent initialized (dynamic attach)");
        install(instrumentation);
    }

    /**
     * Install the agent instrumentation.
     *
     * @param instrumentation Instrumentation instance
     */
    private static void install(Instrumentation instrumentation) {
        // Intercept KafkaProducer constructor
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .type(ElementMatchers.named("org.apache.kafka.clients.producer.KafkaProducer"))
                .transform((builder, typeDescription, classLoader, module, protectionDomain) ->
                        builder.visit(Advice.to(KafkaProducerInterceptor.class)
                                .on(ElementMatchers.isConstructor())))
                .installOn(instrumentation);

        logger.info("Superstream Agent successfully installed instrumentation");
    }
}