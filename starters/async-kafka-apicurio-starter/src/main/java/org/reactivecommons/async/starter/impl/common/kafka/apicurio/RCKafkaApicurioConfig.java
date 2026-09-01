package org.reactivecommons.async.starter.impl.common.kafka.apicurio;

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import org.reactivecommons.async.kafka.apicurio.ApicurioSchemaValidator;
import org.reactivecommons.async.kafka.apicurio.ApicurioSchemaValidatorFactory;
import org.reactivecommons.async.kafka.config.props.ApicurioValidationProperties;
import org.reactivecommons.async.kafka.config.props.AsyncKafkaPropsDomain;
import org.reactivecommons.async.kafka.validation.DomainSchemaValidatorProvider;
import org.reactivecommons.async.kafka.validation.NoOpSchemaValidator;
import org.reactivecommons.async.kafka.validation.SchemaValidator;
import org.reactivecommons.async.starter.exceptions.InvalidConfigurationException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registers the {@link DomainSchemaValidatorProvider} that supplies the Apicurio {@link SchemaValidator} of each
 * Reactive Commons domain.
 * <p>
 * It lives under {@code org.reactivecommons.async.starter.impl.common}, which is the package scanned
 * by {@code ReactiveCommonsConfig}, so adding this starter as a dependency is enough to enable it.
 * <p>
 * The configuration of every domain is read from {@code reactive.commons.kafka.<domain>.apicurio}, either from the
 * configuration files or from a {@code KafkaPropsCustomizer} bean.
 */
@Configuration
public class RCKafkaApicurioConfig {

    @Bean
    @ConditionalOnMissingBean({SchemaValidator.class, DomainSchemaValidatorProvider.class})
    public DomainSchemaValidatorProvider apicurioSchemaValidatorProvider(AsyncKafkaPropsDomain propsDomain) {
        var resolvers = new SharedSchemaResolvers();
        return new ApicurioValidatorProvider(buildValidators(propsDomain, resolvers), resolvers);
    }

    /**
     * Builds the validator of every configured domain, reusing the resolver of the domains that share a registry.
     * <p>
     * They are built eagerly so that an invalid configuration fails at startup instead of when the first message
     * of that domain is handled.
     */
    static Map<String, SchemaValidator> buildValidators(AsyncKafkaPropsDomain propsDomain,
                                                        SharedSchemaResolvers resolvers) {
        Map<String, SchemaValidator> validators = new HashMap<>();
        propsDomain.forEach((domain, props) ->
                validators.put(domain, createValidator(props.getApicurio(), domain, resolvers)));
        return validators;
    }

    /**
     * Holds the validator of every domain and releases the registry clients they share when the context is
     * disposed. Spring infers {@code close} as the destroy method of any {@link AutoCloseable} bean.
     */
    static class ApicurioValidatorProvider implements DomainSchemaValidatorProvider, AutoCloseable {

        private final Map<String, SchemaValidator> byDomain;
        private final SharedSchemaResolvers resolvers;

        ApicurioValidatorProvider(Map<String, SchemaValidator> byDomain, SharedSchemaResolvers resolvers) {
            this.byDomain = Map.copyOf(byDomain);
            this.resolvers = resolvers;
        }

        @Override
        public SchemaValidator forDomain(String domain) {
            return byDomain.getOrDefault(domain, NoOpSchemaValidator.INSTANCE);
        }

        @Override
        public void close() {
            resolvers.close();
        }
    }

    private static SchemaValidator createValidator(ApicurioValidationProperties properties, String domain,
                                                   SharedSchemaResolvers resolvers) {
        if (properties == null || !properties.isEnabled()) {
            return NoOpSchemaValidator.INSTANCE;
        }
        return buildValidator(properties, domain, resolvers);
    }

    static ApicurioSchemaValidator buildValidator(ApicurioValidationProperties properties, String domain,
                                                  SharedSchemaResolvers resolvers) {
        assertValidationIsUseful(properties, domain);
        assertHeadersAreEnabled(properties, domain);
        assertVersionIsResolvable(properties, domain);

        Map<String, Object> configs = new HashMap<>(properties.getProperties());
        return ApicurioSchemaValidatorFactory.create(resolvers.forRegistry(registryConfig(configs)), configs, null,
                properties.isValidateOutbound(), properties.isValidateInbound());
    }

    /**
     * Keys that select which artifact of the registry is validated, as opposed to which registry is contacted.
     * They are resolved per record, so two domains differing only in these still share one connection and cache.
     */
    private static final Set<String> ARTIFACT_KEYS = Set.of(
            SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID,
            SchemaResolverConfig.EXPLICIT_ARTIFACT_ID,
            SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION,
            SchemaResolverConfig.FIND_LATEST_ARTIFACT);

    /**
     * @return everything the registry client and the schema cache depend on: endpoint, credentials, TLS and tuning
     */
    private static Map<String, Object> registryConfig(Map<String, Object> configs) {
        Map<String, Object> registryConfig = new HashMap<>(configs);
        registryConfig.keySet().removeAll(ARTIFACT_KEYS);
        return registryConfig;
    }

    private static void assertValidationIsUseful(ApicurioValidationProperties properties, String domain) {
        String prefix = "reactive.commons.kafka." + domain + ".apicurio";
        if ("false".equalsIgnoreCase(properties.getProperties().get(SerdeConfig.VALIDATION_ENABLED))) {
            throw new InvalidConfigurationException("Conflicting configuration: " + prefix
                    + ".enabled is true but " + SerdeConfig.VALIDATION_ENABLED + " is false. Both switches turn "
                    + "schema validation on and off, so they must hold the same value. Remove "
                    + SerdeConfig.VALIDATION_ENABLED + " to keep validating, or set " + prefix + ".enabled=false "
                    + "to disable the feature without connecting to the registry.");
        }
        if (!properties.isValidateOutbound() && !properties.isValidateInbound()) {
            throw new InvalidConfigurationException("Both " + prefix + ".validate-outbound and " + prefix
                    + ".validate-inbound are false, so Reactive Commons would connect to the Apicurio Registry and "
                    + "resolve schemas without validating any message. Enable at least one direction, or turn the "
                    + "feature off with " + prefix + ".enabled=false.");
        }
    }

    private static void assertHeadersAreEnabled(ApicurioValidationProperties properties, String domain) {
        String value = properties.getProperties().get(KafkaSerdeConfig.ENABLE_HEADERS);
        if (value != null && !Boolean.parseBoolean(value)) {
            throw new InvalidConfigurationException("reactive.commons.kafka." + domain + ".apicurio.properties."
                    + KafkaSerdeConfig.ENABLE_HEADERS + " is " + value + ", but Reactive Commons always writes the "
                    + "schema coordinates in the record headers: they are the only channel it has to tell the "
                    + "consumer which schema version a record was published with. Remove that property or set it "
                    + "to true.");
        }
    }

    /**
     * Rejects a domain that leaves the schema version to chance.
     * <p>
     * {@code find-latest} keeps the Apicurio default, {@code false}, so the version has to be decided explicitly:
     * either by pinning {@code apicurio.registry.artifact.version} or by opting into the latest one. Apicurio would
     * accept the combination but not honour it: with a JSON Schema its resolver cannot derive the schema from the
     * record, so it falls through to resolving the artifact by coordinates and, with no version, obtains the latest
     * one anyway. Failing is better than silently doing what the flag says it does not.
     */
    private static void assertVersionIsResolvable(ApicurioValidationProperties properties, String domain) {
        String prefix = "reactive.commons.kafka." + domain + ".apicurio.properties.";
        Map<String, String> configured = properties.getProperties();
        boolean findLatest = Boolean.parseBoolean(configured.get(SchemaResolverConfig.FIND_LATEST_ARTIFACT));
        String version = configured.get(SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION);
        if (!findLatest && (version == null || version.isBlank())) {
            throw new InvalidConfigurationException("No schema version could be resolved for domain " + domain
                    + ": " + prefix + SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION + " is empty and " + prefix
                    + SchemaResolverConfig.FIND_LATEST_ARTIFACT + " is false, which is its default in Apicurio. Set "
                    + prefix + SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION + " to pin the topic to a single "
                    + "version, or set " + prefix + SchemaResolverConfig.FIND_LATEST_ARTIFACT + "=true to validate "
                    + "against the latest one.");
        }
    }
}
