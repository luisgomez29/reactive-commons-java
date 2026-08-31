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
        SharedSchemaResolvers resolvers = new SharedSchemaResolvers();
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

        // Everything the registry client and the schema cache depend on: endpoint, credentials, TLS and tuning
        Map<String, Object> registryConfig = new HashMap<>(properties.getProperties());
        putIfPresent(registryConfig, SchemaResolverConfig.REGISTRY_URL, properties.getUrl());

        // The artifact coordinates are resolved per record, so they belong to this domain only
        Map<String, Object> configs = new HashMap<>(registryConfig);
        putIfPresent(configs, SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID, properties.getGroupId());
        putIfPresent(configs, SchemaResolverConfig.EXPLICIT_ARTIFACT_ID, properties.getArtifactId());
        putIfPresent(configs, SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION, properties.getVersion());
        configs.putIfAbsent(SchemaResolverConfig.FIND_LATEST_ARTIFACT, properties.isFindLatest());

        return ApicurioSchemaValidatorFactory.create(resolvers.forRegistry(registryConfig), configs, null,
                properties.isValidateOutbound(), properties.isValidateInbound(),
                properties.isTrustInboundCoordinates());
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

    private static void putIfPresent(Map<String, Object> configs, String key, String value) {
        if (value != null && !value.isBlank()) {
            configs.put(key, value);
        }
    }
}
