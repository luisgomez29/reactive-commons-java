package org.reactivecommons.async.kafka.apicurio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import io.apicurio.registry.resolver.DefaultSchemaResolver;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.kafka.headers.DefaultHeadersHandler;
import io.apicurio.registry.serde.kafka.headers.HeadersHandler;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds an {@link ApicurioSchemaValidator} from the very same configuration keys used by the
 * Apicurio Kafka serdes ({@link SerdeConfig}), so an existing configuration can be reused as is.
 * <p>
 * {@code apicurio.registry.find-latest} keeps its meaning: with no explicit version, the latest version of the
 * artifact is resolved. Disabling it without setting a version is rejected here, one step earlier than in Apicurio,
 * where the same combination ends up resolving the latest version anyway.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ApicurioSchemaValidatorFactory {

    /**
     * Lifetime of a cached schema. A registered version is immutable, so the only thing this period delays is
     * noticing that the latest version of an artifact has changed.
     */
    static final long CHECK_PERIOD_MS_DEFAULT = Duration.ofMinutes(30).toMillis();

    public static ApicurioSchemaValidator create(Map<String, Object> configs) {
        return create(configs, null);
    }

    public static ApicurioSchemaValidator create(Map<String, Object> configs, ObjectMapper objectMapper) {
        return create(configs, objectMapper, null, null);
    }

    public static ApicurioSchemaValidator create(Map<String, Object> configs, ObjectMapper objectMapper,
                                                 Boolean validateOutbound, Boolean validateInbound) {
        Map<String, Object> resolved = prepare(configs);
        return build(newResolver(resolved), true, resolved, objectMapper, validateOutbound, validateInbound);
    }

    /**
     * Builds a validator on top of a resolver created with {@link #createResolver(Map)}.
     * <p>
     * The resolver holds the registry client and the schema cache, which only depend on the registry itself, while
     * the artifact coordinates and the directions to validate are read from {@code configs} for this validator
     * alone. That is what lets several domains resolving against the same registry share one connection and one
     * cache while keeping their own group, artifact and switches: the cache is indexed by the full coordinates,
     * so the entries of one group never collide with those of another.
     * <p>
     * The resolver is <b>not</b> owned by the returned validator, so closing it stays the responsibility of
     * whoever created it.
     */
    public static ApicurioSchemaValidator create(SchemaResolver<JsonSchema, Object> schemaResolver,
                                                 Map<String, Object> configs, ObjectMapper objectMapper,
                                                 Boolean validateOutbound, Boolean validateInbound) {
        return build(schemaResolver, false, prepare(configs), objectMapper, validateOutbound, validateInbound);
    }

    /**
     * Creates the registry client and the schema cache shared by every validator of the same registry.
     * <p>
     * Only the registry level keys are read: endpoint, credentials, TLS and cache tuning. The artifact
     * coordinates are ignored here because they are resolved per record by the validator.
     *
     * @return a resolver the caller owns, and must close when it is no longer used
     */
    public static SchemaResolver<JsonSchema, Object> createResolver(Map<String, Object> configs) {
        return newResolver(prepare(configs));
    }

    private static Map<String, Object> prepare(Map<String, Object> configs) {
        Map<String, Object> resolved = new HashMap<>(configs);
        assertHeadersAreEnabled(resolved);
        applyResolverDefaults(resolved);
        return resolved;
    }

    private static SchemaResolver<JsonSchema, Object> newResolver(Map<String, Object> resolved) {
        SchemaResolver<JsonSchema, Object> schemaResolver = new DefaultSchemaResolver<>();
        schemaResolver.configure(resolved, new RestrictedJsonSchemaParser<>());
        return schemaResolver;
    }

    private static ApicurioSchemaValidator build(SchemaResolver<JsonSchema, Object> schemaResolver,
                                                 boolean ownsResolver, Map<String, Object> resolved,
                                                 ObjectMapper objectMapper, Boolean validateOutbound,
                                                 Boolean validateInbound) {
        // Checked here and not while preparing the resolver: the artifact coordinates belong to the validator, a
        // shared resolver is built without them
        assertVersionIsResolvable(resolved);

        HeadersHandler headersHandler = new DefaultHeadersHandler();
        headersHandler.configure(resolved, false);

        boolean validationEnabled = booleanValue(resolved, SerdeConfig.VALIDATION_ENABLED,
                SerdeConfig.VALIDATION_ENABLED_DEFAULT);
        if (!validationEnabled) {
            throw new IllegalArgumentException(SerdeConfig.VALIDATION_ENABLED + " is false, so this validator would "
                    + "connect to the Apicurio Registry and resolve schemas without validating any message. Remove "
                    + "that property to keep validating, or do not create the validator at all.");
        }

        return ApicurioSchemaValidator.builder()
                .schemaResolver(schemaResolver)
                .ownsResolver(ownsResolver)
                .headersHandler(headersHandler)
                .artifactReferenceProvider(new DefaultArtifactReferenceProvider(
                        stringValue(resolved, SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID),
                        stringValue(resolved, SchemaResolverConfig.EXPLICIT_ARTIFACT_ID),
                        stringValue(resolved, SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION)))
                .objectMapper(objectMapper)
                .validateOutbound(validateOutbound)
                .validateInbound(validateInbound)
                .build();
    }

    /**
     * Rejects a configuration that leaves the schema version to chance.
     * <p>
     * {@code apicurio.registry.find-latest} keeps its Apicurio default, {@code false}, so the version has to be
     * decided explicitly. Apicurio would accept the combination but not honour it: with a JSON Schema its resolver
     * cannot derive the schema from the record, so it falls through to resolving the artifact by coordinates and,
     * with no version, obtains the latest one anyway.
     */
    private static void assertVersionIsResolvable(Map<String, Object> resolved) {
        boolean findLatest = booleanValue(resolved, SchemaResolverConfig.FIND_LATEST_ARTIFACT,
                SchemaResolverConfig.FIND_LATEST_ARTIFACT_DEFAULT);
        if (!findLatest && !isSet(stringValue(resolved, SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION))) {
            throw new IllegalArgumentException("No schema version could be resolved: "
                    + SchemaResolverConfig.EXPLICIT_ARTIFACT_VERSION + " is not set and "
                    + SchemaResolverConfig.FIND_LATEST_ARTIFACT + " is false, which is its default. Set the version, "
                    + "or set " + SchemaResolverConfig.FIND_LATEST_ARTIFACT + "=true to resolve the latest one.");
        }
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * Adjusts the defaults of the schema cache, which are meant for a serde and not for a reactive pipeline.
     * <p>
     * Resolving a schema is a <b>blocking</b> HTTP call issued from the thread that publishes or consumes the
     * record, and the Apicurio client does not apply any request timeout, so both how often the cache expires and
     * what happens when the registry is unreachable are relevant here:
     * <ul>
     *     <li>{@code check-period-ms} defaults to 30 seconds, which brings that blocking call back into the hot
     *     path twice a minute per artifact. A registered version is immutable, so a much longer period is used
     *     and only noticing a new latest version is delayed by it.</li>
     *     <li>{@code fault-tolerant-refresh} defaults to false, so a registry that blinks while an entry is being
     *     refreshed fails the message, even though a perfectly usable schema was already cached.</li>
     * </ul>
     * Both remain overridable through {@code properties}.
     */
    static void applyResolverDefaults(Map<String, Object> configs) {
        configs.putIfAbsent(SchemaResolverConfig.CHECK_PERIOD_MS, CHECK_PERIOD_MS_DEFAULT);
        configs.putIfAbsent(SchemaResolverConfig.FAULT_TOLERANT_REFRESH, true);
    }

    /**
     * Rejects an explicit {@code apicurio.registry.headers.enabled=false}.
     * <p>
     * Reactive Commons always writes the schema coordinates in the record headers, so that value would describe
     * a behavior this validator cannot honour. Apicurio 3.x defaults the property to {@code false}, so it is
     * pinned to {@code true} to keep the 2.x behavior when it is not set.
     */
    private static void assertHeadersAreEnabled(Map<String, Object> configs) {
        if (!booleanValue(configs, KafkaSerdeConfig.ENABLE_HEADERS, true)) {
            throw new IllegalArgumentException(KafkaSerdeConfig.ENABLE_HEADERS + " is false, but Reactive Commons "
                    + "always writes the schema coordinates in the record headers: they are the only channel it has "
                    + "to tell the consumer which schema version a record was published with. Remove that property "
                    + "or set " + KafkaSerdeConfig.ENABLE_HEADERS + "=true.");
        }
        configs.put(KafkaSerdeConfig.ENABLE_HEADERS, true);
    }

    private static String stringValue(Map<String, Object> configs, String key) {
        Object value = configs.get(key);
        return value == null ? null : value.toString();
    }

    private static boolean booleanValue(Map<String, Object> configs, String key, boolean defaultValue) {
        Object value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value instanceof Boolean bool ? bool : Boolean.parseBoolean(value.toString());
    }
}
