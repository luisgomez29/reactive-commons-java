package org.reactivecommons.async.kafka.apicurio;

import com.networknt.schema.JsonSchema;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.reactivecommons.async.kafka.validation.SchemaValidationException;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
class ApicurioSchemaValidatorFactoryTest {

    private static final byte[] PAYLOAD = "{}".getBytes(StandardCharsets.UTF_8);

    private Map<String, Object> baseConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SchemaResolverConfig.REGISTRY_URL, "http://localhost:8080/apis/registry/v2");
        return configs;
    }

    @Test
    void shouldBuildValidatorFromApicurioSerdeConfiguration() {
        assertThat(ApicurioSchemaValidatorFactory.create(baseConfig())).isNotNull();
    }

    @Test
    void shouldRejectAValidatorDisabledByTheApicurioConfiguration() {
        Map<String, Object> configs = baseConfig();
        configs.put(SerdeConfig.VALIDATION_ENABLED, "false");

        assertThatThrownBy(() -> ApicurioSchemaValidatorFactory.create(configs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("apicurio.registry.serde.validation-enabled is false");
    }

    @Test
    void shouldRejectAValidatorWithTheHeadersDisabled() {
        Map<String, Object> configs = baseConfig();
        configs.put(KafkaSerdeConfig.ENABLE_HEADERS, "false");

        assertThatThrownBy(() -> ApicurioSchemaValidatorFactory.create(configs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("apicurio.registry.headers.enabled is false");
    }

    @Test
    void shouldAcceptTheHeadersExplicitlyEnabled() {
        Map<String, Object> configs = baseConfig();
        configs.put(KafkaSerdeConfig.ENABLE_HEADERS, "true");

        assertThat(ApicurioSchemaValidatorFactory.create(configs)).isNotNull();
    }

    @Test
    void shouldRejectAValidatorWithBothDirectionsDisabled() {
        Map<String, Object> configs = baseConfig();

        assertThatThrownBy(() -> ApicurioSchemaValidatorFactory.create(configs, null, false, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must validate at least one direction");
    }

    @Test
    void shouldAllowDisablingEachDirectionIndependently() {
        assertThat(ApicurioSchemaValidatorFactory.create(baseConfig(), null, false, true)).isNotNull();
        assertThat(ApicurioSchemaValidatorFactory.create(baseConfig(), null, true, false)).isNotNull();
    }

    @Test
    void shouldKeepTheCachedSchemasLongerThanTheApicurioDefault() {
        Map<String, Object> configs = baseConfig();

        ApicurioSchemaValidatorFactory.applyResolverDefaults(configs);

        // Resolving is a blocking call on the thread handling the record, so the 30s default of the serdes would
        // bring it back into the hot path twice a minute per artifact
        assertThat(ApicurioSchemaValidatorFactory.CHECK_PERIOD_MS_DEFAULT)
                .isGreaterThan(SchemaResolverConfig.CHECK_PERIOD_MS_DEFAULT);
        assertThat(configs).containsEntry(SchemaResolverConfig.CHECK_PERIOD_MS,
                ApicurioSchemaValidatorFactory.CHECK_PERIOD_MS_DEFAULT);
    }

    @Test
    void shouldServeTheCachedSchemaWhenTheRegistryFailsToRefreshIt() {
        Map<String, Object> configs = baseConfig();

        ApicurioSchemaValidatorFactory.applyResolverDefaults(configs);

        assertThat(configs).containsEntry(SchemaResolverConfig.FAULT_TOLERANT_REFRESH, true);
    }

    @Test
    void shouldLetTheApplicationOverrideTheCacheDefaults() {
        Map<String, Object> configs = baseConfig();
        configs.put(SchemaResolverConfig.CHECK_PERIOD_MS, 1000L);
        configs.put(SchemaResolverConfig.FAULT_TOLERANT_REFRESH, false);

        ApicurioSchemaValidatorFactory.applyResolverDefaults(configs);

        assertThat(configs)
                .containsEntry(SchemaResolverConfig.CHECK_PERIOD_MS, 1000L)
                .containsEntry(SchemaResolverConfig.FAULT_TOLERANT_REFRESH, false);
        assertThat(ApicurioSchemaValidatorFactory.create(configs)).isNotNull();
    }

    @Test
    void shouldBuildAResolverThatCanBeSharedBySeveralValidators() {
        SchemaResolver<JsonSchema, Object> shared = mock(SchemaResolver.class);

        Map<String, Object> app = baseConfig();
        app.put(SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID, "app-group");
        Map<String, Object> accounts = baseConfig();
        accounts.put(SchemaResolverConfig.EXPLICIT_ARTIFACT_GROUP_ID, "accounts-group");

        ApicurioSchemaValidator first = ApicurioSchemaValidatorFactory
                .create(shared, app, null, true, true, false);
        ApicurioSchemaValidator second = ApicurioSchemaValidatorFactory
                .create(shared, accounts, null, true, true, false);

        // Nothing is resolved because the shared resolver is a stub, the point is which coordinates it is asked
        Headers headers = new RecordHeaders();
        assertThatThrownBy(() -> first.validateInbound("event.push", PAYLOAD, headers))
                .isInstanceOf(SchemaValidationException.class);
        assertThatThrownBy(() -> second.validateInbound("event.push", PAYLOAD, headers))
                .isInstanceOf(SchemaValidationException.class);

        // One resolver, one connection and one cache, while each validator keeps its own group
        verify(shared).resolveSchemaByArtifactReference(argThat(ref -> "app-group".equals(ref.getGroupId())));
        verify(shared).resolveSchemaByArtifactReference(argThat(ref -> "accounts-group".equals(ref.getGroupId())));
    }

    @Test
    void shouldNotLetASharedResolverBeClosedByItsValidators() throws Exception {
        SchemaResolver<JsonSchema, Object> shared = mock(SchemaResolver.class);
        ApicurioSchemaValidator validator = ApicurioSchemaValidatorFactory
                .create(shared, baseConfig(), null, true, true, false);

        validator.close();

        // The resolver stays usable for the other domains sharing it
        verify(shared, never()).close();
    }
}
