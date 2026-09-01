package org.reactivecommons.async.starter.impl.common.kafka.apicurio;

import com.networknt.schema.JsonSchema;
import io.apicurio.registry.resolver.SchemaResolver;
import org.junit.jupiter.api.Test;
import org.reactivecommons.async.kafka.apicurio.ApicurioSchemaValidator;
import org.reactivecommons.async.kafka.config.KafkaProperties;
import org.reactivecommons.async.kafka.config.props.ApicurioValidationProperties;
import org.reactivecommons.async.kafka.config.props.AsyncKafkaPropsDomain;
import org.reactivecommons.async.kafka.config.props.AsyncKafkaPropsDomainProperties;
import org.reactivecommons.async.kafka.validation.DomainSchemaValidatorProvider;
import org.reactivecommons.async.kafka.validation.NoOpSchemaValidator;
import org.reactivecommons.async.kafka.validation.SchemaValidator;
import org.reactivecommons.async.starter.exceptions.InvalidConfigurationException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
class RCKafkaApicurioConfigTest {

    /**
     * Provides the domain properties the way the Kafka starter does, so the Apicurio configuration is bound from
     * {@code reactive.commons.kafka.<domain>.apicurio}.
     * <p>
     * Deliberately not annotated with {@code @Configuration}: this package is the one scanned by
     * {@code ReactiveCommonsConfig}, so the annotation would make it a candidate for component scan and its
     * {@code AsyncKafkaPropsDomain} would leak into every other test context. It is applied explicitly with
     * {@code withUserConfiguration}, which does not need the annotation.
     */
    @EnableConfigurationProperties(AsyncKafkaPropsDomainProperties.class)
    static class DomainPropertiesConfig {
        @Bean
        public AsyncKafkaPropsDomain asyncKafkaPropsDomain(AsyncKafkaPropsDomainProperties configured) {
            return new AsyncKafkaPropsDomain("test-app", new KafkaProperties(), configured,
                    (ignoredDomain, ignoredProps) -> {
                    });
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(DomainPropertiesConfig.class)
            .withConfiguration(AutoConfigurations.of(RCKafkaApicurioConfig.class))
            // find-latest defaults to false as in Apicurio, so every domain has to decide how the version is
            // resolved. Opting into the latest one is the shortest way to a usable configuration.
            .withPropertyValues(
                    "reactive.commons.kafka.app.apicurio.properties."
                            + "apicurio\\.registry\\.url=http://localhost:8080/apis/registry/v3",
                    "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.find-latest=true");

    @Test
    void shouldRegisterApicurioValidator() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(DomainSchemaValidatorProvider.class);
            assertThat(context.getBean(DomainSchemaValidatorProvider.class).forDomain("app"))
                    .isInstanceOf(ApicurioSchemaValidator.class);
        });
    }

    @Test
    void shouldNotValidateWhenTheDomainDisablesIt() {
        runner.withPropertyValues("reactive.commons.kafka.app.apicurio.properties."
                        + "apicurio\\.registry\\.serde\\.validation-enabled=false")
                .run(context -> assertThat(context.getBean(DomainSchemaValidatorProvider.class).forDomain("app"))
                        .isInstanceOf(NoOpSchemaValidator.class));
    }

    @Test
    void shouldNotCreateAPhantomDomainForTheApicurioConfiguration() {
        runner.run(context -> assertThat(context.getBean(AsyncKafkaPropsDomain.class)).containsOnlyKeys("app"));
    }

    @Test
    void shouldGiveEachDomainItsOwnValidator() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.url=http://accounts/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.find-latest=true",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=accounts")
                .run(context -> {
                    DomainSchemaValidatorProvider provider = context.getBean(DomainSchemaValidatorProvider.class);
                    assertThat(provider.forDomain("accounts"))
                            .isInstanceOf(ApicurioSchemaValidator.class)
                            .isNotSameAs(provider.forDomain("app"));
                });
    }

    @Test
    void shouldNotValidateDomainsThatAreNotConfigured() {
        runner.run(context -> assertThat(context.getBean(DomainSchemaValidatorProvider.class)
                .forDomain("undeclared")).isInstanceOf(NoOpSchemaValidator.class));
    }

    @Test
    void shouldLetASingleDomainDisableItsValidation() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.url=http://accounts/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.find-latest=true",
                        "reactive.commons.kafka.accounts.apicurio.properties."
                                + "apicurio\\.registry\\.serde\\.validation-enabled=false")
                .run(context -> {
                    DomainSchemaValidatorProvider provider = context.getBean(DomainSchemaValidatorProvider.class);
                    assertThat(provider.forDomain("accounts")).isInstanceOf(NoOpSchemaValidator.class);
                    assertThat(provider.forDomain("app")).isInstanceOf(ApicurioSchemaValidator.class);
                });
    }

    @Test
    void shouldReportTheDomainWhenItsConfigurationIsInvalid() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.accounts.apicurio.validate-outbound=false",
                        "reactive.commons.kafka.accounts.apicurio.validate-inbound=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("reactive.commons.kafka.accounts.apicurio.validate-outbound"));
    }

    @Test
    void shouldFailWhenBothDirectionsAreDisabled() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.validate-outbound=false",
                        "reactive.commons.kafka.app.apicurio.validate-inbound=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("apicurio.registry.serde.validation-enabled=false"));
    }

    @Test
    void shouldFailWhenTheApicurioHeadersAreDisabled() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.headers\\.enabled=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio.registry.headers.enabled is false")
                        .hasMessageContaining("set it to true"));
    }

    @Test
    void shouldAllowTheApicurioHeadersExplicitlyEnabled() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.headers\\.enabled=true")
                .run(context -> assertThat(context.getBean(DomainSchemaValidatorProvider.class).forDomain("app"))
                        .isInstanceOf(ApicurioSchemaValidator.class));
    }

    @Test
    void shouldReleaseTheRegistryClientsWhenTheContextIsClosed() {
        runner.run(context -> {
            DomainSchemaValidatorProvider provider = context.getBean(DomainSchemaValidatorProvider.class);
            // Spring infers close() as the destroy method of any AutoCloseable bean
            assertThat(provider).isInstanceOf(AutoCloseable.class);

            assertThatCode(((ConfigurableApplicationContext) context.getSourceApplicationContext())::close)
                    .doesNotThrowAnyException();
        });
    }

    @Test
    void shouldReleaseTheRegistryClientsItCreated() throws Exception {
        SchemaResolver<JsonSchema, Object> resolver = mock(SchemaResolver.class);
        var resolvers = new SharedSchemaResolvers(config -> resolver);
        resolvers.forRegistry(Map.of("apicurio.registry.url", "http://registry:8080"));

        RCKafkaApicurioConfig.ApicurioValidatorProvider provider =
                new RCKafkaApicurioConfig.ApicurioValidatorProvider(Map.of(), resolvers);
        provider.close();

        verify(resolver).close();
        assertThat(provider.forDomain("undeclared")).isInstanceOf(NoOpSchemaValidator.class);
    }

    @Test
    void shouldShareOneRegistryClientBetweenDomainsWithTheSameRegistry() {
        var resolvers = new SharedSchemaResolvers();

        RCKafkaApicurioConfig.buildValidator(
                registryProperties("http://registry:8080/apis/registry/v3", "app-group"), "app", resolvers);
        RCKafkaApicurioConfig.buildValidator(
                registryProperties("http://registry:8080/apis/registry/v3", "accounts-group"), "accounts", resolvers);

        // Same endpoint and credentials, so one connection and one schema cache serve both groups
        assertThat(resolvers.count()).isOne();
    }

    @Test
    void shouldShareOneRegistryClientEvenWhenTheDomainsUseDifferentBrokers() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.connection-properties.bootstrap-servers=broker-a:9092",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.url=http://registry:8080/apis/registry/v3",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=app",
                        "reactive.commons.kafka.accounts.connection-properties.bootstrap-servers=broker-b:9092",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.url=http://registry:8080/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.find-latest=true",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=ap")
                .run(context -> {
                    AsyncKafkaPropsDomain domains = context.getBean(AsyncKafkaPropsDomain.class);
                    assertThat(domains.getProps("app").getConnectionProperties().getBootstrapServers())
                            .isNotEqualTo(domains.getProps("accounts").getConnectionProperties()
                                    .getBootstrapServers());

                    SharedSchemaResolvers resolvers = new SharedSchemaResolvers();
                    Map<String, SchemaValidator> validators =
                            RCKafkaApicurioConfig.buildValidators(domains, resolvers);

                    // The broker is not part of the registry configuration, so a single client serves both domains
                    assertThat(resolvers.count()).isOne();
                    assertThat(validators).containsOnlyKeys("app", "accounts");
                    assertThat(validators.get("app")).isNotSameAs(validators.get("accounts"));
                });
    }

    @Test
    void shouldNotShareTheClientBetweenDifferentRegistries() {
        var resolvers = new SharedSchemaResolvers();

        RCKafkaApicurioConfig.buildValidator(
                registryProperties("http://registry-a:8080/apis/registry/v3", "group"), "app", resolvers);
        RCKafkaApicurioConfig.buildValidator(
                registryProperties("http://registry-b:8080/apis/registry/v3", "group"), "accounts", resolvers);

        assertThat(resolvers.count()).isEqualTo(2);
    }

    @Test
    void shouldNotShareTheClientBetweenDomainsWithDifferentCredentials() {
        var resolvers = new SharedSchemaResolvers();
        ApicurioValidationProperties app = registryProperties("http://registry:8080/apis/registry/v3", "group");
        app.getProperties().put("apicurio.registry.auth.client.id", "app-client");
        ApicurioValidationProperties accounts = registryProperties("http://registry:8080/apis/registry/v3", "group");
        accounts.getProperties().put("apicurio.registry.auth.client.id", "accounts-client");

        RCKafkaApicurioConfig.buildValidator(app, "app", resolvers);
        RCKafkaApicurioConfig.buildValidator(accounts, "accounts", resolvers);

        assertThat(resolvers.count()).isEqualTo(2);
    }

    private ApicurioValidationProperties registryProperties(String url, String groupId) {
        ApicurioValidationProperties properties = new ApicurioValidationProperties();
        Map<String, String> configured = new HashMap<>();
        configured.put("apicurio.registry.url", url);
        configured.put("apicurio.registry.artifact.group-id", groupId);
        configured.put("apicurio.registry.find-latest", "true");
        properties.setProperties(configured);
        return properties;
    }

    @Test
    void shouldIsolateTheRegistryConfigurationOfEachDomain() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=app-group",
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.auth\\.client\\.id=app-client",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.url=http://accounts/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.find-latest=true",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=accounts-group",
                        "reactive.commons.kafka.accounts.apicurio.validate-outbound=false",
                        "reactive.commons.kafka.accounts.apicurio.properties."
                                + "apicurio\\.registry\\.auth\\.client\\.id=accounts-client")
                .run(context -> {
                    AsyncKafkaPropsDomain domains = context.getBean(AsyncKafkaPropsDomain.class);
                    ApicurioValidationProperties app = domains.getProps("app").getApicurio();
                    ApicurioValidationProperties accounts = domains.getProps("accounts").getApicurio();

                    assertThat(app.getProperties())
                            .containsEntry("apicurio.registry.url", "http://localhost:8080/apis/registry/v3")
                            .containsEntry("apicurio.registry.artifact.group-id", "app-group")
                            .containsEntry("apicurio.registry.auth.client.id", "app-client");
                    assertThat(app.isValidateOutbound()).isTrue();

                    assertThat(accounts.getProperties())
                            .containsEntry("apicurio.registry.url", "http://accounts/apis/registry/v3")
                            .containsEntry("apicurio.registry.artifact.group-id", "accounts-group")
                            .containsEntry("apicurio.registry.auth.client.id", "accounts-client");
                    assertThat(accounts.isValidateOutbound()).isFalse();

                    DomainSchemaValidatorProvider provider = context.getBean(DomainSchemaValidatorProvider.class);
                    assertThat(provider.forDomain("app"))
                            .isInstanceOf(ApicurioSchemaValidator.class)
                            .isNotSameAs(provider.forDomain("accounts"));
                });
    }

    @Test
    void shouldSupportTwoBrokersSharingASingleRegistry() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.connection-properties.bootstrap-servers=broker-a:9092",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=app-group",
                        "reactive.commons.kafka.accounts.connection-properties.bootstrap-servers=broker-b:9092",
                        // The very same registry as the default domain
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.url=http://localhost:8080/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.find-latest=true",
                        "reactive.commons.kafka.accounts.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=accounts-group")
                .run(context -> {
                    AsyncKafkaPropsDomain domains = context.getBean(AsyncKafkaPropsDomain.class);
                    assertThat(domains.getProps("app").getApicurio().getProperties())
                            .containsEntry("apicurio.registry.url", "http://localhost:8080/apis/registry/v3");
                    assertThat(domains.getProps("accounts").getApicurio().getProperties())
                            .containsEntry("apicurio.registry.url", "http://localhost:8080/apis/registry/v3");

                    DomainSchemaValidatorProvider provider = context.getBean(DomainSchemaValidatorProvider.class);
                    // Each domain still gets its own validator, so it keeps its own group and credentials even
                    // though both resolve against the same registry
                    assertThat(provider.forDomain("app"))
                            .isInstanceOf(ApicurioSchemaValidator.class)
                            .isNotSameAs(provider.forDomain("accounts"));
                });
    }

    @Test
    void shouldLetACustomProviderShareOneValidatorAcrossDomains() {
        SchemaValidator shared = mock(SchemaValidator.class);

        runner.withBean(DomainSchemaValidatorProvider.class, () -> domain -> shared)
                .run(context -> {
                    DomainSchemaValidatorProvider provider = context.getBean(DomainSchemaValidatorProvider.class);
                    // A user provided bean replaces the one of the starter, which is how a single registry
                    // client and schema cache are shared by every domain
                    assertThat(provider.forDomain("app")).isSameAs(shared);
                    assertThat(provider.forDomain("accounts")).isSameAs(shared);
                });
    }

    @Test
    void shouldFailWhenTheVersionResolutionIsNotDecided() {
        // No find-latest and no version: the Apicurio default of find-latest is false, so nothing resolves
        new ApplicationContextRunner()
                .withUserConfiguration(DomainPropertiesConfig.class)
                .withConfiguration(AutoConfigurations.of(RCKafkaApicurioConfig.class))
                .withPropertyValues("reactive.commons.kafka.app.apicurio.properties."
                        + "apicurio\\.registry\\.url=http://localhost:8080/apis/registry/v3")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("No schema version could be resolved for domain app")
                        .hasMessageContaining("which is its default in Apicurio"));
    }

    @Test
    void shouldFailWhenNoSchemaVersionCanBeResolved() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.find-latest=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio.registry.find-latest is false")
                        .hasMessageContaining("apicurio.registry.artifact.version is empty")
                        .hasMessageContaining("apicurio.registry.find-latest=true"));
    }

    @Test
    void shouldFailWhenNoSchemaVersionCanBeResolvedOnAConsumerOnlyDomain() {
        // The direction does not matter: without a version there is no schema to validate against
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.find-latest=false",
                        "reactive.commons.kafka.app.apicurio.validate-outbound=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("No schema version could be resolved"));
    }

    @Test
    void shouldAllowDisablingTheLatestFallbackWhenTheVersionIsPinned() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.find-latest=false",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.version=1")
                .run(context -> assertThat(context.getBean(DomainSchemaValidatorProvider.class).forDomain("app"))
                        .isInstanceOf(ApicurioSchemaValidator.class));
    }

    @Test
    void shouldBindAllProperties() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.group-id=kafka",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.artifact-id=person",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.artifact\\.version=1",
                        "reactive.commons.kafka.app.apicurio.properties.apicurio\\.registry\\.find-latest=false",
                        "reactive.commons.kafka.app.apicurio.validate-outbound=false",
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.auth\\.client\\.id=id")
                .run(context -> {
                    var properties = context.getBean(AsyncKafkaPropsDomain.class).getProps("app").getApicurio();
                    assertThat(properties.getProperties())
                            .containsEntry("apicurio.registry.artifact.group-id", "kafka")
                            .containsEntry("apicurio.registry.artifact.artifact-id", "person")
                            .containsEntry("apicurio.registry.artifact.version", "1")
                            .containsEntry("apicurio.registry.find-latest", "false")
                            .containsEntry("apicurio.registry.auth.client.id", "id");
                    assertThat(properties.isValidateOutbound()).isFalse();
                    assertThat(properties.isValidateInbound()).isTrue();
                    assertThat(context).hasSingleBean(DomainSchemaValidatorProvider.class);
                });
    }
}
