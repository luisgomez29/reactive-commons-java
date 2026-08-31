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
            .withPropertyValues("reactive.commons.kafka.app.apicurio.url=http://localhost:8080/apis/registry/v3");

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
        runner.withPropertyValues("reactive.commons.kafka.app.apicurio.enabled=false")
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
                        "reactive.commons.kafka.accounts.apicurio.url=http://accounts/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.group-id=accounts")
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
                        "reactive.commons.kafka.accounts.apicurio.url=http://accounts/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.enabled=false")
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
                        .hasMessageContaining("enabled=false"));
    }

    @Test
    void shouldFailWhenTheApicurioValidationSwitchContradictsEnabled() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.serde\\.validation-enabled=false")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("Conflicting configuration")
                        .hasMessageContaining("must hold the same value"));
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
    void shouldNotTrustTheCoordinatesOfIncomingRecordsByDefault() {
        runner.run(context -> assertThat(context.getBean(AsyncKafkaPropsDomain.class).getProps("app").getApicurio()
                .isTrustInboundCoordinates()).isFalse());
    }

    @Test
    void shouldLetADomainTrustTheCoordinatesOfIncomingRecords() {
        runner.withPropertyValues("reactive.commons.kafka.app.apicurio.trust-inbound-coordinates=true")
                .run(context -> {
                    assertThat(context.getBean(AsyncKafkaPropsDomain.class).getProps("app").getApicurio()
                            .isTrustInboundCoordinates()).isTrue();
                    assertThat(context.getBean(DomainSchemaValidatorProvider.class).forDomain("app"))
                            .isInstanceOf(ApicurioSchemaValidator.class);
                });
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
                        "reactive.commons.kafka.app.apicurio.url=http://registry:8080/apis/registry/v3",
                        "reactive.commons.kafka.app.apicurio.group-id=app",
                        "reactive.commons.kafka.accounts.connection-properties.bootstrap-servers=broker-b:9092",
                        "reactive.commons.kafka.accounts.apicurio.url=http://registry:8080/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.group-id=ap")
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
        properties.setUrl(url);
        properties.setGroupId(groupId);
        properties.setProperties(new HashMap<>());
        return properties;
    }

    @Test
    void shouldIsolateTheRegistryConfigurationOfEachDomain() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.group-id=app-group",
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.auth\\.client\\.id=app-client",
                        "reactive.commons.kafka.accounts.apicurio.url=http://accounts/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.group-id=accounts-group",
                        "reactive.commons.kafka.accounts.apicurio.validate-outbound=false",
                        "reactive.commons.kafka.accounts.apicurio.trust-inbound-coordinates=true",
                        "reactive.commons.kafka.accounts.apicurio.properties."
                                + "apicurio\\.registry\\.auth\\.client\\.id=accounts-client")
                .run(context -> {
                    AsyncKafkaPropsDomain domains = context.getBean(AsyncKafkaPropsDomain.class);
                    ApicurioValidationProperties app = domains.getProps("app").getApicurio();
                    ApicurioValidationProperties accounts = domains.getProps("accounts").getApicurio();

                    assertThat(app.getUrl()).isEqualTo("http://localhost:8080/apis/registry/v3");
                    assertThat(app.getGroupId()).isEqualTo("app-group");
                    assertThat(app.isValidateOutbound()).isTrue();
                    assertThat(app.isTrustInboundCoordinates()).isFalse();
                    assertThat(app.getProperties()).containsEntry("apicurio.registry.auth.client.id", "app-client");

                    assertThat(accounts.getUrl()).isEqualTo("http://accounts/apis/registry/v3");
                    assertThat(accounts.getGroupId()).isEqualTo("accounts-group");
                    assertThat(accounts.isValidateOutbound()).isFalse();
                    assertThat(accounts.isTrustInboundCoordinates()).isTrue();
                    assertThat(accounts.getProperties())
                            .containsEntry("apicurio.registry.auth.client.id", "accounts-client");

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
                        "reactive.commons.kafka.app.apicurio.group-id=app-group",
                        "reactive.commons.kafka.accounts.connection-properties.bootstrap-servers=broker-b:9092",
                        // The very same registry as the default domain
                        "reactive.commons.kafka.accounts.apicurio.url=http://localhost:8080/apis/registry/v3",
                        "reactive.commons.kafka.accounts.apicurio.group-id=accounts-group")
                .run(context -> {
                    AsyncKafkaPropsDomain domains = context.getBean(AsyncKafkaPropsDomain.class);
                    assertThat(domains.getProps("app").getApicurio().getUrl())
                            .isEqualTo(domains.getProps("accounts").getApicurio().getUrl());

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
    void shouldBindAllProperties() {
        runner.withPropertyValues(
                        "reactive.commons.kafka.app.apicurio.group-id=kafka",
                        "reactive.commons.kafka.app.apicurio.artifact-id=person",
                        "reactive.commons.kafka.app.apicurio.version=1",
                        "reactive.commons.kafka.app.apicurio.find-latest=false",
                        "reactive.commons.kafka.app.apicurio.validate-outbound=false",
                        "reactive.commons.kafka.app.apicurio.properties."
                                + "apicurio\\.registry\\.auth\\.client\\.id=id")
                .run(context -> {
                    var properties = context.getBean(AsyncKafkaPropsDomain.class).getProps("app").getApicurio();
                    assertThat(properties.getGroupId()).isEqualTo("kafka");
                    assertThat(properties.getArtifactId()).isEqualTo("person");
                    assertThat(properties.getVersion()).isEqualTo("1");
                    assertThat(properties.isFindLatest()).isFalse();
                    assertThat(properties.isValidateOutbound()).isFalse();
                    assertThat(properties.isValidateInbound()).isTrue();
                    assertThat(properties.getProperties())
                            .containsEntry("apicurio.registry.auth.client.id", "id");
                    assertThat(context).hasSingleBean(DomainSchemaValidatorProvider.class);
                });
    }
}
