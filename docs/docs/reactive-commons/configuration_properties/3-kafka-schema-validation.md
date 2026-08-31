---
sidebar_position: 3
---

# Kafka Schema Validation (Apicurio)

Reactive Commons can validate every Kafka message against a **JSON Schema** stored in an
[Apicurio Registry](https://www.apicur.io/registry/), both when it publishes an event and when it consumes one.

Validation is **opt-in**: without the Apicurio starter, Reactive Commons uses a no-op validator and nothing changes.

## Why a validator instead of an Apicurio SerDe

Reactive Commons keeps the Kafka wire format as raw bytes (`StringSerializer` for the key and `ByteArraySerializer`
for the value) because it needs full control of the payload to support CloudEvents, retries and DLQ. Replacing the
serdes with `JsonSchemaKafkaSerializer` / `JsonSchemaKafkaDeserializer` is therefore not possible.

Instead, the schema is resolved from the registry and applied to the payload that Reactive Commons is about to send, or
has just received. The schema coordinates travel in the record **headers**, the same mechanism the Apicurio Kafka serdes
use when `apicurio.registry.headers.enabled` is `true`. This keeps the payload as plain JSON and the messages wire
compatible in both directions with applications that still use the Apicurio Kafka serdes.

:::caution Apicurio changed the default of `apicurio.registry.headers.enabled` from `true` (2.x) to `false` (3.x). With
headers disabled the Apicurio serdes prepend a magic byte and the schema id to the payload, which is **not** compatible
with Reactive Commons. If the other side of the topic uses the Apicurio serdes, set
`apicurio.registry.headers.enabled: true` there. Reactive Commons always defaults to `true`, and **rejects at startup**
an explicit `apicurio.registry.headers.enabled: false`.
:::

## Adding the dependency

```groovy title="build.gradle"

implementation 'org.reactivecommons:async-kafka-apicurio-starter:<version>'
```

The starter transitively brings `async-commons-kafka-starter`, so it replaces it in your build file.

## Configuration

The validation is configured **inside each domain**, next to its connection properties:

```yaml title="application.yaml"
reactive:
  commons:
    kafka:
      app: # the default domain
        connection-properties:
          bootstrap-servers: "localhost:9092"
        apicurio:
          enabled: true                 # default true, set to false to disable validation
          url: "http://localhost:8080/apis/registry/v3"
          group-id:                     # optional. When empty, Apicurio uses the "default" group
          artifact-id:                  # optional. When empty, "<topic>-value" is used
          version:                      # optional. When empty, the latest version is used
          find-latest: true             # resolve the latest version when no version is set
          validate-outbound: true       # validate before publishing
          validate-inbound: true        # validate every consumed record
          trust-inbound-coordinates: false  # let a consumed record select the artifact through its headers
          properties: # any other Apicurio serde property, with its original key
            apicurio.registry.auth.client.id: "${REGISTRY_CLIENT_ID}"
            apicurio.registry.auth.client.secret: "${REGISTRY_CLIENT_SECRET}"
            apicurio.registry.auth.service.token.endpoint: "${REGISTRY_TOKEN_ENDPOINT}"
```

A domain without an `apicurio` block is not validated, so the starter can be on the classpath while only some domains
use it.

:::caution These values apply to **every topic of the domain**. `validate-outbound` and `validate-inbound` select a
*direction*, never a topic, and setting `artifact-id` forces that one artifact on all of them. See
[Per topic granularity](#per-topic-granularity) when a single topic needs to be treated differently.
:::

The `properties` map accepts every key of
[
`SerdeConfig`](https://github.com/Apicurio/apicurio-registry/blob/3.3.2/serdes/generic/serde-common/src/main/java/io/apicurio/registry/serde/config/SerdeConfig.java)
and
[
`SchemaResolverConfig`](https://github.com/Apicurio/apicurio-registry/blob/3.3.2/schema-resolver/src/main/java/io/apicurio/registry/resolver/config/SchemaResolverConfig.java),
including authentication (`apicurio.registry.auth.*`), TLS for the registry client (`apicurio.registry.request.ssl.*`)
and cache tuning (`apicurio.registry.check-period-ms`).

:::note Reactive Commons uses the Apicurio Registry serdes **3.3.2**, which target the Registry **v3** API
(`/apis/registry/v3`). Only the schema resolution and validation artifacts are pulled in
(`apicurio-registry-schema-resolver`, `apicurio-registry-serde-common`, `apicurio-registry-serde-kafka-common` and
`apicurio-registry-serde-common-jsonschema`); the Kafka serdes themselves are not used.
:::

### Defining the properties programmatically

Because the configuration lives inside the domain properties, it is set from code with the very same
[`KafkaPropsCustomizer`](./2-kafka.md) used for the rest of the Kafka settings. The properties bound from the
configuration files are handed over to the customizer, which can complete or override them before the validators are
built:

```java
import io.apicurio.registry.serde.config.SerdeConfig;
import org.reactivecommons.async.kafka.config.props.AsyncKafkaPropsDomain;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApicurioConfig {

    @Bean
    public AsyncKafkaPropsDomain.KafkaPropsCustomizer kafkaPropsCustomizer(RegistryCredentials credentials) {
        return domainProperties -> domainProperties.customize("app", props -> {
            props.getApicurio().setUrl(credentials.url());
            props.getApicurio().getProperties().put(SerdeConfig.AUTH_CLIENT_ID, credentials.clientId());
            props.getApicurio().getProperties().put(SerdeConfig.AUTH_CLIENT_SECRET, credentials.clientSecret());
        });
    }
}
```

This is the way to go when the registry endpoint or its credentials come from a secrets manager, a vault or any other
source that is not available as a configuration file. Use `customize(domain, ...)` rather than
`put(domain, props)`, so the values already bound from the YAML are preserved.

`enabled` is honoured here as well, so `props.getApicurio().setEnabled(false)` leaves that domain with the no-op
validator. All the consistency checks described below run **after** the customizer, on the final values.

:::note Declaring a bean of type `SchemaValidator` is a different, lower level extension point: it replaces the
validator altogether and the properties are no longer read. See [Customizing the validator](#customizing-the-validator).
:::

### Multiple domains

Every domain declared under `reactive.commons.kafka` carries its own `apicurio` block, so each one validates against its
own registry, group and artifacts:

```yaml title="application.yaml"
reactive:
  commons:
    kafka:
      app:
        connection-properties:
          consumer:
            group-id: my-service.consumer-group
        apicurio:
          url: "http://localhost:8080/apis/registry/v3"
          group-id: kafka
      accounts:
        connection-properties:
          consumer:
            group-id: my-service.consumer-group
        apicurio:
          url: "http://accounts-registry:8080/apis/registry/v3"
          group-id: accounts
          validate-outbound: false      # accounts publishes without validating
```

There is no inheritance between domains: each block is self contained, which keeps the effective configuration of a
domain readable in one place. When several domains share the same registry, use a YAML anchor to avoid repeating it:

```yaml
reactive:
  commons:
    kafka:
      app:
        apicurio: &apicurio
          url: "http://localhost:8080/apis/registry/v3"
      accounts:
        apicurio:
          <<: *apicurio
          group-id: accounts
```

A domain is left unvalidated either by omitting its `apicurio` block or with `enabled: false`:

```yaml
      legacy:
        apicurio:
          enabled: false                # legacy has no schemas registered yet
```

All the validators are built when the application starts, so a configuration error fails fast and the message points at
the exact property, for instance
`reactive.commons.kafka.accounts.apicurio.validate-outbound`.

:::note Domains resolving against the **same registry** share a single connection and a single schema cache, even when
their group or artifact differ: the cache is indexed by the full coordinates, so the entries of one group never collide
with those of another. Two domains only get separate clients when their registry configuration differs in something the
client depends on, such as the endpoint, the credentials or the cache tuning.
:::

#### Two brokers, one registry

Nothing ties a registry to a broker, so two domains connected to **different Kafka clusters** may validate against the
same registry. It is a supported setup, and both domains will share one registry client. The thing to watch is that the
artifact of a topic defaults to `<topic>-value` inside the domain's group, so two clusters that happen to have a topic
with the same name resolve the **same artifact** when both domains also share a group.

```yaml
reactive:
  commons:
    kafka:
      app:
        connection-properties:
          bootstrap-servers: "broker-a:9092"
        apicurio:
          url: "http://registry:8080/apis/registry/v3"
          group-id: app                 # keeps app's event.push apart from accounts'
      accounts:
        connection-properties:
          bootstrap-servers: "broker-b:9092"
        apicurio:
          url: "http://registry:8080/apis/registry/v3"
          group-id: accounts
```

Give each domain its own `group-id` when the same topic name means different things in each cluster, and share one group
when the intention is precisely that both clusters honour a single contract.

### About `group-id`

Leaving it empty is the same as setting it to `default`: when no group is given, Apicurio's
`ArtifactReferenceImpl.build()` sets the group to the literal `"default"`, which is the group the registry uses for
artifacts that were not created inside an explicit group. Set it only if you registered your schemas under a custom
group.

### `enabled` vs `validate-outbound` / `validate-inbound`

They act on different axes and none of them replaces the other:

| Property                                 | Effect                                                                                                                                                                  |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enabled: false`                         | No Apicurio validator is built for the domain and Reactive Commons keeps its no-op validator. The registry is never contacted, so `url` and credentials are not needed. |
| `validate-outbound` / `validate-inbound` | The validator **is** created and connected to the registry, only the given direction is skipped.                                                                        |

Apicurio itself has a **single** switch, `apicurio.registry.serde.validation-enabled`
([
`SerdeConfig.VALIDATION_ENABLED`](https://github.com/Apicurio/apicurio-registry/blob/3.3.2/serdes/generic/serde-common/src/main/java/io/apicurio/registry/serde/config/SerdeConfig.java),
default `true`), read by both `JsonSchemaSerializer` and `JsonSchemaDeserializer`. It applies to *both* directions,
which in Kafka is never a problem: the serializer and the deserializer are different objects living in different
applications, so a producer only ever configures the serializer.

Reactive Commons is different: **a single `SchemaValidator` instance serves the producer and the consumer of the same
domain**, so one flag could not express the common case of *"publish freely, validate what I receive"*. That is why the
two directions are split, and why `apicurio.registry.serde.validation-enabled` is not the way to switch the feature off:
`enabled` is.

:::danger Reactive Commons **fails at startup** with an `InvalidConfigurationException` for any configuration that would
create the validator, connect to the registry and cache schemas without validating a single message:

- `validate-outbound` and `validate-inbound` both `false`.
- `enabled: true` together with `apicurio.registry.serde.validation-enabled: false` in `properties`. Both are on/off
  switches for the same feature, so they must hold the same value; keeping them apart is a contradiction, not a valid
  combination.

If the intention is to turn the feature off, use `enabled: false`, which does not create anything.
:::

:::danger Reactive Commons also **fails at startup** when `properties` sets `apicurio.registry.headers.enabled: false`.
The schema coordinates always travel in the record headers, so that property may only be set to `true`. See
[Why the schema coordinates are always written](#why-the-schema-coordinates-are-always-written).
:::

:::caution
`validate-outbound: false` also means the schema is **not resolved** when publishing, so the record leaves **without the
schema coordinates in its headers**. The consumer loses version fidelity and an Apicurio-serdes consumer will not be
able to read the message. Disabling the outbound direction implies giving up the coordinates.
:::

### When to use each combination

| Scenario                                                                                                                                                                                     | Configuration                                         |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| **Progressive adoption.** The topic already has legacy producers that do not comply. Guarantee what *this* service publishes without breaking the consumption of records still in the topic. | `validate-inbound: false`                             |
| **Defensive consumer.** Reject malformed records coming from another team, while publishing to a topic whose schema is not registered yet.                                                   | `validate-outbound: false`                            |
| **DLQ reprocessor.** Its input is invalid by definition; it validates only what it republishes after fixing it.                                                                              | `validate-inbound: false`                             |
| **Producer with generated types.** The payload is built from classes generated out of the very same artifact, so validating on the way out is redundant on high volume topics.               | `validate-outbound: false`                            |
| **Local development and tests.** There is no registry reachable from the laptop or the CI job.                                                                                               | `enabled: false`                                      |
| **Production incident.** An incompatible version was registered or the registry is degraded, and the flow must be restored without a redeployment.                                           | `enabled: false`                                      |
| **Service that only produces or only consumes.**                                                                                                                                             | nothing to set, the unused direction is never invoked |

### Why the schema coordinates are always written

When a record is published, the resolved coordinates (`apicurio.value.groupId`, `apicurio.value.artifactId`,
`apicurio.value.version`, ...) are written into the record headers. This is **not configurable**: setting
`apicurio.registry.headers.enabled: false` in `properties` fails at startup, because the resulting behaviour would not
be the one described by that property. The property may be set, but only to `true`.

The headers are the only channel Reactive Commons has to tell the consumer *which schema version* a record was produced
with. Disabling them would not make the payload compatible with anything: the Apicurio Kafka serdes expect a magic byte
and a schema id at the start of the value, which Reactive Commons never writes. So the only real effect of not writing
them would be losing version fidelity, and that failure is silent:

- **While the schema has a single version**, everything works. The consumer falls back to the configured coordinates,
  resolves the very same artifact, and validation passes.
- **The day the schema evolves**, records already published in the topic start being validated against whatever
  `find-latest` returns *now* instead of the version they were produced with, and previously valid records begin to
  fail.

Because the option can only turn a working setup into one that breaks later, it is rejected instead of honoured.
Consumers that do not understand the headers simply ignore them, so writing them is always safe.

:::note Apicurio 3.x changed the default of `apicurio.registry.headers.enabled` from `true` to `false`. Reactive Commons
keeps the 2.x behaviour and always writes them, so the value is forced to `true`.
:::

### Which artifact is used

| Situation                                                | Artifact resolved                                                 |
|----------------------------------------------------------|-------------------------------------------------------------------|
| `artifact-id` is configured                              | that artifact, for every topic                                    |
| `artifact-id` is empty                                   | `<topic>-value` (same convention as Apicurio's `TopicIdStrategy`) |
| Consuming a record whose headers name the artifact above | the same artifact, at the **version** of the headers              |
| Consuming a record whose headers name another artifact   | the configured artifact, the headers are ignored                  |

On the consumer side the artifact is always the one configured for the topic, and only the **version** is taken from the
record headers, so a message keeps being validated against the very same schema version its producer used while it can
never point the consumer somewhere else. If the record has no usable headers, the configured artifact is resolved at its
latest version.

:::danger Why the headers cannot choose the artifact The headers are written by whoever produced the record, so trusting
them completely would mean letting the producer choose the schema its own payload is validated against: pointing them at
a permissive artifact registered anywhere in the registry turns inbound validation into a no-op, and pointing every
record at a different artifact turns the consumer into an amplifier of requests against the registry.
:::

`trust-inbound-coordinates: true` restores the unrestricted behaviour, where a content id, global id or full set of
coordinates coming in the headers resolves whatever it names.

#### When `trust-inbound-coordinates` is needed

A content id, a global id and a content hash identify a schema by its content, so the registry cannot tell which
artifact they belong to: resolving one of them returns the schema and nothing else. There is no way to check that it is
the artifact of the topic, which is why honouring them has to be an explicit decision.

| Who produces the records                                       | What travels in the headers | Strict, the default                     | With `trust-inbound-coordinates` |
|----------------------------------------------------------------|-----------------------------|-----------------------------------------|----------------------------------|
| Reactive Commons                                               | group, artifact and version | The version is honoured                 | Same                             |
| Apicurio Kafka serdes, default configuration                   | **content id**              | Ignored, the configured version is used | The exact schema is resolved     |
| Apicurio Kafka serdes with `apicurio.registry.use-id=globalId` | global id                   | Ignored                                 | The exact schema is resolved     |

So a Reactive Commons producer never needs it: it writes the full coordinates, and the strict mode already keeps every
record validated against the version it was published with.

It becomes relevant when consuming a topic **produced by an application that uses the Apicurio serdes**. Leaving it off
there is not wrong, only less precise: records are validated against whatever version the configuration resolves rather
than the one they carry. That works while the schema has a single version and starts rejecting old records the day it
evolves, so the rejection message points at the coordinates that were ignored and names this property.

:::caution Enable it only when every producer of the topic is trusted. It hands the choice of the schema to whoever
publishes the record, which is exactly what the strict mode prevents.
:::

The resolved schemas are **cached** by the Apicurio schema resolver, so the registry is not called for every message.

### Remote `$ref` are not downloaded

A schema is only allowed to reference the artifacts the registry itself resolves as references. A `$ref` pointing at an
arbitrary URL is rejected while the schema is being parsed, instead of making the application download it, and every
reference is resolved at that moment rather than during the validation of the first message.

### Blocking behaviour and cache lifetime

Resolving a schema is a **blocking** HTTP call issued from the thread that publishes or consumes the record, and the
Apicurio registry client does not apply a request timeout, so an unreachable registry can hold that thread. Two defaults
are therefore changed with respect to the Apicurio serdes, both overridable through `properties`:

| Property                                   | Apicurio default | Reactive Commons default | Reason                                                                                                                 |
|--------------------------------------------|------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------|
| `apicurio.registry.check-period-ms`        | `30000`          | `1800000`                | A registered version is immutable, so re-resolving it twice a minute only puts a blocking call back into the hot path. |
| `apicurio.registry.fault-tolerant-refresh` | `false`          | `true`                   | A registry that blinks while an entry is refreshed keeps serving the cached schema instead of failing the message.     |

:::tip Keep the registry close to the application, and lower `check-period-ms` only if `find-latest` has to pick up new
versions quickly.
:::

## What is validated: the whole record value

Reactive Commons validates the **exact bytes** that travel in the record value, which is the Reactive Commons envelope,
not the domain payload alone. For a `DomainEvent` the value published to the topic is:

```json
{
  "name": "event.push",
  "eventId": "9894f4a7-4cdb-4fd0-8314-c7514f71bf76",
  "data": {
    "title": "Notification title",
    "message": "Hello",
    "dateSend": "2026-08-29T17:43:55.888052"
  }
}
```

:::caution The artifact registered in Apicurio must therefore describe the **envelope**, not only the contents of`data`.
Registering the domain schema alone is the most common mistake: with `"additionalProperties": false` it fails with
`required property 'title' not found` plus `property 'name'/'eventId'/'data' is not defined in the schema`, because the
validator is comparing the envelope against a schema written for `data`.
:::

Wrap your domain schema like this:

```json title="event.push-value"
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    },
    "eventId": {
      "type": "string"
    },
    "data": {
      "type": "object",
      "properties": {
        "title": {
          "type": "string"
        },
        "message": {
          "type": "string"
        },
        "dateSend": {
          "type": "string"
        }
      },
      "required": [
        "title",
        "message",
        "dateSend"
      ],
      "additionalProperties": false
    }
  },
  "required": [
    "name",
    "eventId",
    "data"
  ],
  "additionalProperties": false
}
```

When the event is emitted as a **CloudEvent** the record value is the CloudEvent itself in structured mode
(`application/cloudevents+json`), so the schema must describe the CloudEvent attributes and its `data` member.

## What happens when a message does not comply

A `SchemaValidationException` is raised **per message**, so an invalid record never blocks the rest of the partition.

### Producing

The `Mono` returned by `DomainEventBus.emit(...)` fails and nothing is written to the topic. The error surfaces to your
own code, which decides what to do with it.

### Consuming

The record is **not** delivered to the handler and, most importantly, **schema validation is never retried**.

Validation runs in `GenericMessageListener.handle(...)` *before* the handler is invoked and *outside* the
`retryWhen(...)` operator, so `maxRetries` and `retryDelay` do not apply to it. This is intentional: an invalid payload
will never become valid by being processed again, so retrying it only delays the inevitable and wastes consumer
throughput.

The record therefore goes straight to the fallback strategy on the very first attempt:

| `maxRetries`     | Behaviour on schema validation failure                                      |
|------------------|-----------------------------------------------------------------------------|
| `>= 0` (default) | `DEFINITIVE_DISCARD`: sent to the DLQ (or acknowledged if the DLQ is off)   |
| `-1`             | `FAST_RETRY`: **infinite** re-delivery loop, the message is never discarded |

:::warning Do not use `maxRetries = -1` together with schema validation. That setting means *infinite fast retries*, and
an invalid payload will be redelivered forever, blocking the partition.
:::

## Customizing the validator

There are two extension points, resolved in this order:

| Bean                            | Scope                                            | Effect                                                                   |
|---------------------------------|--------------------------------------------------|--------------------------------------------------------------------------|
| `SchemaValidator`               | Global, one instance shared by every domain      | Wins over everything else, the `apicurio` properties are not read        |
| `DomainSchemaValidatorProvider` | Per domain, asked once for each connected domain | Replaces the provider of the starter, the properties are not read either |
| *(none)*                        | —                                                | The starter builds the Apicurio validators from the properties           |

If neither bean is declared and validation is disabled, Reactive Commons falls back to `NoOpSchemaValidator`.

Any bean of type `SchemaValidator` replaces the default one, so a fully custom implementation can be provided:

```java
import org.apache.kafka.common.header.Headers;
import org.reactivecommons.async.kafka.validation.SchemaValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaValidationConfig {

    @Bean
    public SchemaValidator schemaValidator() {
        return new SchemaValidator() {
            @Override
            public void validateOutbound(String topic, byte[] payload, Headers headers) {
                // custom validation, may also enrich headers
            }

            @Override
            public void validateInbound(String topic, byte[] payload, Headers headers) {
                // custom validation
            }
        };
    }
}
```

To keep the Apicurio behaviour but change how the artifact is chosen per topic, declare an
`ArtifactReferenceProvider` and build the validator with `ApicurioSchemaValidator.builder()`.

### Choosing the validator per domain

A `SchemaValidator` bean applies to every domain. When the decision depends on the domain and
[the properties](#multiple-domains) are not enough, declare a `DomainSchemaValidatorProvider` instead:

```java
import org.reactivecommons.async.kafka.validation.DomainSchemaValidatorProvider;
import org.reactivecommons.async.kafka.validation.NoOpSchemaValidator;
import org.reactivecommons.async.kafka.validation.SchemaValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DomainSchemaValidationConfig {

    @Bean
    public DomainSchemaValidatorProvider domainSchemaValidatorProvider(SchemaValidator strict) {
        return domain -> "legacy".equals(domain) ? NoOpSchemaValidator.INSTANCE : strict;
    }
}
```

The provider is asked once per domain while the connections are being created, so it does not need to cache anything,
but it should return the same instance for the same domain to avoid duplicating the schema cache. Returning `null` is
allowed and means "no validation for this domain".

:::caution A validator built with `ApicurioSchemaValidatorFactory` holds a registry client, so it implements`Closeable`.
The provider of the starter releases the clients it created when the context is disposed; a custom
`SchemaValidator` or `DomainSchemaValidatorProvider` bean is responsible for its own, either by implementing
`AutoCloseable` (Spring infers `close` as the destroy method) or by declaring `@Bean(destroyMethod = "close")`.

To build several validators over a single connection and cache, create the resolver once with
`ApicurioSchemaValidatorFactory.createResolver(configs)` and pass it to
`ApicurioSchemaValidatorFactory.create(resolver, configs, ...)`. A validator built that way does **not** own the
resolver, so closing it leaves the resolver usable for the other domains and releasing it stays with the caller.
:::

### Per topic granularity

Since the configuration cannot distinguish topics, treating one of them differently requires a custom bean that
delegates to a validator built with `ApicurioSchemaValidatorFactory.create(...)`. The factory takes the very same
Apicurio keys used in `properties`, so nothing else changes:

```java
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import org.apache.kafka.common.header.Headers;
import org.reactivecommons.async.kafka.apicurio.ApicurioSchemaValidatorFactory;
import org.reactivecommons.async.kafka.validation.SchemaValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Set;

@Configuration
public class SelectiveSchemaValidationConfig {

    private static final Set<String> VALIDATED_ON_PUBLISH = Set.of("event.audit");

    @Bean
    public SchemaValidator schemaValidator(@Value("${apicurio.url}") String url) {
        SchemaValidator delegate = ApicurioSchemaValidatorFactory.create(
                Map.of(SchemaResolverConfig.REGISTRY_URL, url));

        return new SchemaValidator() {
            @Override
            public void validateOutbound(String topic, byte[] payload, Headers headers) {
                if (VALIDATED_ON_PUBLISH.contains(topic)) {
                    delegate.validateOutbound(topic, payload, headers);
                }
            }

            @Override
            public void validateInbound(String topic, byte[] payload, Headers headers) {
                delegate.validateInbound(topic, payload, headers);
            }
        };
    }
}
```

Because the bean is declared with the plain `SchemaValidator` type, the one from the starter backs off
(`@ConditionalOnMissingBean`) and the `apicurio` properties are no longer read: the delegate owns the whole
configuration. Skipping `validateOutbound` for a topic also skips writing its schema coordinates in the headers.

If what changes per topic is only the artifact, do not write a custom `SchemaValidator`: implement
`ArtifactReferenceProvider` and pass it to `ApicurioSchemaValidator.builder()`.
