---
sidebar_position: 2
---

# Kafka

## API specification

### DomainEvent model

To emit a Domain Event we need to know the DomainEvent structure, which is represented with the next class:

```java
public class DomainEvent<T> {
    private final String name;
    private final String eventId;
    private final T data;
}
```

Where name is the event name, eventId is an unique event identifier and data is a JSON Serializable payload.

### DomainEventBus interface

```java
public interface DomainEventBus {
    <T> Publisher<Void> emit(DomainEvent<T> event);

    <T> Publisher<Void> emit(String domain, DomainEvent<T> event);

    Publisher<Void> emit(CloudEvent event);

    Publisher<Void> emit(String domain, CloudEvent event);

    Publisher<Void> emit(RawMessage event);
    
    Publisher<Void> emit(String domain, RawMessage event);
}
```

## Enabling autoconfiguration

To send Domain Events you should enable the respecting spring boot autoconfiguration using the `@EnableDomainEventBus`
annotation For example:

```java
@RequiredArgsConstructor
@EnableDomainEventBus
public class ReactiveEventsGateway {
    public static final String SOME_EVENT_NAME = "some.event.name";
    private final DomainEventBus domainEventBus; // Auto injected bean created by the @EnableDomainEventBus annotation

    public Mono<Void> emit(Object event) {
         return Mono.from(domainEventBus.emit(new DomainEvent<>(SOME_EVENT_NAME, UUID.randomUUID().toString(), event)));
    }
}
```

After that you can emit events from you application.

## Sending a Raw Message

`DomainEventBus.emit(RawMessage event)` bypasses the `DomainEvent` / `CloudEvent` conventions: instead of building a
generic envelope, you hand over the broker-specific message yourself, with full control over its body, routing and
headers. This is the emitting counterpart of
[Listening Raw Events](../handling-domain-events/kafka.md#listening-raw-events): build a `KafkaMessage` and pass it to
`emit(...)`.

`emit(String domain, RawMessage event)` is **not implemented**; it always throws `UnsupportedOperationException`,
exactly like the `DomainEvent` and `CloudEvent` overloads with an explicit domain.

For Kafka there is no separate routing key parameter: the **topic** (and the partitioning key) travel inside the
`KafkaMessage` itself, through `KafkaMessageProperties`:

```java
@RequiredArgsConstructor
@EnableDomainEventBus
public class ReactiveEventsGateway {
    private final DomainEventBus domainEventBus;

    public Mono<Void> emitRaw(byte[] payload) {
        KafkaMessage.KafkaMessageProperties properties = new KafkaMessage.KafkaMessageProperties();
        properties.setTopic("some.event.name");         // required: this is where the record is published
        properties.setKey(UUID.randomUUID().toString()); // optional: Kafka partitioning key
        properties.getHeaders().put("content-type", "application/json");

        RawMessage rawMessage = new KafkaMessage(payload, properties, null);
        return Mono.from(domainEventBus.emit(rawMessage));
    }
}
```

:::caution `KafkaMessageProperties.topic` is **required**. Reactive Commons resolves the topic to publish to directly
from the `KafkaMessage` properties, unlike `DomainEvent` and `CloudEvent` whose name/type is used for that purpose. A
missing or blank topic fails the send, either because it does not exist (when topic checking is enabled) or because
Kafka itself rejects a record without a topic.
:::

Because the schema validator (when configured) validates the outbound payload against the topic in
`KafkaMessageProperties.topic`, a raw message is validated exactly like any other message published to that topic. This
is where a producer that pins its own schema version — instead of also resolving `find-latest` — silently propagates
that pinned version to every consumer of the topic, see
[A producer that pins the version silently defeats
`find-latest` downstream](../configuration_properties/3-kafka-schema-validation.md#a-producer-that-pins-the-version-silently-defeats-find-latest-downstream).

## Example

You can see a real example
at [samples/async/async-sender-client](https://github.com/reactive-commons/reactive-commons-java/tree/master/samples/async/async-sender-client)
