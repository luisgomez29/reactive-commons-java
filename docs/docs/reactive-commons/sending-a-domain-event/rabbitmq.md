---
sidebar_position: 1
---

# RabbitMQ

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
[Listening Raw Events](../handling-domain-events/rabbitmq.md#listening-raw-events): build a `RabbitMessage` and pass it
to `emit(...)`.

`emit(String domain, RawMessage event)` is **not implemented**; it always throws `UnsupportedOperationException`,
exactly like the `DomainEvent` and `CloudEvent` overloads with an explicit domain.

`emit(RawMessage event)` always publishes to the domain's events exchange, using `RawMessage.getType()` as the **routing
key**. Build a `RabbitMessage` with the body and properties you need:

```java
@RequiredArgsConstructor
@EnableDomainEventBus
public class ReactiveEventsGateway {
    private final DomainEventBus domainEventBus;

    public Mono<Void> emitRaw(byte[] payload) {
        RabbitMessage.RabbitMessageProperties properties = new RabbitMessage.RabbitMessageProperties();
        properties.setContentType("application/json");
        properties.getHeaders().put("x-custom-header", "value");

        RawMessage rawMessage = new RabbitMessage(payload, properties, "some.event.name" /* routing key */);
        return Mono.from(domainEventBus.emit(rawMessage));
    }
}
```

`RabbitMessage.getType()` is used as the routing key on the domain's events exchange, exactly like
`DomainEvent.getName()`
or `CloudEvent.getType()` are for their respective overloads. The exchange itself is always the one configured for the
domain, it cannot be overridden through the raw message.

## Example

You can see a real example
at [samples/async/async-sender-client](https://github.com/reactive-commons/reactive-commons-java/tree/master/samples/async/async-sender-client)
