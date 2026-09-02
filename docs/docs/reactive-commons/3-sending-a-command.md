---
sidebar_position: 3
---

# Sending a Command

:::warning Not available on Kafka Commands are **not supported by the Kafka implementation** of Reactive Commons. Use
the RabbitMQ implementation, or model the interaction as a
[domain event](./sending-a-domain-event/kafka.md) when the transport must be Kafka.
:::

## API specification

### Command model

To send a Command we need to know the Command structure, which is represented with the next class:

```java
public class Command<T> {
    private final String name;
    private final String commandId;
    private final T data;
}
```

Where name is the command name, commandId is an unique command identifier and data is a JSON Serializable payload.

### DirectAsyncGateway interface

```java
public interface DirectAsyncGateway {

    <T> Mono<Void> sendCommand(Command<T> command, String targetName); 

    <T> Mono<Void> sendCommand(Command<T> command, String targetName, long delayMillis);    

    <T> Mono<Void> sendCommand(Command<T> command, String targetName, String domain); // Send to specific domain

    <T> Mono<Void> sendCommand(Command<T> command, String targetName, long delayMillis, String domain); // Send to specific domain with delay

    Mono<Void> sendCommand(CloudEvent command, String targetName); // Send with CloudEvent format

    Mono<Void> sendCommand(CloudEvent command, String targetName, long delayMillis); // Send with CloudEvent format and delay

    Mono<Void> sendCommand(CloudEvent command, String targetName, String domain); // Send with CloudEvent format to specific domain
    
    Mono<Void> sendCommand(CloudEvent command, String targetName, long delayMillis, String domain);
}
```

You can send a CloudEvent or a Command\<T> to a target application. You also can send a command to a specific domain
(remote broker out of you application context).

## Enabling autoconfiguration

To send Commands you should enable the respecting spring boot autoconfiguration using the `@EnableDirectAsyncGateway` annotation
For example:

```java
@RequiredArgsConstructor
@EnableDirectAsyncGateway
public class ReactiveDirectAsyncGateway {
    public static final String TARGET_NAME = "other-app";// refers to remote spring.application.name property
    public static final String SOME_COMMAND_NAME = "some.command.name";
    private final DirectAsyncGateway gateway; // Auto injected bean created by the @EnableDirectAsyncGateway annotation

    public Mono<Void> runRemoteJob(Object command/*change for proper model*/)  {
         return gateway.sendCommand(new Command<>(SOME_COMMAND_NAME, UUID.randomUUID().toString(), command), TARGET_NAME);
    }
}
```

After that you can send commands from you application to a remote application that handles this command.

## Sending a Raw Command

There is no separate API to *send* a raw command: `RawCommandHandler` (see
[Listening Raw Commands](./7-handling-commands.md#listening-raw-commands)) is a **receiving-side** concept. You send the
command exactly like any other one, with `sendCommand(Command<T>, targetName)` or
`sendCommand(CloudEvent, targetName)`; what makes it "raw" is that the receiver processes it without converting it to a
`Command<T>` or `CloudEvent` first, and without filtering by command name.

```java
@RequiredArgsConstructor
@EnableDirectAsyncGateway
public class ReactiveDirectAsyncGateway {
    public static final String TARGET_NAME = "other-app";
    public static final String SOME_COMMAND_NAME = "some.command.name";
    private final DirectAsyncGateway gateway;

    public Mono<Void> runRemoteJob(Object command) {
        // Sent the same way as any other command; the target application decides to handle it with a
        // RawCommandHandler instead of a DomainCommandHandler
        return gateway.sendCommand(new Command<>(SOME_COMMAND_NAME, UUID.randomUUID().toString(), command), TARGET_NAME);
    }
}
```

This is only relevant for RabbitMQ, since commands are not supported on Kafka at all. A `RawCommandHandler` receives
every command routed to its queue as a `RabbitMessage`, regardless of the name used to send it, which is why it is
useful for consumers that do not want to declare a handler per command name, or that need the raw body/headers rather
than a deserialized payload.

## Example

You can see a real example at [samples/async/async-sender-client](https://github.com/reactive-commons/reactive-commons-java/tree/master/samples/async/async-sender-client)