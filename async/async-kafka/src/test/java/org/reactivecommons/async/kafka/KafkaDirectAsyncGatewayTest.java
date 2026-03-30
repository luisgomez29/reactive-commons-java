package org.reactivecommons.async.kafka;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.AsyncQuery;
import org.reactivecommons.async.api.DirectAsyncGateway;
import org.reactivecommons.async.api.From;
import org.reactivecommons.async.kafka.communications.ReactiveMessageSender;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaDirectAsyncGatewayTest {
    @Mock
    private ReactiveMessageSender sender;
    @Mock
    private CloudEvent cloudEvent;
    @Mock
    private AsyncQuery<String> query;
    @Mock
    private From from;

    private final String targetName = "targetName";
    private final String domain = "domain";
    private final long delay = 1000L;

    @Test
    void shouldSendDomainCommand() {
        Command<String> command = new Command<>("testCmd", "cmd-1", "data");
        when(sender.send(any(Command.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(command, targetName))
                .verifyComplete();

        verify(sender).send(command, targetName);
    }

    @Test
    void shouldSendDomainCommandWithDelay() {
        Command<String> command = new Command<>("testCmd", "cmd-1", "data");
        when(sender.send(any(Command.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(command, targetName, delay))
                .verifyComplete();

        verify(sender).send(command, targetName);
    }

    @Test
    void shouldSendDomainCommandWithDomain() {
        Command<String> command = new Command<>("testCmd", "cmd-1", "data");
        when(sender.send(any(Command.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(command, targetName, domain))
                .verifyComplete();

        verify(sender).send(command, targetName);
    }

    @Test
    void shouldSendDomainCommandWithDelayAndDomain() {
        Command<String> command = new Command<>("testCmd", "cmd-1", "data");
        when(sender.send(any(Command.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(command, targetName, delay, domain))
                .verifyComplete();

        verify(sender).send(command, targetName);
    }

    @Test
    void shouldSendCloudEventCommand() {
        when(sender.send(any(CloudEvent.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(cloudEvent, targetName))
                .verifyComplete();

        verify(sender).send(cloudEvent, targetName);
    }

    @Test
    void shouldSendCloudEventCommandWithDelay() {
        when(sender.send(any(CloudEvent.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(cloudEvent, targetName, delay))
                .verifyComplete();

        verify(sender).send(cloudEvent, targetName);
    }

    @Test
    void shouldSendCloudEventCommandWithDomain() {
        when(sender.send(any(CloudEvent.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(cloudEvent, targetName, domain))
                .verifyComplete();

        verify(sender).send(cloudEvent, targetName);
    }

    @Test
    void shouldSendCloudEventCommandWithDelayAndDomain() {
        when(sender.send(any(CloudEvent.class), eq(targetName))).thenReturn(Mono.empty());
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        StepVerifier.create(gateway.sendCommand(cloudEvent, targetName, delay, domain))
                .verifyComplete();

        verify(sender).send(cloudEvent, targetName);
    }

    @Test
    void requestReplyMethodsAreNotImplemented() {
        DirectAsyncGateway gateway = new KafkaDirectAsyncGateway(sender);

        assertThrows(UnsupportedOperationException.class,
                () -> gateway.requestReply(cloudEvent, targetName, CloudEvent.class)
        );
        assertThrows(UnsupportedOperationException.class,
                () -> gateway.requestReply(cloudEvent, targetName, CloudEvent.class, domain)
        );
        assertThrows(UnsupportedOperationException.class,
                () -> gateway.requestReply(query, targetName, CloudEvent.class)
        );
        assertThrows(UnsupportedOperationException.class,
                () -> gateway.requestReply(query, targetName, CloudEvent.class, domain)
        );
        assertThrows(UnsupportedOperationException.class,
                () -> gateway.reply(targetName, from)
        );
    }
}
