package org.reactivecommons.async.kafka.listeners;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.handlers.CloudCommandHandler;
import org.reactivecommons.async.api.handlers.CommandHandler;
import org.reactivecommons.async.api.handlers.DomainCommandHandler;
import org.reactivecommons.async.api.handlers.registered.RegisteredCommandHandler;
import org.reactivecommons.async.commons.HandlerResolver;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.kafka.communications.ReactiveMessageListener;
import org.reactivecommons.async.kafka.communications.topology.TopologyCreator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"rawtypes", "unchecked"})
@ExtendWith(MockitoExtension.class)
class ApplicationCommandListenerTest {

    @Mock
    private ReactiveMessageListener receiver;
    @Mock
    private HandlerResolver resolver;
    @Mock
    private MessageConverter messageConverter;
    @Mock
    private Message message;

    private ApplicationCommandListener applicationCommandListener;

    @BeforeEach
    void setup() {
        when(resolver.getCommandNames()).thenReturn(List.of("create-order", "cancel-order"));
        applicationCommandListener = new ApplicationCommandListener(
                receiver,
                resolver,
                messageConverter,
                true,
                true,
                3,
                1000,
                null,
                null,
                "testApp"
        );
    }

    @Test
    void shouldHandleRawMessageSuccessfullyWithDomainCommand() {
        Command<String> command = new Command<>("sample", "id", "data");
        DomainCommandHandler domainCommandHandler = mock(DomainCommandHandler.class);
        when(domainCommandHandler.handle(command)).thenReturn(Mono.empty());
        RegisteredCommandHandler<Object, Object> registeredCommandHandlerMock = mock(RegisteredCommandHandler.class);
        when(registeredCommandHandlerMock.handler()).thenReturn(domainCommandHandler);
        when(registeredCommandHandlerMock.inputClass()).thenReturn(Object.class);
        when(resolver.getCommandHandler(anyString())).thenReturn(registeredCommandHandlerMock);
        when(messageConverter.readCommand(any(Message.class), any(Class.class))).thenReturn(command);

        Mono<Object> flow = applicationCommandListener.rawMessageHandler("executorPath").apply(message);

        StepVerifier.create(flow)
                .verifyComplete();

        verify(resolver, times(1)).getCommandHandler(anyString());
        verify(messageConverter, times(1)).readCommand(any(Message.class), any(Class.class));
    }

    @Test
    void shouldHandleRawMessageSuccessfullyWhenCloudEventCommand() {
        CloudEvent event = mock(CloudEvent.class);
        CommandHandler cloudCommandHandler = mock(CloudCommandHandler.class);
        when(cloudCommandHandler.handle(event)).thenReturn(Mono.empty());
        RegisteredCommandHandler<Object, Object> registeredCommandHandlerMock = mock(RegisteredCommandHandler.class);
        when(registeredCommandHandlerMock.handler()).thenReturn(cloudCommandHandler);
        when(resolver.getCommandHandler(anyString())).thenReturn(registeredCommandHandlerMock);
        when(messageConverter.readCloudEvent(any(Message.class))).thenReturn(event);

        Mono<Object> flow = applicationCommandListener.rawMessageHandler("executorPath").apply(message);

        StepVerifier.create(flow)
                .verifyComplete();

        verify(resolver, times(1)).getCommandHandler(anyString());
        verify(messageConverter, times(1)).readCloudEvent(any(Message.class));
    }

    @Test
    void shouldCreatePerCommandTopicsAndSingleDlqTopicForAppName() {
        TopologyCreator topologyCreator = mock(TopologyCreator.class);
        when(topologyCreator.createTopics(any(List.class))).thenReturn(Mono.empty());
        when(topologyCreator.createDlqTopics(any(List.class))).thenReturn(Mono.empty());

        applicationCommandListener.setUpBindings(topologyCreator).block();

        verify(topologyCreator, times(1)).createTopics(List.of("create-order", "cancel-order"));
        verify(topologyCreator, times(1)).createDlqTopics(List.of("testApp"));
    }
}
