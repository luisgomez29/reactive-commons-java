package org.reactivecommons.async.kafka.listeners;

import lombok.extern.java.Log;
import org.reactivecommons.async.api.handlers.CloudCommandHandler;
import org.reactivecommons.async.api.handlers.DomainCommandHandler;
import org.reactivecommons.async.api.handlers.RawCommandHandler;
import org.reactivecommons.async.api.handlers.registered.RegisteredCommandHandler;
import org.reactivecommons.async.commons.CommandExecutor;
import org.reactivecommons.async.commons.DiscardNotifier;
import org.reactivecommons.async.commons.HandlerResolver;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.kafka.KafkaMessage;
import org.reactivecommons.async.kafka.communications.ReactiveMessageListener;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.function.Function;

@Log
public class ApplicationCommandListener extends GenericMessageListener {

    private static final String NAME = "name";
    private static final String COMMAND_ID = "commandId";
    private static final String TYPE = "type";
    private final MessageConverter messageConverter;
    private final HandlerResolver resolver;
    private final String appName;

    public ApplicationCommandListener(ReactiveMessageListener receiver,
                                      HandlerResolver resolver,
                                      MessageConverter messageConverter,
                                      boolean withDLQRetry,
                                      boolean createTopology,
                                      long maxRetries,
                                      int retryDelay,
                                      DiscardNotifier discardNotifier,
                                      CustomReporter errorReporter,
                                      String appName) {
        super(receiver, withDLQRetry, createTopology, maxRetries, retryDelay, discardNotifier,
                "command", errorReporter, appName + "-commands", resolver.getCommandNames());
        this.resolver = resolver;
        this.messageConverter = messageConverter;
        this.appName = appName;
    }

    // TODO: Check if necessary
    @Override
    protected List<String> getDlqBaseTopics() {
        return List.of(appName);
    }

    @Override
    protected Function<Message, Mono<Object>> rawMessageHandler(String executorPath) {
        final RegisteredCommandHandler<Object, Object> commandHandler = resolver.getCommandHandler(executorPath);
        Function<Message, Object> converter = resolveConverter(commandHandler);
        final CommandExecutor<Object> executor = new CommandExecutor<>(commandHandler.handler(), converter);
        return msj -> executor.execute(msj).cast(Object.class);
    }

    @Override
    protected String getExecutorPath(ReceiverRecord<String, byte[]> msj) {
        KafkaMessage kafkaMessage = KafkaMessage.fromDelivery(msj);
        JsonNode jsonNode = messageConverter.readValue(kafkaMessage, JsonNode.class);
        if (jsonNode.get(COMMAND_ID) != null) {
            return jsonNode.get(NAME).asString();
        }
        if (jsonNode.get(TYPE) != null) {
            return jsonNode.get(TYPE).asString();
        }
        return kafkaMessage.getType();
    }

    @Override
    protected Object parseMessageForReporter(Message message) {
        return messageConverter.readCommandStructure(message);
    }

    private <T, D> Function<Message, Object> resolveConverter(RegisteredCommandHandler<T, D> registeredCommandHandler) {
        if (registeredCommandHandler.handler() instanceof DomainCommandHandler) {
            final Class<T> commandClass = registeredCommandHandler.inputClass();
            return msj -> messageConverter.readCommand(msj, commandClass);
        } else if (registeredCommandHandler.handler() instanceof CloudCommandHandler) {
            return messageConverter::readCloudEvent;
        } else if (registeredCommandHandler.handler() instanceof RawCommandHandler) {
            return message -> message;
        }
        throw new RuntimeException("Unknown handler type");
    }
}
