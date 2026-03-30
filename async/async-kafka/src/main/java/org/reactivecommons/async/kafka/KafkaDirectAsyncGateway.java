package org.reactivecommons.async.kafka;

import io.cloudevents.CloudEvent;
import lombok.RequiredArgsConstructor;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.AsyncQuery;
import org.reactivecommons.async.api.DirectAsyncGateway;
import org.reactivecommons.async.api.From;
import org.reactivecommons.async.kafka.communications.ReactiveMessageSender;
import reactor.core.publisher.Mono;

import static org.reactivecommons.async.api.HandlerRegistry.DEFAULT_DOMAIN;

@RequiredArgsConstructor
public class KafkaDirectAsyncGateway implements DirectAsyncGateway {

    public static final String NOT_IMPLEMENTED_YET = "Not implemented yet";
    private final ReactiveMessageSender sender;

    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName) {
        return sendCommand(command, targetName, 0, DEFAULT_DOMAIN);
    }

    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName, long delayMillis) {
        return sendCommand(command, targetName, delayMillis, DEFAULT_DOMAIN);
    }

    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName, String domain) {
        return sendCommand(command, targetName, 0, domain);
    }

    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName, long delayMillis, String domain) {
        return sender.send(command, targetName);
    }

    @Override
    public Mono<Void> sendCommand(CloudEvent command, String targetName) {
        return sendCommand(command, targetName, 0, DEFAULT_DOMAIN);
    }

    @Override
    public Mono<Void> sendCommand(CloudEvent command, String targetName, long delayMillis) {
        return sendCommand(command, targetName, delayMillis, DEFAULT_DOMAIN);
    }

    @Override
    public Mono<Void> sendCommand(CloudEvent command, String targetName, String domain) {
        return sendCommand(command, targetName, 0, domain);
    }

    @Override
    public Mono<Void> sendCommand(CloudEvent command, String targetName, long delayMillis, String domain) {
        return sender.send(command, targetName);
    }

    @Override
    public <T, R> Mono<R> requestReply(AsyncQuery<T> query, String targetName, Class<R> type) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED_YET);
    }

    @Override
    public <T, R> Mono<R> requestReply(AsyncQuery<T> query, String targetName, Class<R> type, String domain) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED_YET);
    }

    @Override
    public <R extends CloudEvent> Mono<R> requestReply(CloudEvent query, String targetName, Class<R> type) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED_YET);
    }

    @Override
    public <R extends CloudEvent> Mono<R> requestReply(CloudEvent query, String targetName, Class<R> type, String domain) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED_YET);
    }

    @Override
    public <T> Mono<Void> reply(T response, From from) {
        throw new UnsupportedOperationException(NOT_IMPLEMENTED_YET);
    }
}
