package org.reactivecommons.async.starter.listeners;


import org.reactivecommons.async.commons.HandlerResolver;
import org.reactivecommons.async.starter.config.ConnectionManager;
import org.reactivecommons.async.starter.broker.BrokerProvider;
import org.reactivecommons.async.starter.config.DomainHandlers;
import org.reactivecommons.async.starter.config.ReactiveCommonsConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ReactiveCommonsConfig.class)
public class NotificationEventsListenerConfig extends AbstractListenerConfig {

    public NotificationEventsListenerConfig(ConnectionManager manager, DomainHandlers handlers) {
        super(manager, handlers);
    }

    @SuppressWarnings("rawtypes")
    @Override
    void listen(String domain, BrokerProvider provider, HandlerResolver resolver) {
        provider.listenNotificationEvents(resolver);
    }
}
