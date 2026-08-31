package org.reactivecommons.async.starter.impl.common.kafka.apicurio;

import com.networknt.schema.JsonSchema;
import io.apicurio.registry.resolver.SchemaResolver;
import lombok.extern.java.Log;
import org.reactivecommons.async.kafka.apicurio.ApicurioSchemaValidatorFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Hands out one Apicurio resolver, and therefore one registry connection and one schema cache, per distinct
 * registry configuration.
 * <p>
 * Domains are grouped by the keys the registry client depends on, that is the endpoint, the credentials, the TLS
 * settings and the cache tuning. The artifact coordinates are deliberately left out because they are resolved per
 * record by each validator, and the cache is indexed by the full coordinates, so domains with different groups
 * share a connection without their entries ever colliding.
 * <p>
 * The resolvers are owned by this class, which is what makes it responsible for releasing them.
 */
@Log
class SharedSchemaResolvers implements Closeable {

    private final Function<Map<String, Object>, SchemaResolver<JsonSchema, Object>> factory;
    private final Map<Map<String, Object>, SchemaResolver<JsonSchema, Object>> byRegistry = new LinkedHashMap<>();

    SharedSchemaResolvers() {
        this(ApicurioSchemaValidatorFactory::createResolver);
    }

    SharedSchemaResolvers(Function<Map<String, Object>, SchemaResolver<JsonSchema, Object>> factory) {
        this.factory = factory;
    }

    /**
     * @param registryConfig the registry level configuration of a domain
     * @return the resolver of that registry, created on first use and reused afterwards
     */
    SchemaResolver<JsonSchema, Object> forRegistry(Map<String, Object> registryConfig) {
        // The key is a copy, so a later change to the caller's map cannot corrupt the lookup
        return byRegistry.computeIfAbsent(new HashMap<>(registryConfig), factory);
    }

    /**
     * @return how many registry connections are being kept, one per distinct configuration
     */
    int count() {
        return byRegistry.size();
    }

    @Override
    public void close() {
        byRegistry.values().forEach(resolver -> {
            try {
                resolver.close();
            } catch (Exception e) {
                log.log(Level.WARNING, "Unable to release an Apicurio Registry client", e);
            }
        });
    }
}
