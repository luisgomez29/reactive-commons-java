package org.reactivecommons.async.kafka.config.props;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Apicurio Registry schema validation of a single Reactive Commons domain.
 * <p>
 * Only the switches that have no Apicurio equivalent are typed here. Everything the registry itself understands is
 * written in {@code properties} with its original Apicurio key, so an existing serde configuration can be pasted
 * as is and there are no two names for the same thing. Schema validation itself is turned on and off with
 * {@code apicurio.registry.serde.validation-enabled}, exactly as it would be for the Apicurio serdes: when it is
 * {@code false} Reactive Commons keeps its default no-op validator instead of connecting to the registry.
 * <pre>
 * reactive:
 *   commons:
 *     kafka:
 *       app:
 *         apicurio:
 *           validate-inbound: true
 *           properties:
 *             apicurio.registry.url: http://localhost:8080/apis/registry/v3
 *             apicurio.registry.artifact.group-id: kafka
 *       accounts:
 *         apicurio:
 *           properties:
 *             apicurio.registry.serde.validation-enabled: false
 * </pre>
 * <p>
 * These values are only read when the {@code async-commons-kafka-apicurio-starter} dependency is present.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class ApicurioValidationProperties {

    /**
     * Validates the payload before publishing it.
     */
    @Builder.Default
    private boolean validateOutbound = true;

    /**
     * Validates the payload of every consumed record before it reaches the handler.
     */
    @Builder.Default
    private boolean validateInbound = true;

    /**
     * Every Apicurio setting, using its original key: {@code apicurio.registry.url},
     * {@code apicurio.registry.artifact.group-id}, {@code apicurio.registry.artifact.artifact-id},
     * {@code apicurio.registry.artifact.version}, {@code apicurio.registry.find-latest},
     * {@code apicurio.registry.serde.validation-enabled}, {@code apicurio.registry.auth.*} and any other one the
     * serdes accept.
     * <p>
     * Two of them are constrained: {@code apicurio.registry.headers.enabled} may only be {@code true}, because the
     * schema coordinates always travel in the record headers, and the schema version has to be decided explicitly,
     * either with {@code apicurio.registry.artifact.version} or with {@code apicurio.registry.find-latest=true}.
     * Both are rejected at startup, and note that {@code find-latest} defaults to {@code false} as it does in
     * Apicurio.
     */
    @Builder.Default
    private Map<String, String> properties = new HashMap<>();
}
