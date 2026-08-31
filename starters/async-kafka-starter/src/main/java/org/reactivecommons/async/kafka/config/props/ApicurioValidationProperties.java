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
 * It is bound as part of the domain properties, so each domain validates against its own registry, group and
 * artifacts:
 * <pre>
 * reactive:
 *   commons:
 *     kafka:
 *       app:
 *         apicurio:
 *           url: http://localhost:8080/apis/registry/v3
 *           group-id: kafka
 *       accounts:
 *         apicurio:
 *           url: http://localhost:8080/apis/registry/v3
 *           group-id: accounts
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
     * Enables schema validation for the domain. When false Reactive Commons keeps its default no-op validator.
     */
    @Builder.Default
    private boolean enabled = true;

    /**
     * Apicurio Registry endpoint, for example {@code http://localhost:8080/apis/registry/v3}.
     */
    private String url;

    /**
     * Explicit artifact group id. When empty, Apicurio resolves the artifacts in the {@code default} group.
     */
    private String groupId;

    /**
     * Explicit artifact id. When empty the convention {@code <topic>-value} is used.
     */
    private String artifactId;

    /**
     * Explicit artifact version. When empty the latest version is used if {@code findLatest} is true.
     */
    private String version;

    /**
     * Resolves the latest version of the artifact when no explicit version is configured.
     */
    @Builder.Default
    private boolean findLatest = true;

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
     * Lets an incoming record select the artifact it is validated against through its own schema coordinate
     * headers.
     * <p>
     * By default, only the <em>version</em> is taken from the headers, and only when they name the artifact the
     * topic already uses, because otherwise whoever publishes a record chooses the schema its own payload is
     * checked against.
     * <p>
     * A Reactive Commons producer never needs this: it writes the full coordinates, which the default already
     * honours. It matters when consuming a topic produced with the Apicurio Kafka serdes, which identify the
     * schema by content id, and a content id cannot be matched against the artifact of the topic because the
     * registry does not tell which artifact it belongs to. Enable it only when every producer of the topic is
     * trusted.
     */
    @Builder.Default
    private boolean trustInboundCoordinates = false;

    /**
     * Any other Apicurio serde property, using its original key, for instance
     * {@code apicurio.registry.auth.client.id} or {@code apicurio.registry.request.ssl.truststore.location}.
     * <p>
     * {@code apicurio.registry.headers.enabled} may only be set to {@code true}: the schema coordinates always
     * travel in the record headers, so a {@code false} is rejected at startup.
     */
    @Builder.Default
    private Map<String, String> properties = new HashMap<>();
}
