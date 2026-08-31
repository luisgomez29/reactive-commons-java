package org.reactivecommons.async.kafka.apicurio;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import lombok.RequiredArgsConstructor;

/**
 * Default {@link ArtifactReferenceProvider}.
 * <p>
 * When explicit coordinates are configured ({@code apicurio.registry.artifact.artifact-id}) they win
 * for every topic. Otherwise, it falls back to the same convention used by Apicurio's
 * {@code TopicIdStrategy}: {@code <topic>-value}.
 */
@RequiredArgsConstructor
public class DefaultArtifactReferenceProvider implements ArtifactReferenceProvider {

    private static final String VALUE_SUFFIX = "-value";

    private final String explicitGroupId;
    private final String explicitArtifactId;
    private final String explicitVersion;

    @Override
    public ArtifactReference referenceFor(String topic) {
        if (explicitArtifactId != null && !explicitArtifactId.isBlank()) {
            return ArtifactReference.builder()
                    .groupId(emptyToNull(explicitGroupId))
                    .artifactId(explicitArtifactId)
                    .version(emptyToNull(explicitVersion))
                    .build();
        }
        return ArtifactReference.builder()
                .groupId(emptyToNull(explicitGroupId))
                .artifactId(topic + VALUE_SUFFIX)
                .version(emptyToNull(explicitVersion))
                .build();
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
