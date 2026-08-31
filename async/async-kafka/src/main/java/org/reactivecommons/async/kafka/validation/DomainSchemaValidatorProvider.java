package org.reactivecommons.async.kafka.validation;

/**
 * Resolves the {@link SchemaValidator} that must be applied to a given Reactive Commons domain.
 * <p>
 * Every domain declared under {@code reactive.commons.kafka.<domain>} has its own connection, and may also have
 * its own schema registry, credentials or artifact conventions. Implementations return the validator configured
 * for the requested domain, which allows two domains of the same application to be validated independently.
 * <p>
 * A plain {@link SchemaValidator} bean takes precedence over this provider and is applied to every domain.
 */
@FunctionalInterface
public interface DomainSchemaValidatorProvider {

    /**
     * @param domain name of the domain, for instance the default {@code app}
     * @return the validator for that domain, never {@code null}. Use {@link NoOpSchemaValidator#INSTANCE} to skip
     * the validation of a particular domain.
     */
    SchemaValidator forDomain(String domain);
}
