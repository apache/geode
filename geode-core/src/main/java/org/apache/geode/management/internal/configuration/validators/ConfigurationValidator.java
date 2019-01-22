package org.apache.geode.management.internal.configuration.validators;

import org.apache.geode.cache.configuration.CacheElement;

public interface ConfigurationValidator<T extends CacheElement> {

  public void validate(T config);
}
