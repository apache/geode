/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.serialization.filter;

import static java.util.Objects.requireNonNull;
import static org.apache.geode.internal.serialization.filter.ObjectInputFilterUtils.supportsObjectInputFilter;
import static org.apache.geode.internal.serialization.filter.ObjectInputFilterUtils.throwUnsupportedOperationException;

import java.util.Set;

/**
 * Creates an instance of {@code ObjectInputFilter} that delegates to {@code ObjectInputFilterApi}
 * to maintain independence from the JRE version.
 */
public class DelegatingObjectInputFilterFactory implements ObjectInputFilterFactory {

  private static final String UNSUPPORTED_MESSAGE =
      "A serialization filter has been specified but this version of Java does not support serialization filters - ObjectInputFilter is not available";

  private final ObjectInputFilterApi api;

  public DelegatingObjectInputFilterFactory() {
    this(new ReflectionObjectInputFilterApiFactory().createObjectInputFilterApi());
  }

  private DelegatingObjectInputFilterFactory(ObjectInputFilterApi api) {
    this.api = requireNonNull(api, "ObjectInputFilterApi is required");
  }

  @Override
  public ObjectInputFilter create(SerializableObjectConfig config, Set<String> sanctionedClasses) {
    if (config.getValidateSerializableObjects()) {
      requireObjectInputFilter();

      String pattern = new SanctionedSerializablesFilterPattern()
          .append(config.getSerializableObjectFilter())
          .pattern();

      return new DelegatingObjectInputFilter(api, pattern, sanctionedClasses);
    }
    return new NullObjectInputFilter();
  }

  public static void requireObjectInputFilter() {
    if (!supportsObjectInputFilter()) {
      throwUnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
  }
}
