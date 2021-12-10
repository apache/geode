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

import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedClassNames;
import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedSerializablesServices;

import java.util.Set;
import java.util.function.Supplier;

public class ConditionalGlobalSerialFilterConfiguration implements FilterConfiguration {

  private final SerializableObjectConfig serializableObjectConfig;
  private final FilterPatternFactory filterPatternFactory;
  private final Supplier<Set<String>> sanctionedClassesSupplier;

  public ConditionalGlobalSerialFilterConfiguration(
      SerializableObjectConfig serializableObjectConfig) {
    this(serializableObjectConfig,
        new DefaultFilterPatternFactory(),
        () -> loadSanctionedClassNames(loadSanctionedSerializablesServices()));
  }

  private ConditionalGlobalSerialFilterConfiguration(
      SerializableObjectConfig serializableObjectConfig,
      FilterPatternFactory filterPatternFactory,
      Supplier<Set<String>> sanctionedClassesSupplier) {
    this.serializableObjectConfig = serializableObjectConfig;
    this.filterPatternFactory = filterPatternFactory;
    this.sanctionedClassesSupplier = sanctionedClassesSupplier;
  }

  @Override
  public boolean configure() {
    serializableObjectConfig.setValidateSerializableObjects(true);

    String filterPattern = filterPatternFactory
        .create(serializableObjectConfig.getSerializableObjectFilterIfEnabled());

    Set<String> sanctionedClasses = sanctionedClassesSupplier.get();

    GlobalSerialFilter globalSerialFilter = new DelegatingGlobalSerialFilterFactory()
        .create(filterPattern, sanctionedClasses);

    return new GlobalSerialFilterConfiguration(globalSerialFilter).configure();
  }

  @FunctionalInterface
  interface FilterPatternFactory {
    String create(String optionalSerializableObjectFilter);
  }

  static class DefaultFilterPatternFactory implements FilterPatternFactory {

    @Override
    public String create(String optionalSerializableObjectFilter) {
      return new SanctionedSerializablesFilterPattern()
          .append(optionalSerializableObjectFilter)
          .pattern();
    }
  }
}
