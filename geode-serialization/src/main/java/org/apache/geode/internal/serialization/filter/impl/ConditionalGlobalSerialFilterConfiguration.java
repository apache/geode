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
package org.apache.geode.internal.serialization.filter.impl;

import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedClassNames;
import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedSerializablesServices;

import java.util.Set;

import org.apache.geode.internal.serialization.filter.FilterConfiguration;
import org.apache.geode.internal.serialization.filter.SanctionedSerializablesFilterPattern;
import org.apache.geode.internal.serialization.filter.SerializableObjectConfig;

public class ConditionalGlobalSerialFilterConfiguration implements FilterConfiguration {

  private final SerializableObjectConfig serializableObjectConfig;

  public ConditionalGlobalSerialFilterConfiguration(
      SerializableObjectConfig serializableObjectConfig) {
    this.serializableObjectConfig = serializableObjectConfig;
  }

  @Override
  public boolean configure() {
    serializableObjectConfig.setValidateSerializableObjects(true);

    String filterPattern = new SanctionedSerializablesFilterPattern()
        .append(serializableObjectConfig.getFilterPatternIfEnabled())
        .pattern();

    Set<String> sanctionedClasses =
        loadSanctionedClassNames(loadSanctionedSerializablesServices());

    GlobalSerialFilter globalSerialFilter = new DelegatingGlobalSerialFilterFactory()
        .create(filterPattern, sanctionedClasses);

    return new GlobalSerialFilterConfiguration(globalSerialFilter).configure();
  }
}
