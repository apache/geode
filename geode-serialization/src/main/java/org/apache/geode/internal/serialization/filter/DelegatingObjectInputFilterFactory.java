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

import java.util.Set;

import org.apache.geode.internal.serialization.filter.impl.DelegatingObjectInputFilter;
import org.apache.geode.internal.serialization.filter.impl.ObjectInputFilterApi;
import org.apache.geode.internal.serialization.filter.impl.ObjectInputFilterApiFactory;
import org.apache.geode.internal.serialization.filter.impl.ReflectionObjectInputFilterApiFactory;

public class DelegatingObjectInputFilterFactory implements ObjectInputFilterFactory {

  private final Runnable precondition;
  private final ObjectInputFilterApiFactory apiFactory;

  public DelegatingObjectInputFilterFactory(Runnable precondition) {
    this(new ReflectionObjectInputFilterApiFactory(), precondition);
  }

  private DelegatingObjectInputFilterFactory(ObjectInputFilterApiFactory apiFactory,
      Runnable precondition) {
    this.apiFactory = requireNonNull(apiFactory, "apiFactory is required");
    this.precondition = requireNonNull(precondition, "precondition is required");
  }

  @Override
  public ObjectInputFilter create(SerializableObjectConfig config, Set<String> sanctionedClasses) {
    if (config.getValidateSerializableObjects()) {
      precondition.run();

      ObjectInputFilterApi api = apiFactory.createObjectInputFilterApi();
      requireNonNull(api, "apiFactory is required to create a non-null filter api");

      String filterPattern = new SanctionedSerializablesFilterPattern()
          .append(config.getSerializableObjectFilter())
          .pattern();

      return new DelegatingObjectInputFilter(api, filterPattern, sanctionedClasses);
    }
    return new EmptyObjectInputFilter();
  }
}
