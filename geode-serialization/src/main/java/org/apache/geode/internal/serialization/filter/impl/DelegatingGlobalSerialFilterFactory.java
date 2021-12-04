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

import static java.util.Objects.requireNonNull;

import java.util.Collection;

class DelegatingGlobalSerialFilterFactory implements GlobalSerialFilterFactory {

  private final ObjectInputFilterApiFactory apiFactory;

  DelegatingGlobalSerialFilterFactory() {
    this(new ReflectionObjectInputFilterApiFactory());
  }

  private DelegatingGlobalSerialFilterFactory(ObjectInputFilterApiFactory apiFactory) {
    this.apiFactory = requireNonNull(apiFactory, "apiFactory is required");
  }

  @Override
  public GlobalSerialFilter create(String pattern, Collection<String> sanctionedClasses) {
    ObjectInputFilterApi api = apiFactory.createObjectInputFilterApi();
    requireNonNull(api, "apiFactory must create a non-null filter api");

    return new DelegatingGlobalSerialFilter(api, pattern, sanctionedClasses);
  }
}
