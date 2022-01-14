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

import java.util.Collection;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Creates an instance of {@code GlobalSerialFilter} that delegates to {@code ObjectInputFilterApi}
 * to maintain independence from the JRE version.
 */
class ReflectiveFacadeGlobalSerialFilterFactory implements GlobalSerialFilterFactory {

  private final ObjectInputFilterApi api;

  ReflectiveFacadeGlobalSerialFilterFactory() {
    this(new ReflectiveObjectInputFilterApiFactory().createObjectInputFilterApi());
  }

  @VisibleForTesting
  ReflectiveFacadeGlobalSerialFilterFactory(ObjectInputFilterApi api) {
    this.api = requireNonNull(api, "ObjectInputFilterApi is required");
  }

  @Override
  public GlobalSerialFilter create(String pattern, Collection<String> sanctionedClasses) {
    return new ReflectiveFacadeGlobalSerialFilter(api, pattern, sanctionedClasses);
  }
}
