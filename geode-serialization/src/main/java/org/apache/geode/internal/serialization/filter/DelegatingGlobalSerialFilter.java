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

import static java.util.Collections.unmodifiableCollection;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

class DelegatingGlobalSerialFilter implements GlobalSerialFilter {

  private final ObjectInputFilterApi api;
  private final String pattern;
  private final Collection<String> sanctionedClasses;

  DelegatingGlobalSerialFilter(ObjectInputFilterApi api, String pattern,
      Collection<String> sanctionedClasses) {
    this.pattern = pattern;
    this.sanctionedClasses = unmodifiableCollection(sanctionedClasses);
    this.api = api;
  }

  @Override
  public void setFilter() {
    try {
      // create the ObjectInputFilter to set as the global serial filter
      Object objectInputFilter = api.createObjectInputFilterProxy(pattern, sanctionedClasses);

      // set the global serial filter
      api.setSerialFilter(objectInputFilter);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Geode was unable to configure a global serialization filter",
          e);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DelegatingGlobalSerialFilter{");
    sb.append("api=").append(api);
    sb.append(", pattern='").append(pattern).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
