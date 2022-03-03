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

import java.util.Set;

/**
 * Creates an instance of {@code ObjectInputFilter} that delegates to {@code ObjectInputFilterApi}
 * to maintain independence from the JRE version.
 */
public class ReflectiveFacadeStreamSerialFilterFactory implements StreamSerialFilterFactory {

  @Override
  public StreamSerialFilter create(SerializableObjectConfig config, Set<String> sanctionedClasses) {
    ObjectInputFilterApi api =
        new ReflectiveObjectInputFilterApiFactory().createObjectInputFilterApi();

    if (config.getValidateSerializableObjects()) {
      String pattern = new SanctionedSerializablesFilterPattern()
          .append(config.getSerializableObjectFilter())
          .pattern();

      return new ReflectiveFacadeStreamSerialFilter(api, pattern, sanctionedClasses);
    }
    return new NullStreamSerialFilter();
  }
}
