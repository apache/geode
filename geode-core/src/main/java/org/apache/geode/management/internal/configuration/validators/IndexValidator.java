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

package org.apache.geode.management.internal.configuration.validators;

import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.internal.CacheElementOperation;

public class IndexValidator implements ConfigurationValidator<Index> {
  @Override
  public void validate(CacheElementOperation operation, Index config)
      throws IllegalArgumentException {
    if (operation == CacheElementOperation.CREATE) {
      if (config.getName() == null) {
        throw new IllegalArgumentException("Name is required.");
      }

      if (config.getExpression() == null) {
        throw new IllegalArgumentException("Expression is required.");
      }

      if (config.getRegionPath() == null) {
        throw new IllegalArgumentException("RegionPath is required.");
      }

      if (config.getIndexType() == IndexType.HASH_DEPRECATED) {
        throw new IllegalArgumentException("IndexType HASH is not allowed.");
      }
    }
  }
}
