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

import java.util.List;

import org.apache.geode.management.configuration.AutoSerializer;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.internal.CacheElementOperation;

public class PdxValidator implements ConfigurationValidator<Pdx> {
  @Override
  public void validate(CacheElementOperation operation, Pdx pdx) {
    AutoSerializer autoSerializer = pdx.getAutoSerializer();
    if (autoSerializer != null) {
      if (pdx.getPdxSerializer() != null) {
        throw new IllegalArgumentException(
            "Both autoSerializer and pdxSerializer were specified.");
      }
      if (operation == CacheElementOperation.CREATE) {
        List<String> patterns = autoSerializer.getPatterns();
        if (patterns == null || patterns.isEmpty()) {
          throw new IllegalArgumentException(
              "The autoSerializer must have at least one pattern.");
        }
      }
    }
  }
}
