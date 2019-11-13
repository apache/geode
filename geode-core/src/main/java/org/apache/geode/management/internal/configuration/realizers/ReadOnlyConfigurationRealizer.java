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

package org.apache.geode.management.internal.configuration.realizers;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

public abstract class ReadOnlyConfigurationRealizer<T extends AbstractConfiguration<R>, R extends RuntimeInfo>
    implements ConfigurationRealizer<T, R> {
  public final RealizationResult create(T config, InternalCache cache) {
    throw new IllegalStateException("should not be invoked");
  }

  public final RealizationResult update(T config, InternalCache cache) {
    throw new IllegalStateException("should not be invoked");
  }

  public final RealizationResult delete(T config, InternalCache cache) {
    throw new IllegalStateException("should not be invoked");
  }

  public abstract R get(T config, InternalCache cache);

  public final boolean isReadyOnly() {
    return true;
  }
}
