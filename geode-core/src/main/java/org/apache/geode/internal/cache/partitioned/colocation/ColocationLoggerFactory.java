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
package org.apache.geode.internal.cache.partitioned.colocation;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.internal.cache.PartitionedRegion;

@FunctionalInterface
public interface ColocationLoggerFactory {

  @VisibleForTesting
  String COLOCATION_LOGGER_FACTORY_PROPERTY = "geode.ColocationLoggerFactory";

  static ColocationLoggerFactory create() {
    try {
      String className = System.getProperty(COLOCATION_LOGGER_FACTORY_PROPERTY,
          SingleThreadColocationLoggerFactory.class.getName());
      return (ColocationLoggerFactory) ClassPathLoader.getLatest().forName(className).newInstance();
    } catch (Exception e) {
      return new SingleThreadColocationLoggerFactory();
    }
  }

  /**
   * Returns a newly started {@code ColocationLogger}.
   */
  ColocationLogger startColocationLogger(PartitionedRegion region);
}
