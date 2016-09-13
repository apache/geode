/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.internal.cache.extension;

import org.apache.geode.internal.cache.xmlcache.CacheXml;

/**
 * Provides marker for extensibility and access to {@link ExtensionPoint}.
 * Objects should implement this interface to support modular config and
 * extensions.
 * 
 * Used in {@link CacheXml} to read and write cache xml configurations.
 * 
 *
 * @since GemFire 8.1
 */
public interface Extensible<T> {

  /**
   * Get {@link ExtensionPoint} for this object.
   * 
   * @return {@link ExtensionPoint} for this object.
   * @since GemFire 8.1
   */
  public ExtensionPoint<T> getExtensionPoint();

}
