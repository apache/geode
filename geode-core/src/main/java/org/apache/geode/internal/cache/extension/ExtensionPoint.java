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

/**
 * Allows {@link Extensible} objects to add and remove {@link Extension}s.
 * 
 *
 * @since GemFire 8.1
 */
public interface ExtensionPoint<T> {

  /**
   * Add {@link Extension} to {@link ExtensionPoint}.
   * 
   * @param extension
   *          to add.
   * @since GemFire 8.1
   */
  void addExtension(Extension<T> extension);

  /**
   * Remove {@link Extension} from {@link ExtensionPoint}.
   * 
   * @param extension
   *          to remove.
   * @since GemFire 8.1
   */
  void removeExtension(Extension<T> extension);

  /**
   * Get {@link Iterable} of {@link Extension}s.
   * 
   * @return {@link Exception}s
   * @since GemFire 8.1
   */
  Iterable<Extension<T>> getExtensions();

  /**
   * Helper method to get appropriately typed access to target
   * {@link Extensible} object.
   * 
   * @return {@link Extensible} object target.
   * 
   * @since GemFire 8.1
   */
  T getTarget();

}
