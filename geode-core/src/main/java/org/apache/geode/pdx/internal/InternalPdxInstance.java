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
package org.apache.geode.pdx.internal;

import org.apache.geode.internal.Sendable;
import org.apache.geode.pdx.PdxInstance;

public interface InternalPdxInstance extends PdxInstance, ConvertableToBytes, Sendable {
  /**
   * The same as calling getObject() but may also cache the result and future calls
   * of this method will return the cached object instead of recomputing it.
   * Implementors that do not want to support a cache can just use the default implementation
   * which simply calls getObject().
   */
  default Object getCachedObject() {
    return getObject();
  }
}
