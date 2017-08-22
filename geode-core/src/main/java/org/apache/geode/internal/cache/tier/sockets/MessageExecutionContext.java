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

package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalLocator;

@Experimental
public class MessageExecutionContext {
  private Cache cache;
  private InternalLocator locator;

  public MessageExecutionContext(Cache cache) {
    this.cache = cache;
  }

  public MessageExecutionContext(InternalLocator locator) {
    this.locator = locator;
  }

  // This throws if the cache isn't present because we know that non of the callers can take any
  // reasonable action if the cache is not present
  public Cache getCache() throws InvalidExecutionContextException {
    if (cache != null) {
      return cache;
    } else {
      throw new InvalidExecutionContextException(
          "Operations on the locator should not to try to operate on a cache");
    }
  }

  // This throws if the locator isn't present because we know that non of the callers can take any
  // reasonable action if the locator is not present
  public InternalLocator getLocator() throws InvalidExecutionContextException {
    if (locator != null) {
      return locator;
    } else {
      throw new InvalidExecutionContextException(
          "Operations on the server should not to try to operate on a locator");
    }
  }
}
