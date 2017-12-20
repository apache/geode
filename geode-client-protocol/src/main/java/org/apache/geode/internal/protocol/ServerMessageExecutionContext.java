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

package org.apache.geode.internal.protocol;


import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.state.ConnectionStateProcessor;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;

@Experimental
public class ServerMessageExecutionContext extends MessageExecutionContext {
  private final Cache cache;

  public ServerMessageExecutionContext(Cache cache, ProtocolClientStatistics statistics,
      ConnectionStateProcessor initialConnectionStateProcessor) {
    super(statistics, initialConnectionStateProcessor);
    this.cache = cache;
  }

  /**
   * Returns the cache associated with this execution
   * <p>
   *
   * @throws InvalidExecutionContextException if there is no cache available
   */
  @Override
  public Cache getCache() throws InvalidExecutionContextException {
    return cache;
  }

  /**
   * Returns the locator associated with this execution
   * <p>
   *
   * @throws InvalidExecutionContextException if there is no locator available
   */
  @Override
  public Locator getLocator() throws InvalidExecutionContextException {
    throw new InvalidExecutionContextException(
        "Operations on the server should not to try to operate on a locator");
  }

}
