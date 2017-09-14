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
public class MessageExecutionContext {
  private final Cache cache;
  private final Locator locator;
  private final ProtocolClientStatistics statistics;
  private ConnectionStateProcessor connectionStateProcessor;

  public MessageExecutionContext(Cache cache, ProtocolClientStatistics statistics,
      ConnectionStateProcessor initialConnectionStateProcessor) {
    this.cache = cache;
    this.locator = null;
    this.statistics = statistics;
    this.connectionStateProcessor = initialConnectionStateProcessor;
  }

  public MessageExecutionContext(InternalLocator locator, ProtocolClientStatistics statistics,
      ConnectionStateProcessor initialConnectionStateProcessor) {
    this.locator = locator;
    this.cache = null;
    this.statistics = statistics;
    connectionStateProcessor = initialConnectionStateProcessor;
  }

  public ConnectionStateProcessor getConnectionStateProcessor() {
    return connectionStateProcessor;
  }

  /**
   * Returns the cache associated with this execution
   * <p>
   *
   * @throws InvalidExecutionContextException if there is no cache available
   */
  public Cache getCache() throws InvalidExecutionContextException {
    if (cache != null) {
      return cache;
    }
    throw new InvalidExecutionContextException(
        "Operations on the locator should not to try to operate on a cache");
  }

  /**
   * Returns the locator associated with this execution
   * <p>
   *
   * @throws InvalidExecutionContextException if there is no locator available
   */
  public Locator getLocator() throws InvalidExecutionContextException {
    if (locator != null) {
      return locator;
    }
    throw new InvalidExecutionContextException(
        "Operations on the server should not to try to operate on a locator");
  }

  /**
   * Returns the statistics for recording operation stats. In a unit test environment this may not
   * be a protocol-specific statistics implementation.
   */
  public ProtocolClientStatistics getStatistics() {
    return statistics;
  }

  public void setConnectionStateProcessor(ConnectionStateProcessor connectionStateProcessor) {
    this.connectionStateProcessor = connectionStateProcessor;
  }
}
