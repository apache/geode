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
package org.apache.geode.internal.protocol.protobuf.v1;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.state.ProtobufConnectionStateProcessor;

@Experimental
public abstract class MessageExecutionContext {
  protected final ClientStatistics statistics;
  protected ProtobufConnectionStateProcessor protobufConnectionStateProcessor;

  public MessageExecutionContext(ClientStatistics statistics,
      ProtobufConnectionStateProcessor protobufConnectionStateProcessor) {
    this.statistics = statistics;
    this.protobufConnectionStateProcessor = protobufConnectionStateProcessor;
  }

  public ProtobufConnectionStateProcessor getConnectionStateProcessor() {
    return protobufConnectionStateProcessor;
  }

  public abstract InternalCache getCache() throws InvalidExecutionContextException;

  public abstract Locator getLocator() throws InvalidExecutionContextException;

  /**
   * Returns the statistics for recording operation stats. In a unit test environment this may not
   * be a protocol-specific statistics implementation.
   */
  public ClientStatistics getStatistics() {
    return statistics;
  }

  public void setConnectionStateProcessor(
      ProtobufConnectionStateProcessor protobufConnectionStateProcessor) {
    this.protobufConnectionStateProcessor = protobufConnectionStateProcessor;
  }
}
