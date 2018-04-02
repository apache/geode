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
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.Authorizer;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingCache;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.AuthorizingLocator;
import org.apache.geode.internal.protocol.protobuf.v1.state.ConnectionState;
import org.apache.geode.internal.protocol.serialization.NoOpCustomValueSerializer;
import org.apache.geode.protocol.serialization.ValueSerializer;

@Experimental
public abstract class MessageExecutionContext {
  protected final ClientStatistics statistics;
  protected ConnectionState connectionState;
  public ProtobufSerializationService serializationService =
      new ProtobufSerializationService(new NoOpCustomValueSerializer());

  public MessageExecutionContext(ClientStatistics statistics, ConnectionState connectionState) {
    this.statistics = statistics;
    this.connectionState = connectionState;
  }

  public ConnectionState getConnectionStateProcessor() {
    return connectionState;
  }

  public ProtobufSerializationService getSerializationService() {
    return serializationService;
  }

  public abstract AuthorizingCache getAuthorizingCache() throws InvalidExecutionContextException;

  public abstract AuthorizingLocator getAuthorizingLocator()
      throws InvalidExecutionContextException;

  /**
   * Returns the statistics for recording operation stats. In a unit test environment this may not
   * be a protocol-specific statistics implementation.
   */
  public ClientStatistics getStatistics() {
    return statistics;
  }

  public void setConnectionStateProcessor(ConnectionState connectionState) {
    this.connectionState = connectionState;
  }

  public abstract void setAuthorizer(Authorizer authorizer);

  public abstract void setValueSerializer(ValueSerializer valueSerializer);
}
