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

import java.util.Properties;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.protobuf.security.SecureCache;
import org.apache.geode.internal.protocol.protobuf.security.SecureLocator;
import org.apache.geode.internal.protocol.protobuf.statistics.ClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.state.ConnectionState;
import org.apache.geode.internal.protocol.protobuf.v1.state.RequireVersion;
import org.apache.geode.internal.protocol.serialization.NoOpCustomValueSerializer;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.protocol.serialization.ValueSerializer;

@Experimental
public abstract class MessageExecutionContext {
  protected final ClientStatistics statistics;
  protected final SecurityService securityService;
  protected ConnectionState connectionState;
  public ProtobufSerializationService serializationService =
      new ProtobufSerializationService(new NoOpCustomValueSerializer());

  public MessageExecutionContext(ClientStatistics statistics, SecurityService securityService) {
    this.securityService = securityService;
    this.statistics = statistics;
    this.connectionState = new RequireVersion(securityService);
  }

  public ConnectionState getConnectionState() {
    return connectionState;
  }

  public ProtobufSerializationService getSerializationService() {
    return serializationService;
  }

  public abstract SecureCache getSecureCache() throws InvalidExecutionContextException;

  public abstract SecureLocator getSecureLocator() throws InvalidExecutionContextException;

  /**
   * Returns the statistics for recording operation stats. In a unit test environment this may not
   * be a protocol-specific statistics implementation.
   */
  public ClientStatistics getStatistics() {
    return statistics;
  }

  public void setState(ConnectionState connectionState) {
    this.connectionState = connectionState;
  }

  public abstract void authenticate(Properties properties);

  public abstract void setValueSerializer(ValueSerializer valueSerializer);
}
