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
package org.apache.geode.internal.protocol.protobuf.v1.state;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufOperationContext;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;

/**
 * This object encapsulates any operation processing that is specific to the current state of a
 * protobuf connection. It is used to inject behavior at specific points in the processing of
 * protocol operations as opposed to requiring an explicit state machine to drive the state
 * transitions from outside the protocol.
 */
public interface ConnectionState {
  /**
   * @throws ConnectionStateException if incapable of handling the given operationContext when the
   *         connection is in the state contained in the provided messageContext. Otherwise, does
   *         nothing.
   */
  void validateOperation(ProtobufOperationContext operationContext)
      throws ConnectionStateException, DecodingException;

  /**
   * Allow the state processor to take over the entire processing of a given message.
   *
   * @return - True if the message has been handled by the state processor, false to continue normal
   *         processing.
   */
  default boolean handleMessageIndependently(InputStream inputStream, OutputStream outputStream,
      MessageExecutionContext executionContext) throws IOException {
    return false;
  }

  /**
   * This indicates whether this state is capable of receiving any more messages
   *
   * @return True if the socket should be closed
   */
  default boolean socketProcessingIsFinished() {
    return false;
  }
}
