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
package org.apache.geode.internal.protocol.state;

import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.protocol.OperationContext;
import org.apache.geode.internal.protocol.ProtocolErrorCode;
import org.apache.geode.internal.protocol.state.exception.ConnectionStateException;

/**
 * This object encapsulates any operation processing that is specific to the current state of a
 * protocol connection. It is used to inject behavior at specific points in the processing of
 * protocol operations as opposed to requiring an explicit state machine to drive the state
 * transitions from outside the protocol.
 */
public interface ConnectionStateProcessor {
  /**
   * @throws ConnectionStateException if incapable of handling the given operationContext when the
   *         connection is in the state contained in the provided messageContext. Otherwise, does
   *         nothing.
   */
  void validateOperation(MessageExecutionContext messageContext, OperationContext operationContext)
      throws ConnectionStateException;

  /**
   * This indicates whether this specific state processor is able to handle authentication requests.
   *
   * @return specialized ConnectionAuthenticatingStateProcessor interface implementation which can
   *         move to a new state
   * @throws ConnectionStateException if unable to handle handshakes in this state.
   */
  default ConnectionAuthenticatingStateProcessor allowAuthentication()
      throws ConnectionStateException {
    throw new ConnectionStateException(ProtocolErrorCode.UNSUPPORTED_OPERATION,
        "Requested operation not allowed at this time");
  }

  /**
   * This indicates whether this specific state processor is able to handle handshake requests.
   *
   * @return specialized ConnectionHandshakingStateProcessor interface implementation which can move
   *         to a new state
   * @throws ConnectionStateException if unable to handle handshakes in this state.
   */
  default ConnectionHandshakingStateProcessor allowHandshake() throws ConnectionStateException {
    throw new ConnectionStateException(ProtocolErrorCode.UNSUPPORTED_OPERATION,
        "Requested operation not allowed at this time");
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
