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

public class ConnectionTerminatingStateProcessor implements ConnectionStateProcessor {
  @Override
  public void validateOperation(MessageExecutionContext messageContext,
      OperationContext operationContext) throws ConnectionStateException {
    throw new ConnectionStateException(ProtocolErrorCode.GENERIC_FAILURE,
        "This connection has been marked as terminating.");
  }

  @Override
  public boolean socketProcessingIsFinished() {
    return true;
  }
}
