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
package org.apache.geode.protocol.operations.registry;

import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerAlreadyRegisteredException;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerNotRegisteredException;

import java.util.HashMap;

/**
 * This class tracks which operation handlers are expected to handle which types of operations.
 */
public class OperationsHandlerRegistry {
  private HashMap<Integer, OperationHandler> registeredOperations = new HashMap<>();

  public OperationHandler getOperationHandlerForOperationId(int operationCode)
      throws OperationHandlerNotRegisteredException {
    OperationHandler operationHandler = registeredOperations.get(operationCode);
    if (operationHandler == null) {
      throw new OperationHandlerNotRegisteredException(
          "There is no operation handler registered for operation code: " + operationCode);
    }
    return operationHandler;
  }

  public synchronized void registerOperationHandlerForOperationId(int operationCode,
      OperationHandler operationHandler) throws OperationHandlerAlreadyRegisteredException {
    if (registeredOperations.containsKey(operationCode)) {
      throw new OperationHandlerAlreadyRegisteredException(
          "An operation handler for operationCode: " + operationCode
              + " has already been registered!");
    }
    registeredOperations.put(operationCode, operationHandler);
  }

  public int getRegisteredOperationHandlersCount() {
    return registeredOperations.size();
  }
}
