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
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.codearte.catchexception.shade.mockito.Mockito.mock;
import static org.junit.Assert.*;

@Category(UnitTest.class)
public class OperationsHandlerRegistryJUnitTest {
  public static final int DUMMY_OPERATION_CODE = 999;
  private OperationsHandlerRegistry operationsHandlerRegistry;

  @Before
  public void setup() throws OperationHandlerAlreadyRegisteredException {
    operationsHandlerRegistry = new OperationsHandlerRegistry();
  }

  @Test
  public void testAddOperationsHandlerForOperationType()
      throws OperationHandlerAlreadyRegisteredException {
    int initialHandlerCount = operationsHandlerRegistry.getRegisteredOperationHandlersCount();
    operationsHandlerRegistry.registerOperationHandlerForOperationId(DUMMY_OPERATION_CODE,
        mock(OperationHandler.class));
    assertEquals(initialHandlerCount + 1,
        operationsHandlerRegistry.getRegisteredOperationHandlersCount());
  }

  @Test
  public void testAddingDuplicateOperationsHandlerForOperationType_ThrowsException()
      throws OperationHandlerAlreadyRegisteredException, OperationHandlerNotRegisteredException {
    OperationHandler expectedOperationHandler = mock(OperationHandler.class);
    OperationHandler unexpectedOperationHandler = mock(OperationHandler.class);
    operationsHandlerRegistry.registerOperationHandlerForOperationId(DUMMY_OPERATION_CODE,
        expectedOperationHandler);
    int initialHandlerCount = operationsHandlerRegistry.getRegisteredOperationHandlersCount();
    boolean exceptionCaught = false;
    try {
      operationsHandlerRegistry.registerOperationHandlerForOperationId(DUMMY_OPERATION_CODE,
          unexpectedOperationHandler);
    } catch (OperationHandlerAlreadyRegisteredException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);
    assertEquals(initialHandlerCount,
        operationsHandlerRegistry.getRegisteredOperationHandlersCount());
    assertSame(expectedOperationHandler,
        operationsHandlerRegistry.getOperationHandlerForOperationId(DUMMY_OPERATION_CODE));
  }

  @Test
  public void testGetOperationsHandlerForOperationType()
      throws OperationHandlerAlreadyRegisteredException, OperationHandlerNotRegisteredException {
    OperationHandler expectedOperationHandler = mock(OperationHandler.class);

    operationsHandlerRegistry.registerOperationHandlerForOperationId(DUMMY_OPERATION_CODE,
        expectedOperationHandler);
    OperationHandler operationHandler =
        operationsHandlerRegistry.getOperationHandlerForOperationId(DUMMY_OPERATION_CODE);
    assertSame(expectedOperationHandler, operationHandler);
  }

  @Test
  public void testGetOperationsHandlerForMissingOperationType_ThrowsException() {
    boolean exceptionCaught = false;
    try {
      operationsHandlerRegistry.getOperationHandlerForOperationId(DUMMY_OPERATION_CODE);
    } catch (OperationHandlerNotRegisteredException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);
  }
}
