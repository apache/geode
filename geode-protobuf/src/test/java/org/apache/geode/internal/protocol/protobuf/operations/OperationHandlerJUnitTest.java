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
package org.apache.geode.internal.protocol.protobuf.operations;

import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.protocol.operations.OperationHandler;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OperationHandlerJUnitTest {
  protected Cache cacheStub;
  protected SerializationService serializationServiceStub;
  protected OperationHandler operationHandler;

  @Before
  public void setUp() throws Exception {
    cacheStub = mock(Cache.class);
    serializationServiceStub = mock(SerializationService.class);
  }
}
