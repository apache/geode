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
package com.gemstone.gemfire.cache.execute;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Category(UnitTest.class)
public class FunctionExceptionTest {

  @Test
  public void shouldBeMockable() throws Exception {
    FunctionException mockFunctionException = mock(FunctionException.class);
    Throwable cause = new Exception("message");
    List<Throwable> listOfThrowables = new ArrayList<>();
    Collection<? extends Throwable> collectionOfThrowables = new ArrayList<>();

    when(mockFunctionException.getExceptions()).thenReturn(listOfThrowables);

    mockFunctionException.addException(cause);
    mockFunctionException.addExceptions(collectionOfThrowables);

    verify(mockFunctionException, times(1)).addException(cause);
    verify(mockFunctionException, times(1)).addExceptions(collectionOfThrowables);

    assertThat(mockFunctionException.getExceptions()).isSameAs(listOfThrowables);
  }
}
