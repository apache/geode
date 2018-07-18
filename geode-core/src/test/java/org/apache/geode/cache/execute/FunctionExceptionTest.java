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
package org.apache.geode.cache.execute;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;


public class FunctionExceptionTest {

  @Test
  public void shouldBeMockable() throws Exception {
    FunctionException mockFunctionException = mock(FunctionException.class);
    Exception cause = new Exception();
    List<Exception> exceptions = new ArrayList<>();
    exceptions.add(cause);

    when(mockFunctionException.getExceptions()).thenReturn(Collections.emptyList());

    mockFunctionException.addException(cause);
    mockFunctionException.addExceptions(exceptions);

    verify(mockFunctionException, times(1)).addException(cause);
    verify(mockFunctionException, times(1)).addExceptions(exceptions);

    assertThat(mockFunctionException.getExceptions()).isEmpty();
  }
}
