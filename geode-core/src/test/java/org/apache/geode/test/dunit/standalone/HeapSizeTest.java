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
package org.apache.geode.test.dunit.standalone;

import static org.apache.geode.test.dunit.standalone.HeapSize.MAX_HEAP_MB_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class HeapSizeTest {

  private List<String> inputArguments;
  private HeapSize heapSize;

  @Before
  public void setUp() {
    InputArgumentsProvider inputArgumentsProvider = mock(InputArgumentsProvider.class);
    inputArguments = new ArrayList<>();

    when(inputArgumentsProvider.getInputArguments()).thenReturn(inputArguments);

    heapSize = new HeapSize(inputArgumentsProvider);
  }

  @Test
  public void returnsDefaultIfNoInputArgs() {
    String maxHeap = heapSize.getMaxHeapSize();

    assertThat(maxHeap).isEqualTo("" + MAX_HEAP_MB_DEFAULT);
    assertThat(Integer.valueOf(maxHeap)).isEqualTo(768);
  }

  @Test
  public void returnsMegabytesFromInputArgs() {
    inputArguments.add("-Xmx1024m");

    String maxHeap = heapSize.getMaxHeapSize();

    assertThat(maxHeap).isEqualTo(String.valueOf(1024));
    assertThat(Integer.valueOf(maxHeap)).isEqualTo(1024);
  }

  @Test
  public void returnsGigabytesAsMegabytesFromInputArgs() {
    inputArguments.add("-Xmx2g");

    String maxHeap = heapSize.getMaxHeapSize();

    assertThat(maxHeap).isEqualTo(String.valueOf(2 * 1024));
    assertThat(Integer.valueOf(maxHeap)).isEqualTo(2 * 1024);
  }
}
