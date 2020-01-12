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
package org.apache.geode.internal.cache.versions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.io.DataOutput;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;


public class RVVExceptionJUnitTest {

  @Test
  public void testRVVExceptionB() {
    RVVExceptionB ex = new RVVExceptionB(5, 10);
    ex.add(8);
    ex.add(6);
    assertEquals(8, ex.getHighestReceivedVersion());
    ex.add(5);
    assertEquals(8, ex.getHighestReceivedVersion());
  }

  @Test
  public void testRVVExceptionBOutput() throws Exception {
    testExceptionOutput(new RVVExceptionB(50, 100));
  }

  @Test
  public void testRVVExceptionT() {
    RVVExceptionT ex = new RVVExceptionT(5, 10);
    ex.add(8);
    ex.add(6);
    assertEquals(8, ex.getHighestReceivedVersion());
  }

  @Test
  public void testRVVExceptionTOutput() throws Exception {
    testExceptionOutput(new RVVExceptionT(50, 100));
  }

  // Exception is expected to be initialized with (50, 100)
  private void testExceptionOutput(RVVException ex) throws Exception {
    ex.add(60);
    ex.add(85);
    ex.add(70);
    ex.add(72);
    ex.add(74);
    ex.add(73);

    DataOutput mockOutput = mock(DataOutput.class);
    InOrder inOrder = Mockito.inOrder(mockOutput);
    ex.toData(mockOutput);

    inOrder.verify(mockOutput).writeByte(50); // prev = 50
    inOrder.verify(mockOutput).writeByte(6); // 6 received versions
    inOrder.verify(mockOutput, times(2)).writeByte(10); // 60, 70
    inOrder.verify(mockOutput).writeByte(2); // 72
    inOrder.verify(mockOutput, times(2)).writeByte(1); // 73, 74
    inOrder.verify(mockOutput).writeByte(11); // 85
    inOrder.verify(mockOutput).writeByte(15); // 100
    inOrder.verifyNoMoreInteractions();
  }
}
