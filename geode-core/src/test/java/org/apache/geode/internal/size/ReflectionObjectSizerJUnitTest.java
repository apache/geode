/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.size;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ReflectionObjectSizerJUnitTest {

  @Test
  public void skipsSizingDistributedSystem() {
    Object referenceObject = mock(InternalDistributedSystem.class);
    checkSizeDoesNotChange(referenceObject);
  }

  @Test
  public void skipsSizingClassLoader() {
    checkSizeDoesNotChange(Thread.currentThread().getContextClassLoader());
  }

  @Test
  public void skipsSizingLogger() {
    checkSizeDoesNotChange(LogService.getLogger());
  }

  private void checkSizeDoesNotChange(final Object referenceObject) {
    final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();
    final TestObject nullReference = new TestObject(null);
    int sizeWithoutReference = sizer.sizeof(nullReference);
    final TestObject distributedSystemReference = new TestObject(referenceObject);
    final TestObject stringReference = new TestObject("hello");

    assertEquals(sizeWithoutReference, sizer.sizeof(distributedSystemReference));
    assertNotEquals(sizeWithoutReference, sizer.sizeof(stringReference));
  }

  private static class TestObject {

    public TestObject(final Object reference) {
      this.reference = reference;
    }

    Object reference = null;
  }

}
