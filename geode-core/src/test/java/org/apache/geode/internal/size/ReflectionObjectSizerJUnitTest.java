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
package org.apache.geode.internal.size;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ReflectionObjectSizerJUnitTest {

  @Test
  public void skipsSizingDistributedSystem() {
    checkSizingSkippedFor(mock(InternalDistributedSystem.class));
  }

  @Test
  public void skipsSizingClassLoader() {
    checkSizingSkippedFor(Thread.currentThread().getContextClassLoader());
  }

  @Test
  public void skipsSizingLogger() {
    checkSizingSkippedFor(LogService.getLogger());
  }

  @Test
  public void skipSizingThread() {
    checkSizingSkippedFor(new Thread(() -> {
    }));
  }

  @Test
  public void skipSizingThreadGroup() {
    checkSizingSkippedFor(new ThreadGroup("test"));
  }

  private void checkSizingSkippedFor(final Object referenceObject) {
    final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();
    final TestObject nullReference = new TestObject(null);
    int sizeWithoutReference = sizer.sizeof(nullReference);
    final TestObject objectReference = new TestObject(referenceObject);
    final TestObject stringReference = new TestObject("hello");

    assertThat(sizer.sizeof(objectReference)).isEqualTo(sizeWithoutReference);
    assertThat(sizer.sizeof(stringReference)).isNotEqualTo(sizeWithoutReference);
  }

  private static class TestObject {
    public TestObject(final Object reference) {
      this.reference = reference;
    }

    Object reference;
  }

}
