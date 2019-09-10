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
package org.apache.geode.management.internal.beans;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class AsyncEventQueueMBeanTest {

  private AsyncEventQueueMBean asyncEventQueueMBean;

  private AsyncEventQueueMBeanBridge asyncEventQueueMBeanBridge;

  @Test
  public void asyncEvenQueueCreatedWithDispatcherNotPaused() {
    asyncEventQueueMBeanBridge = mock(AsyncEventQueueMBeanBridge.class);
    asyncEventQueueMBean = new AsyncEventQueueMBean(asyncEventQueueMBeanBridge);

    when(asyncEventQueueMBeanBridge.isDispatchingPaused()).thenReturn(false);

    assertThat(asyncEventQueueMBean.isDispatchingPaused()).isFalse();
  }

  @Test
  public void asyncEvenQueueCreatedWithDispatcherPaused() {
    asyncEventQueueMBeanBridge = mock(AsyncEventQueueMBeanBridge.class);
    asyncEventQueueMBean = new AsyncEventQueueMBean(asyncEventQueueMBeanBridge);

    when(asyncEventQueueMBeanBridge.isDispatchingPaused()).thenReturn(true);

    assertThat(asyncEventQueueMBean.isDispatchingPaused()).isTrue();
  }
}
