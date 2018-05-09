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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Contains simple tests for the {@link
 * org.apache.geode.distributed.internal.ThreadMonitoringUtils}.
 *
 * @since Geode 1.5
 */
@Category({UnitTest.class})
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest(InternalDistributedSystem.class)
public class ThreadMonitoringUtilsJUnitTest {

  /**
   * Tests that in case no instance of internal distribution system exists dummy instance is used
   */
  @Test
  public void testDummyInstanceUsed() {

    PowerMockito.mockStatic(InternalDistributedSystem.class);
    PowerMockito.when(InternalDistributedSystem.getAnyInstance()).thenReturn(null);

    assertTrue(ThreadMonitoringUtils.getThreadMonitorObj() instanceof ThreadMonitoringImplDummy);
  }
}
