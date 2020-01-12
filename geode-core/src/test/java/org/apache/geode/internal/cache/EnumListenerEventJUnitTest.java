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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class EnumListenerEventJUnitTest {

  /**
   * tests whether EnumListenerEvent.getEnumListenerEvent(int cCode) returns the right result
   */
  @Test
  public void testGetEnumListEvent() {
    checkAndAssert(0, null);
    checkAndAssert(1, EnumListenerEvent.AFTER_CREATE);
    checkAndAssert(2, EnumListenerEvent.AFTER_UPDATE);
    checkAndAssert(3, EnumListenerEvent.AFTER_INVALIDATE);
    checkAndAssert(4, EnumListenerEvent.AFTER_DESTROY);
    checkAndAssert(5, EnumListenerEvent.AFTER_REGION_CREATE);
    checkAndAssert(6, EnumListenerEvent.AFTER_REGION_INVALIDATE);
    checkAndAssert(7, EnumListenerEvent.AFTER_REGION_CLEAR);
    checkAndAssert(8, EnumListenerEvent.AFTER_REGION_DESTROY);
    checkAndAssert(9, EnumListenerEvent.AFTER_REMOTE_REGION_CREATE);
    checkAndAssert(10, EnumListenerEvent.AFTER_REMOTE_REGION_DEPARTURE);
    checkAndAssert(11, EnumListenerEvent.AFTER_REMOTE_REGION_CRASH);
    checkAndAssert(12, EnumListenerEvent.AFTER_ROLE_GAIN);
    checkAndAssert(13, EnumListenerEvent.AFTER_ROLE_LOSS);
    checkAndAssert(14, EnumListenerEvent.AFTER_REGION_LIVE);
    checkAndAssert(15, EnumListenerEvent.AFTER_REGISTER_INSTANTIATOR);
    checkAndAssert(16, EnumListenerEvent.AFTER_REGISTER_DATASERIALIZER);
    checkAndAssert(17, EnumListenerEvent.AFTER_TOMBSTONE_EXPIRATION);
    // extra non-existent code checks as a markers so that this test will
    // fail if further events are added (0th or +1 codes) without updating this test
    checkAndAssert(18, EnumListenerEvent.TIMESTAMP_UPDATE);
    checkAndAssert(19, null);
  }

  // check that the code and object both match
  private void checkAndAssert(int code, EnumListenerEvent event) {
    EnumListenerEvent localEvent = EnumListenerEvent.getEnumListenerEvent(code);
    assertTrue(localEvent == event);
    if (localEvent != null) {
      assertTrue(localEvent.getEventCode() == code);
    }
  }
}
