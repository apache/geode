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
package org.apache.geode.internal.cache.execute;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TimeoutHelperTest {

  @Before
  public void setUp() {

  }

  @Test
  public void toMillisLessThanOneMillisec() {
    assertEquals(1, TimeoutHelper.toMillis(999999, TimeUnit.NANOSECONDS));
    assertEquals(1, TimeoutHelper.toMillis(999, TimeUnit.MICROSECONDS));
  }

  @Test
  public void toMillisGreaterThanMaxIntMillisecs() {
    assertEquals(Integer.MAX_VALUE, TimeoutHelper.toMillis(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
    assertEquals(Integer.MAX_VALUE, TimeoutHelper.toMillis(Long.MAX_VALUE, TimeUnit.SECONDS));
    assertEquals(Integer.MAX_VALUE, TimeoutHelper.toMillis(Integer.MAX_VALUE, TimeUnit.SECONDS));
    assertEquals(Integer.MAX_VALUE, TimeoutHelper.toMillis(100000, TimeUnit.DAYS));
  }

  @Test
  public void toMillisZero() {
    assertEquals(0, TimeoutHelper.toMillis(0, TimeUnit.NANOSECONDS));
    assertEquals(0, TimeoutHelper.toMillis(0, TimeUnit.MICROSECONDS));
    assertEquals(0, TimeoutHelper.toMillis(0, TimeUnit.DAYS));
  }

  @Test
  public void toMillisGreaterThanZero() {
    assertEquals(3, TimeoutHelper.toMillis(3100000, TimeUnit.NANOSECONDS));
    assertEquals(4, TimeoutHelper.toMillis(4100, TimeUnit.MICROSECONDS));
  }
}
