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
package com.gemstone.gemfire.cache.execute;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class FunctionAdapterJUnitTest {

  private FunctionAdapter adapter;

  @Before
  public void createDefaultAdapter() {
    adapter = new MyFunctionAdapter();
  }

  @Test
  public void optimizeForWriteDefaultsFalse() {
    assertFalse(adapter.optimizeForWrite());
  }

  @Test
  public void idDefaultsToClassName() {
    assertEquals(MyFunctionAdapter.class.getCanonicalName(), adapter.getId());
  }

  @Test
  public void hasResultDefaultsTrue() {
    assertTrue(adapter.hasResult());

  }

  @Test
  public void isHADefaultsTrue() {
    assertTrue(adapter.isHA());
  }

  private static class MyFunctionAdapter extends FunctionAdapter {

    @Override
    public void execute(final FunctionContext context) {
    }

  }
}
