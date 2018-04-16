/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.FileInputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.util.test.TestUtil;

@Category(UnitTest.class)
public class FunctionAdapterJUnitTest {

  private static final long serialVersionUID = 1L;
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
    public void execute(final FunctionContext context) {}

  }

  @Test
  public void deserializePreGeodeFunctionAdapterShouldNotThrowIncompatibleException()
      throws Exception {
    FileInputStream fis =
        new FileInputStream(TestUtil.getResourcePath(getClass(), getClass().getSimpleName() + "."
            + "serializedFunctionAdapterWithDifferentSerialVersionUID.ser"));

    DataInputStream dis = new VersionedDataInputStream(new DataInputStream(fis), Version.GFE_82);
    Object o = InternalDataSerializer.basicReadObject(dis);
    assertTrue(o instanceof FunctionAdapter);
  }

  private static class SomeFunction extends FunctionAdapter {

    private static final long serialVersionUID = -6417837315839543937L;

    @Override
    public void execute(FunctionContext context) {
      context.getResultSender().lastResult("S");
    }

    @Override
    public String getId() {
      return "I";
    }

    public boolean equals(Object o) {
      if (o instanceof FunctionAdapter) {
        return ((FunctionAdapter) o).getId().equals(("I"));
      }
      return false;
    }
  }

}
