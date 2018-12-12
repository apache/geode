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
package org.apache.geode.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.util.DscodeHelper;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class DSCODETest {
  @Test
  public void testNoDuplicateByteValues() throws Exception {
    Set<Integer> previouslySeen = new HashSet<>();
    for (DSCODE value : DSCODE.values()) {
      final int integerValue = (int) value.toByte();
      assertFalse("Each byte value should only occur with a single header byte enumerate",
          previouslySeen.contains(integerValue));
      previouslySeen.add(integerValue);
    }
  }

  @Test
  public void testGetEnumFromByte() {
    Arrays.stream(DSCODE.values())
        .filter(dscode -> dscode != DSCODE.RESERVED_FOR_FUTURE_USE && dscode != DSCODE.ILLEGAL)
        .forEach(dscode -> {
          try {
            Assert.assertEquals(dscode, DscodeHelper.toDSCODE(dscode.toByte()));
          } catch (IOException e) {
            fail("No exception should have been caught, as the \"error\" codes are filtered out.");
          }
        });
  }
}
