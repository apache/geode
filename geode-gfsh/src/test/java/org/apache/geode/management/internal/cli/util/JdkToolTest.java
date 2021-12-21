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

package org.apache.geode.management.internal.cli.util;

import static org.junit.Assert.assertEquals;

import java.util.Stack;

import org.junit.Test;

import org.apache.geode.GemFireException;
import org.apache.geode.internal.util.IOUtils;

public class JdkToolTest {

  @Test
  public void testGetJavaPathname() {
    assertEquals(
        IOUtils.appendToPath(System.getProperty("java.home"), "bin",
            "java" + JdkTool.getExecutableSuffix()),
        JdkTool.getJdkToolPathname("java" + JdkTool.getExecutableSuffix(),
            new GemFireException() {}));
  }

  @Test(expected = NullPointerException.class)
  public void testGetJdkToolPathnameWithNullPathnames() {
    try {
      JdkTool.getJdkToolPathname((Stack<String>) null, new GemFireException() {});
    } catch (NullPointerException expected) {
      assertEquals("The JDK tool executable pathnames cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testGetJdkToolPathnameWithNullGemFireException() {
    try {
      JdkTool.getJdkToolPathname(new Stack<>(), null);
    } catch (NullPointerException expected) {
      assertEquals("The GemFireException cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetJdkToolPathnameForNonExistingTool() {
    try {
      final GemFireException expected = new GemFireException() {
        @Override
        public String getMessage() {
          return "expected";
        }
      };

      JdkTool.getJdkToolPathname("nonExistingTool.exe", expected);
    } catch (GemFireException expected) {
      assertEquals("expected", expected.getMessage());
    }
  }
}
