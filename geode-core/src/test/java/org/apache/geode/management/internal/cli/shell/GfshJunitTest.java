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

package org.apache.geode.management.internal.cli.shell;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class GfshJunitTest {
  private String testString;

  @Before
  public void before() {
    testString = "This is a test string.";
  }

  @Test
  public void testWrapTest() {
    assertThat(Gfsh.wrapText(testString, 0, -1)).isEqualTo(testString);
    assertThat(Gfsh.wrapText(testString, 0, 0)).isEqualTo(testString);
    assertThat(Gfsh.wrapText(testString, 0, 1)).isEqualTo(testString);
    assertThat(Gfsh.wrapText(testString, 0, 10))
        .isEqualTo("This is a" + Gfsh.LINE_SEPARATOR + "test" + Gfsh.LINE_SEPARATOR + "string.");
    assertThat(Gfsh.wrapText(testString, 1, 100)).isEqualTo(Gfsh.LINE_INDENT + testString);
    assertThat(Gfsh.wrapText(testString, 2, 100))
        .isEqualTo(Gfsh.LINE_INDENT + Gfsh.LINE_INDENT + testString);
  }

}
