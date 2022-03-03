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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;


public class LineWrapUnitJUnitTest {

  @Test
  public void test() {
    String test = "aaa aaaaa  aaa aaaa";

    assertEquals(list("aaa", "aaaaa", "aaa", "aaaa"), SystemAdmin.lineWrapOut(test, 3));
    assertEquals(list("aaa", "aaaaa", "aaa aaaa"), SystemAdmin.lineWrapOut(test, 8));

    assertEquals(list("aaa aaaaa", "aaa aaaa"), SystemAdmin.lineWrapOut(test, 9));
    assertEquals(list("aaa aaaaa  aaa", "aaaa"), SystemAdmin.lineWrapOut(test, 14));

    String test2 = "aaa\n aaaaa  aaa aaaa";
    assertEquals(list("aaa", " aaaaa  aaa", "aaaa"), SystemAdmin.lineWrapOut(test2, 14));
  }

  private List<String> list(String... strings) {
    return Arrays.asList(strings);
  }

}
