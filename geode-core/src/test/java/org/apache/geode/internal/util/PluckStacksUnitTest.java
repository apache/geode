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
package org.apache.geode.internal.util;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class PluckStacksUnitTest {


  @Test
  public void testPluckingStacksFromJVMGeneratedDump() throws Exception {
    LineNumberReader reader = new LineNumberReader(
        new FileReader(createTempFileFromResource(getClass(), "PluckStacksJstackGeneratedDump.txt")
            .getAbsolutePath()));

    Map<String, List<PluckStacks.ThreadStack>> dumps =
        new PluckStacks().getThreadDumps(reader, "PluckStacksSystemGeneratedDump.txt");
    assertEquals(3, dumps.size());
    // there should be thread stacks in the list
    assertNotEquals(0, dumps.values().iterator().next().size());
  }


}
