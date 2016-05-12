/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.test.junit.rules;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class DiskDirRuleTest {
  @Test
  public void shouldDeleteDirInAfter() throws Throwable {
    DiskDirRule diskDirRule = new DiskDirRule();
    diskDirRule.before();
    final File dir = diskDirRule.get();
    assertTrue(dir.exists());
    final File file1 = new File(dir, "file1");
    final File subdir = new File(dir, "subdir");
    final File file2 = new File(dir, "file2");
    subdir.mkdir();
    Files.write(file1.toPath(), Arrays.asList("abc"));
    Files.write(file2.toPath(), Arrays.asList("stuff"));
    diskDirRule.after();
    assertFalse(dir.exists());
  }
}
