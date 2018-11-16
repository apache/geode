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
package org.apache.geode.internal.logging;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

@Category(LoggingTest.class)
public class LogWriterImplTest {

  @Test
  public void testAllowedLogLevels() {
    assertEquals("all|finest|finer|fine|config|info|warning|error|severe|none",
        LogWriterImpl.allowedLogLevels());
  }

  @Test
  public void testLevelNames() {
    String[] levelNames = LogWriterImpl.levelNames;
    assertEquals("all", levelNames[0]);
    assertEquals("finest", levelNames[1]);
    assertEquals("finer", levelNames[2]);
    assertEquals("fine", levelNames[3]);
    assertEquals("config", levelNames[4]);
    assertEquals("info", levelNames[5]);
    assertEquals("warning", levelNames[6]);
    assertEquals("error", levelNames[7]);
    assertEquals("severe", levelNames[8]);
    assertEquals("none", levelNames[9]);
    assertEquals(10, levelNames.length);
  }

  @Test
  public void testLevelNameToCode() {
    assertEquals(Integer.MIN_VALUE, LogWriterImpl.levelNameToCode("all"));
    assertEquals(300, LogWriterImpl.levelNameToCode("finest"));
    assertEquals(300, LogWriterImpl.levelNameToCode("trace"));
    assertEquals(400, LogWriterImpl.levelNameToCode("finer"));
    assertEquals(500, LogWriterImpl.levelNameToCode("fine"));
    assertEquals(500, LogWriterImpl.levelNameToCode("debug"));
    assertEquals(700, LogWriterImpl.levelNameToCode("config"));
    assertEquals(800, LogWriterImpl.levelNameToCode("info"));
    assertEquals(900, LogWriterImpl.levelNameToCode("warning"));
    assertEquals(900, LogWriterImpl.levelNameToCode("warn"));
    assertEquals(950, LogWriterImpl.levelNameToCode("error"));
    assertEquals(1000, LogWriterImpl.levelNameToCode("severe"));
    assertEquals(1000, LogWriterImpl.levelNameToCode("fatal"));
    assertEquals(Integer.MAX_VALUE, LogWriterImpl.levelNameToCode("none"));
  }

  @Test
  public void testLevelToString() {
    assertEquals("all", LogWriterImpl.levelToString(Integer.MIN_VALUE));
    assertEquals("finest", LogWriterImpl.levelToString(300));
    assertEquals("finer", LogWriterImpl.levelToString(400));
    assertEquals("fine", LogWriterImpl.levelToString(500));
    assertEquals("config", LogWriterImpl.levelToString(700));
    assertEquals("info", LogWriterImpl.levelToString(800));
    assertEquals("warning", LogWriterImpl.levelToString(900));
    assertEquals("error", LogWriterImpl.levelToString(950));
    assertEquals("severe", LogWriterImpl.levelToString(1000));
    assertEquals("none", LogWriterImpl.levelToString(Integer.MAX_VALUE));
    // everything else...
    assertEquals("level-600", LogWriterImpl.levelToString(600));
  }
}
