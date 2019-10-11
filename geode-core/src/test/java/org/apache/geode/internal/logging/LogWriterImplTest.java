/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@code LogWriterImpl}.
 */
@Category(LoggingTest.class)
@SuppressWarnings("deprecation")
public class LogWriterImplTest {

  @Test
  public void testAllowedLogLevels() {
    assertThat(LogWriterImpl.allowedLogLevels())
        .isEqualTo("all|finest|finer|fine|config|info|warning|error|severe|none");
  }

  @Test
  public void testLevelNameToCode() {
    assertThat(LogWriterImpl.levelNameToCode("all")).isEqualTo(Integer.MIN_VALUE);
    assertThat(LogWriterImpl.levelNameToCode("finest")).isEqualTo(300);
    assertThat(LogWriterImpl.levelNameToCode("trace")).isEqualTo(300);
    assertThat(LogWriterImpl.levelNameToCode("finer")).isEqualTo(400);
    assertThat(LogWriterImpl.levelNameToCode("fine")).isEqualTo(500);
    assertThat(LogWriterImpl.levelNameToCode("debug")).isEqualTo(500);
    assertThat(LogWriterImpl.levelNameToCode("config")).isEqualTo(700);
    assertThat(LogWriterImpl.levelNameToCode("info")).isEqualTo(800);
    assertThat(LogWriterImpl.levelNameToCode("warning")).isEqualTo(900);
    assertThat(LogWriterImpl.levelNameToCode("warn")).isEqualTo(900);
    assertThat(LogWriterImpl.levelNameToCode("error")).isEqualTo(950);
    assertThat(LogWriterImpl.levelNameToCode("severe")).isEqualTo(1000);
    assertThat(LogWriterImpl.levelNameToCode("fatal")).isEqualTo(1000);
    assertThat(LogWriterImpl.levelNameToCode("none")).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void testLevelToString() {
    assertThat(LogWriterImpl.levelToString(Integer.MIN_VALUE)).isEqualTo("all");
    assertThat(LogWriterImpl.levelToString(300)).isEqualTo("finest");
    assertThat(LogWriterImpl.levelToString(400)).isEqualTo("finer");
    assertThat(LogWriterImpl.levelToString(500)).isEqualTo("fine");
    assertThat(LogWriterImpl.levelToString(700)).isEqualTo("config");
    assertThat(LogWriterImpl.levelToString(800)).isEqualTo("info");
    assertThat(LogWriterImpl.levelToString(900)).isEqualTo("warning");
    assertThat(LogWriterImpl.levelToString(950)).isEqualTo("error");
    assertThat(LogWriterImpl.levelToString(1000)).isEqualTo("severe");
    assertThat(LogWriterImpl.levelToString(Integer.MAX_VALUE)).isEqualTo("none");

    // everything else adds the prefix level-
    assertThat(LogWriterImpl.levelToString(600)).isEqualTo("level-600");
  }
}
