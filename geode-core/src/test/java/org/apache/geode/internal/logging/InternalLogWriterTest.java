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
 * Unit tests for {@code InternalLogWriter}.
 */
@Category(LoggingTest.class)
@SuppressWarnings("deprecation")
public class InternalLogWriterTest {

  @Test
  public void testLevelNames() {
    String[] levelNames = InternalLogWriter.levelNames.toArray(new String[0]);

    assertThat(levelNames[0]).isEqualTo("all");
    assertThat(levelNames[1]).isEqualTo("finest");
    assertThat(levelNames[2]).isEqualTo("finer");
    assertThat(levelNames[3]).isEqualTo("fine");
    assertThat(levelNames[4]).isEqualTo("config");
    assertThat(levelNames[5]).isEqualTo("info");
    assertThat(levelNames[6]).isEqualTo("warning");
    assertThat(levelNames[7]).isEqualTo("error");
    assertThat(levelNames[8]).isEqualTo("severe");
    assertThat(levelNames[9]).isEqualTo("none");

    assertThat(levelNames).hasSize(10);
  }
}
