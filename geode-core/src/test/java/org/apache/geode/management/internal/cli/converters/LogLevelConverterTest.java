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
package org.apache.geode.management.internal.cli.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class LogLevelConverterTest {

  @Test
  public void testCompletionContainsOnlyLog4jLevels() {
    LogLevelConverter converter = new LogLevelConverter();
    List<Completion> completions = new ArrayList<>();

    converter.getAllPossibleValues(completions, null, null, null, null);

    assertThat(completions.size()).isEqualTo(8);

    for (Completion completion : completions) {
      String level = completion.getValue();
      assertThat(Level.getLevel(level)).isNotNull();
    }
  }
}
