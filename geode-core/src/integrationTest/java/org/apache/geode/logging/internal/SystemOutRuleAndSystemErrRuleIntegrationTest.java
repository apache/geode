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
package org.apache.geode.logging.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintStream;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Verifies behavior of {@code SystemOutRule} and {@code SystemErrRule} that other Geode logging
 * tests depend on. If this behavior changes, then those tests may also need to change.
 */
@Category(LoggingTest.class)
public class SystemOutRuleAndSystemErrRuleIntegrationTest {

  private static PrintStream systemOutBeforeRule;
  private static PrintStream systemErrBeforeRule;

  private PrintStream systemOutAfterRule;
  private PrintStream systemErrAfterRule;
  private String message;

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @BeforeClass
  public static void beforeClass() {
    systemOutBeforeRule = System.out;
    systemErrBeforeRule = System.err;
  }

  @Before
  public void setUp() {
    systemOutAfterRule = System.out;
    systemErrAfterRule = System.err;
    message = "simple message";
  }

  @AfterClass
  public static void afterClass() {
    systemOutBeforeRule = null;
    systemErrBeforeRule = null;
  }

  @Test
  public void ruleDoesNotReceiveFromSystemOutBeforeRule() {
    systemOutBeforeRule.println(message);

    assertThat(systemOutRule.getLog()).doesNotContain(message);
  }

  @Test
  public void ruleDoesReceiveFromSystemOutAfterRule() {
    systemOutAfterRule.println(message);

    assertThat(systemOutRule.getLog()).contains(message);
  }

  @Test
  public void ruleDoesNotReceiveFromSystemErrBeforeRule() {
    systemErrBeforeRule.println(message);

    assertThat(systemErrRule.getLog()).doesNotContain(message);
  }

  @Test
  public void ruleDoesReceiveFromSystemErrAfterRule() {
    systemErrAfterRule.println(message);

    assertThat(systemErrRule.getLog()).contains(message);
  }
}
