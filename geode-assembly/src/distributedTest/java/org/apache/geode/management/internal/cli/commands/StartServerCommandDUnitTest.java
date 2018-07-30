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
package org.apache.geode.management.internal.cli.commands;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StartServerCommandDUnitTest {
  private static MemberVM locator;
  private static String locatorConnectionString;

  @ClassRule
  public static final ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static final GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void before() {
    // locator used to clean up members started during tests
    locator = cluster.startLocatorVM(0, 0);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";
  }

  @Test
  public void testWithMissingCacheXml() {}

  @Test
  public void testWithMissingGemFirePropertiesFile() {}

  @Test
  public void testWithMissingPassword() {}

  @Test
  public void testWithMissingSecurityPropertiesFile() {}

  @Test
  public void testWithUnavailablePort() {}

  @Test
  public void testWithMissingStartDirectory() {}

  @Test
  public void testWithRelativeStartDirectory() {}

  @Test
  public void testWithConflictingPIDFile() {}

  @Test
  public void testWithForceOverwriteConflictingPIDFile() {}

  @Test
  public void testWithConnectionToLocator() {}
}
