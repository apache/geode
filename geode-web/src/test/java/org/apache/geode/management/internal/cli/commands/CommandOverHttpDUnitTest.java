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

import org.junit.ClassRule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runner.SuiteRunner;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(SuiteRunner.class)
@Suite.SuiteClasses({ConfigCommandsDUnitTest.class, DeployCommandsDUnitTest.class,
    DiskStoreCommandsDUnitTest.class, FunctionCommandsDUnitTest.class,
    GemfireDataCommandsDUnitTest.class,
    GetCommandOnRegionWithCacheLoaderDuringCacheMissDUnitTest.class, IndexCommandsDUnitTest.class,
    ListAndDescribeDiskStoreCommandsDUnitTest.class, ListIndexCommandDUnitTest.class,
    MemberCommandsDUnitTest.class, MiscellaneousCommandsDUnitTest.class,
    MiscellaneousCommandsExportLogsPart1DUnitTest.class,
    MiscellaneousCommandsExportLogsPart2DUnitTest.class,
    MiscellaneousCommandsExportLogsPart3DUnitTest.class,
    MiscellaneousCommandsExportLogsPart4DUnitTest.class, QueueCommandsDUnitTest.class,
    SharedConfigurationCommandsDUnitTest.class, ShellCommandsDUnitTest.class,
    ShowDeadlockDUnitTest.class, ShowMetricsDUnitTest.class, ShowStackTraceDUnitTest.class,
    UserCommandsDUnitTest.class})
public class CommandOverHttpDUnitTest {
  @ClassRule
  public static ProvideSystemProperty provideSystemProperty =
      new ProvideSystemProperty(CliCommandTestBase.USE_HTTP_SYSTEM_PROPERTY, "true");
}
