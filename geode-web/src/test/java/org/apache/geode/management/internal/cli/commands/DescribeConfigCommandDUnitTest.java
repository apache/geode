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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class DescribeConfigCommandDUnitTest {
  @Rule
  public LocatorServerStartupRule startupRule =
      new LocatorServerStartupRule().withTempWorkingDir().withLogFile();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  @Parameters({"true", "false"})
  public void testDescribeConfig(final boolean connectOverHttp) throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    localProps.setProperty(ENABLE_TIME_STATISTICS, "true");
    localProps.setProperty(GROUPS, "G1");
    MemberVM server0 = startupRule.startServerAsJmxManager(0, localProps);

    if (connectOverHttp) {
      gfsh.connectAndVerify(server0.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server0.getJmxPort(), GfshShellConnectionRule.PortType.jmxManager);
    }

    server0.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      InternalDistributedSystem system = cache.getInternalDistributedSystem();
      DistributionConfig config = system.getConfig();
      config.setArchiveFileSizeLimit(1000);
    });

    gfsh.executeAndVerifyCommand("describe config --member=" + server0.getName());
    String result = gfsh.getGfshOutput();

    assertThat(result).containsPattern("enable-time-statistics\\s+: true");
    assertThat(result).containsPattern("groups\\s+: G1");
    assertThat(result).containsPattern("archive-file-size-limit\\s+: 1000");
    assertThat(result).containsPattern("name\\s+: server-0");
    assertThat(result).containsPattern("is-server\\s+: true");
    assertThat(result).doesNotContain("copy-on-read");

    gfsh.executeAndVerifyCommand(
        "describe config --member=" + server0.getName() + " --hide-defaults=false");
    result = gfsh.getGfshOutput();
    assertThat(result).contains("copy-on-read");
  }
}
