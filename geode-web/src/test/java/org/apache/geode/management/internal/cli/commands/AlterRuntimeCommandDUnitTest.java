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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class AlterRuntimeCommandDUnitTest {
  @Rule
  public LocatorServerStartupRule startupRule =
      new LocatorServerStartupRule().withTempWorkingDir().withLogFile();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  @Parameters({"true", "false"})
  public void testAlterRuntimeConfig(final boolean connectOverHttp) throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"log-disk-space-limit\"");

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM server0 = startupRule.startServerAsJmxManager(0, props);

    if (connectOverHttp) {
      gfsh.connectAndVerify(server0.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server0.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.MEMBER, server0.getName());
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, "info");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE,
        temporaryFolder.newFile("stats.gfs"));
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "true");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndVerifyCommand(csb.toString());

    server0.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogLevel()).isEqualTo(LogWriterImpl.INFO_LEVEL);
      assertThat(config.getLogFileSizeLimit()).isEqualTo(50);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(32);
      assertThat(config.getStatisticSampleRate()).isEqualTo(2000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("stats.gfs");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
    });

    CommandResult result = gfsh.executeCommand("alter runtime");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    result = gfsh.executeCommand("alter runtime  --log-disk-space-limit=2000000000");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"log-disk-space-limit\" to \"2,000,000,000\"");
  }

  @Test
  @Parameters({"true", "false"})
  public void alterRuntimeConfig_logDiskSpaceLimitWithFileSizeLimitNotSet_OK(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator = startupRule.startLocatorVM(0, props);
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndVerifyCommand(csb.toString());
    String resultStr = gfsh.getGfshOutput();

    server1.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
    });
    server2.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void alterRuntimeConfig_logDiskSpaceLimitWithFileSizeLimitSet_OK(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator = startupRule.startLocatorVM(0, props);
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    CommandStringBuilder csbSetFileSizeLimit =
        new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csbSetFileSizeLimit.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    gfsh.executeAndVerifyCommand(csbSetFileSizeLimit.toString());

    server2.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(50);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
    });

    CommandStringBuilder csbSetDiskSpaceLimit =
        new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csbSetDiskSpaceLimit.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    csbSetDiskSpaceLimit.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndVerifyCommand(csbSetDiskSpaceLimit.toString());
    String resultStr = gfsh.getGfshOutput();

    server1.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(50);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
    server2.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(50);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void alterRuntimeConfig_logDiskSpaceLimitOnMember_OK(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator = startupRule.startLocatorVM(0, props);
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.MEMBERS, server1.getName());
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndVerifyCommand(csb.toString());
    String resultStr = gfsh.getGfshOutput();

    server1.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
    server2.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void alterRuntimeConfig_logDiskSpaceLimitOnGroup_OK(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator = startupRule.startLocatorVM(0, props);
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.GROUPS, "G1");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndVerifyCommand(csb.toString());
    String resultStr = gfsh.getGfshOutput();

    server1.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getGroups()).isEqualTo("");
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
    server2.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getGroups()).isEqualTo("G1");
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
  }

  /**
   * Test to verify that when 'alter runtime' without relevant options does not change the server's
   * configuration
   */
  @Test
  @Parameters({"true", "false"})
  public void alterRuntimeConfig_groupWithoutOptions_needsRelevantParameter(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator = startupRule.startLocatorVM(0, props);
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    server2.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getGroups()).isEqualTo("G1");
    });

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.GROUPS, "G1");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    server1.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
  }

  /**
   * Test to verify that when 'alter runtime' without relevant options does not change the server's
   * configuration
   */
  @Test
  @Parameters({"true", "false"})
  public void alterRuntimeConfig_memberWithoutOptions_needsRelevantParameter(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator = startupRule.startLocatorVM(0, props);
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.MEMBERS, server1.getName());

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    server1.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
      assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled()).isTrue();
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void testAlterUpdatesSharedConfig(final boolean connectOverHttp) throws Exception {
    MemberVM locator = startupRule.startLocatorVM(0);

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    Properties props = new Properties();
    props.setProperty(GROUPS, "Group1");
    props.setProperty(LOG_LEVEL, "error");
    startupRule.startServerVM(1, props, locator.getPort());

    String command = "alter runtime --group=Group1 --log-level=fine";
    gfsh.executeAndVerifyCommand(command);

    locator.invoke(() -> {
      ClusterConfigurationService sharedConfig =
          LocatorServerStartupRule.locatorStarter.getLocator().getSharedConfiguration();
      Properties properties = sharedConfig.getConfiguration("Group1").getGemfireProperties();
      assertThat(properties.get(LOG_LEVEL)).isEqualTo("fine");
    });
  }
}
