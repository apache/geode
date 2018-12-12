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

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class})
@RunWith(JUnitParamsRunner.class)
public class AlterRuntimeCommandDUnitTest {
  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule().withLogFile();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private void verifyDefaultConfig(MemberVM[] servers) {
    for (MemberVM server : servers) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogLevel()).isEqualTo(LogWriterImpl.ERROR_LEVEL);
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testAlterRuntimeConfig(final boolean connectOverHttp) throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"log-disk-space-limit\"");

    MemberVM server0 =
        startupRule.startServerVM(0,
            x -> x.withJMXManager().withHttpService().withProperty(LOG_LEVEL, "error"));

    if (connectOverHttp) {
      gfsh.connectAndVerify(server0.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
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

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    server0.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
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
        .contains("Could not set \"log-disk-space-limit\" to \"2000000000\"");
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitWithFileSizeLimitNotSet_OK(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitWithFileSizeLimitSet_OK(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csbSetFileSizeLimit =
        new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csbSetFileSizeLimit.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    gfsh.executeAndAssertThat(csbSetFileSizeLimit.toString()).statusIsSuccess();

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      assertThat(config.getLogFileSizeLimit()).isEqualTo(50);
      assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
    });

    CommandStringBuilder csbSetDiskSpaceLimit =
        new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csbSetDiskSpaceLimit.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(csbSetDiskSpaceLimit.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(50);
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(10);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitOnMember_OK(final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withProperties(props).withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.MEMBERS, server1.getName());
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit;
      if (server == server1) {
        expectedLimit = 10;
      } else {
        expectedLimit = 0;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(expectedLimit);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitOnGroup_OK(final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 10;
    final String TEST_GROUP = "G1";
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.GROUPS, TEST_GROUP);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT,
        String.valueOf(TEST_LIMIT));

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit;
      String expectedGroup;
      if (server == server2) {
        expectedLimit = TEST_LIMIT;
        expectedGroup = TEST_GROUP;
      } else {
        expectedLimit = 0;
        expectedGroup = "";
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getGroups()).isEqualTo(expectedGroup);
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(expectedLimit);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogFileSizeLimit_changesConfigOnAllServers(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "11");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(11);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogFileSizeLimitNegative_errorCanNotSet(final boolean connectOverHttp)
      throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"log-file-size-limit\"");

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "-1");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains("Could not set \"log-file-size-limit\" to \"-1\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogFileSizeLimitTooBig_errorCanNotSet(final boolean connectOverHttp)
      throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"log-file-size-limit\"");

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "1000001");
    CommandStringBuilder csbGroup = new CommandStringBuilder(csb.toString());
    csbGroup.addOption(GROUPS, "G1");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"log-file-size-limit\" to \"1000001\"");

    csb.addOption(CliStrings.MEMBER, server2.getName());
    result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"log-file-size-limit\" to \"1000001\"");

    result = gfsh.executeCommand(csbGroup.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"log-file-size-limit\" to \"1000001\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatArchiveFile_updatesAllServerConfigs(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final String TEST_NAME = "statisticsArchive";
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, TEST_NAME);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo(TEST_NAME);
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatArchiveFileWithMember_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final String TEST_NAME = "statisticsArchive";
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, TEST_NAME);
    csb.addOption(CliStrings.MEMBERS, server1.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (final MemberVM server : new MemberVM[] {server1, server2}) {
      String expectedName;
      if (server == server1) {
        expectedName = TEST_NAME;
      } else {
        expectedName = "";
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo(expectedName);
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatArchiveFileWithGroup_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final String TEST_NAME = "statisticsArchive";
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, TEST_NAME);
    csb.addOption(CliStrings.GROUP, "G1");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      String expectedName;
      if (server == server2) {
        expectedName = TEST_NAME;
      } else {
        expectedName = "";
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo(expectedName);
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatSampleRate_updatesAllServerConfigs(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(2000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatSampleRateWithMember_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_RATE = 2000;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE,
        String.valueOf(TEST_RATE));
    csb.addOption(CliStrings.MEMBER, server1.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (final MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedSampleRate;
      if (server == server1) {
        expectedSampleRate = TEST_RATE;
      } else {
        expectedSampleRate = 1000;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(expectedSampleRate);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatSampleRateWithGroup_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_RATE = 2500;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE,
        String.valueOf(TEST_RATE));
    csb.addOption(CliStrings.GROUP, "G1");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedSampleRate;
      if (server == server2) {
        expectedSampleRate = TEST_RATE;
      } else {
        expectedSampleRate = 1000;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(expectedSampleRate);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatisticSampleRateRangeIsEnforced(final boolean connectOverHttp)
      throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"statistic-sample-rate");

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "99");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput()).contains("Could not set \"statistic-sample-rate\" to \"99\"");

    csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "60001");

    result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"statistic-sample-rate\" to \"60001\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimit_updatesAllServerConfigs(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 10;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT,
        String.valueOf(TEST_LIMIT));

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(TEST_LIMIT);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimitWithMember_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 10;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT,
        String.valueOf(TEST_LIMIT));
    csb.addOption(CliStrings.MEMBER, server1.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (final MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit;
      if (server == server1) {
        expectedLimit = TEST_LIMIT;
      } else {
        expectedLimit = 0;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(expectedLimit);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimitWithGroup_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 25;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT,
        String.valueOf(TEST_LIMIT));
    csb.addOption(CliStrings.GROUP, "G1");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit;
      if (server == server2) {
        expectedLimit = TEST_LIMIT;
      } else {
        expectedLimit = 0;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(expectedLimit);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimitRangeIsEnforced(final boolean connectOverHttp)
      throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"archive-disk-space-limit");

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "-1");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"archive-disk-space-limit\" to \"-1\"");

    csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "1000001");

    result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"archive-disk-space-limit\" to \"1000001\"");

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimit_updatesAllServerConfigs(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 10;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT,
        String.valueOf(TEST_LIMIT));

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(TEST_LIMIT);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimitWithMember_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 10;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT,
        String.valueOf(TEST_LIMIT));
    csb.addOption(CliStrings.MEMBER, server1.getName());

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (final MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit;
      if (server == server1) {
        expectedLimit = TEST_LIMIT;
      } else {
        expectedLimit = 0;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(expectedLimit);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimitWithGroup_updatesSelectedServerConfigs(
      final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    final int TEST_LIMIT = 25;
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT,
        String.valueOf(TEST_LIMIT));
    csb.addOption(CliStrings.GROUP, "G1");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit;
      if (server == server2) {
        expectedLimit = TEST_LIMIT;
      } else {
        expectedLimit = 0;
      }
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit()).isEqualTo(expectedLimit);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isTrue();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimitRangeIsEnforced(final boolean connectOverHttp)
      throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"archive-file-size-limit");

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "-1");

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"archive-file-size-limit\" to \"-1\"");

    csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "1000001");

    result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains("Could not set \"archive-file-size-limit\" to \"1000001\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterDisableStatisticSampling(final boolean connectOverHttp) throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "false");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
        assertThat(config.getLogFileSizeLimit()).isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit()).isEqualTo(0);
        assertThat(config.getStatisticSampleRate()).isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName()).isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled()).isFalse();
        assertThat(config.getLogDiskSpaceLimit()).isEqualTo(0);
      });
    }
  }

  /**
   * Test to verify that when 'alter runtime' without relevant options does not change the server's
   * configuration
   */
  @Test
  @Parameters({"true", "false"})
  public void alterGroupWithoutOptions_needsRelevantParameter(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
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
      InternalCache cache = ClusterStartupRule.getCache();
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
  public void alterMemberWithoutOptions_needsRelevantParameter(final boolean connectOverHttp)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");
    MemberVM locator =
        startupRule.startLocatorVM(0, l -> l.withHttpService().withProperties(props));
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.MEMBERS, server1.getName());

    CommandResult result = gfsh.executeCommand(csb.toString());
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(gfsh.getGfshOutput())
        .contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
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
    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    Properties props = new Properties();
    props.setProperty(GROUPS, "Group1");
    props.setProperty(LOG_LEVEL, "error");
    startupRule.startServerVM(1, props, locator.getPort());

    String command = "alter runtime --group=Group1 --log-level=fine";
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService sharedConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Properties properties = sharedConfig.getConfiguration("Group1").getGemfireProperties();
      assertThat(properties.get(LOG_LEVEL)).isEqualTo("fine");
    });
  }
}
