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

import static java.lang.String.valueOf;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE;
import static org.apache.geode.management.internal.i18n.CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED;
import static org.apache.geode.management.internal.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.i18n.CliStrings.MEMBER;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.spi.LogWriterLevel;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(GfshTest.class)
@RunWith(JUnitParamsRunner.class)
public class AlterRuntimeCommandDistributedTest {

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule().withLogFile();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  @Parameters({"true", "false"})
  public void testAlterRuntimeConfig(boolean connectOverHttp) throws Exception {
    MemberVM server0 = startupRule.startServerVM(0, x -> x
        .withJMXManager()
        .withHttpService());

    ignoreIllegalArgumentException("Could not set \"log-disk-space-limit\"");
    if (connectOverHttp) {
      gfsh.connectAndVerify(server0.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder alterRuntimeConfig = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server0.getName())
        .addOption(ALTER_RUNTIME_CONFIG__LOG__LEVEL, "info")
        .addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50")
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32")
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49")
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000")
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, statsFile())
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "true")
        .addOption(ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(alterRuntimeConfig.toString())
        .statusIsSuccess();

    server0.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

      assertThat(config.getLogLevel())
          .isEqualTo(LogWriterLevel.INFO.intLevel());
      assertThat(config.getLogFileSizeLimit())
          .isEqualTo(50);
      assertThat(config.getArchiveDiskSpaceLimit())
          .isEqualTo(32);
      assertThat(config.getStatisticSampleRate())
          .isEqualTo(2000);
      assertThat(config.getStatisticArchiveFile().getName())
          .isEqualTo("stats.gfs");
      assertThat(config.getStatisticSamplingEnabled())
          .isTrue();
      assertThat(config.getLogDiskSpaceLimit())
          .isEqualTo(10);
    });

    gfsh.executeAndAssertThat("alter runtime")
        .statusIsError()
        .containsOutput(ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    gfsh.executeAndAssertThat("alter runtime  --log-disk-space-limit=2000000000")
        .statusIsError()
        .containsOutput("Could not set \"log-disk-space-limit\" to \"2000000000\"");
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitWithFileSizeLimitNotSet_OK(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setLogDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(setLogDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(10);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitWithFileSizeLimitSet_OK(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");

    gfsh.executeAndAssertThat(setFileSizeLimit.toString())
        .statusIsSuccess();

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

      assertThat(config.getLogFileSizeLimit())
          .isEqualTo(50);
      assertThat(config.getLogDiskSpaceLimit())
          .isEqualTo(0);
    });

    CommandStringBuilder setDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(setDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(50);
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(10);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitOnMember_OK(boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setLogDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server1.getName())
        .addOption(ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    gfsh.executeAndAssertThat(setLogDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit = server == server1 ? 10 : 0;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(expectedLimit);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogDiskSpaceLimitOnGroup_OK(boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    String testGroup = "G1";
    int testLimit = 10;

    CommandStringBuilder setLogDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(CliStrings.GROUPS, testGroup)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setLogDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit = server == server2 ? testLimit : 0;
      String expectedGroup = server == server2 ? testGroup : "";

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getGroups())
            .isEqualTo(expectedGroup);
        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(expectedLimit);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogFileSizeLimit_changesConfigOnAllServers(boolean connectOverHttp)
      throws Exception {

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setLogFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "11");

    gfsh.executeAndAssertThat(setLogFileSizeLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(11);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogFileSizeLimitNegative_errorCanNotSet(boolean connectOverHttp)
      throws Exception {

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, locator.getPort());

    ignoreIllegalArgumentException("Could not set \"log-file-size-limit\"");
    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setLogFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "-1");

    gfsh.executeAndAssertThat(setLogFileSizeLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"log-file-size-limit\" to \"-1\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterLogFileSizeLimitTooBig_errorCanNotSet(boolean connectOverHttp)
      throws Exception {

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());

    Properties props = new Properties();
    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    ignoreIllegalArgumentException("Could not set \"log-file-size-limit\"");
    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setLogFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "1000001");

    CommandStringBuilder withGroup = new CommandStringBuilder(setLogFileSizeLimit.toString())
        .addOption(GROUP, "G1");

    gfsh.executeAndAssertThat(setLogFileSizeLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"log-file-size-limit\" to \"1000001\"");

    setLogFileSizeLimit.addOption(MEMBER, server2.getName());

    gfsh.executeAndAssertThat(setLogFileSizeLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"log-file-size-limit\" to \"1000001\"");

    gfsh.executeAndAssertThat(withGroup.toString())
        .statusIsError()
        .containsOutput("Could not set \"log-file-size-limit\" to \"1000001\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatArchiveFile_updatesAllServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    String testName = "statisticsArchive";

    CommandStringBuilder setStatisticArchiveFile = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, testName);

    gfsh.executeAndAssertThat(setStatisticArchiveFile.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo(testName);
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatArchiveFileWithMember_updatesSelectedServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    String testName = "statisticsArchive";

    CommandStringBuilder setStatisticArchiveFile = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server1.getName())
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, testName);

    gfsh.executeAndAssertThat(setStatisticArchiveFile.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      String expectedName = server == server1 ? testName : "";

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo(expectedName);
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatArchiveFileWithGroup_updatesSelectedServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    String testName = "statisticsArchive";

    CommandStringBuilder setStatisticArchiveFile = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(GROUP, "G1")
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, testName);

    gfsh.executeAndAssertThat(setStatisticArchiveFile.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      String expectedName = server == server2 ? testName : "";

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo(expectedName);
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatSampleRate_updatesAllServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setStatisticSampleRate = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000");

    gfsh.executeAndAssertThat(setStatisticSampleRate.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(2000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatSampleRateWithMember_updatesSelectedServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testRate = 2000;

    CommandStringBuilder setStatSampleRate = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server1.getName())
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, valueOf(testRate));

    gfsh.executeAndAssertThat(setStatSampleRate.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedSampleRate = server == server1 ? testRate : 1000;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(expectedSampleRate);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatSampleRateWithGroup_updatesSelectedServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testRate = 2500;

    CommandStringBuilder setStatSampleRate = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(GROUP, "G1")
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, valueOf(testRate));

    gfsh.executeAndAssertThat(setStatSampleRate.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedSampleRate = server == server2 ? testRate : 1000;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(expectedSampleRate);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterStatisticSampleRateRangeIsEnforced(boolean connectOverHttp) throws Exception {

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, locator.getPort());

    ignoreIllegalArgumentException("Could not set \"statistic-sample-rate\"");

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setStatSampleRate = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "99");

    gfsh.executeAndAssertThat(setStatSampleRate.toString())
        .statusIsError()
        .containsOutput("Could not set \"statistic-sample-rate\" to \"99\"");

    setStatSampleRate = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "60001");

    gfsh.executeAndAssertThat(setStatSampleRate.toString())
        .statusIsError()
        .containsOutput("Could not set \"statistic-sample-rate\" to \"60001\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimit_updatesAllServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testLimit = 10;
    CommandStringBuilder setArchiveDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setArchiveDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(testLimit);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimitWithMember_updatesSelectedServerConfigs(
      boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testLimit = 10;

    CommandStringBuilder setArchiveDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server1.getName())
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setArchiveDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit = server == server1 ? testLimit : 0;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(expectedLimit);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimitWithGroup_updatesSelectedServerConfigs(
      boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testLimit = 25;

    CommandStringBuilder setArchiveDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(GROUP, "G1")
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setArchiveDiskSpaceLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit = server == server2 ? testLimit : 0;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(expectedLimit);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveDiskSpaceLimitRangeIsEnforced(boolean connectOverHttp) throws Exception {

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, locator.getPort());

    ignoreIllegalArgumentException("Could not set \"archive-disk-space-limit");
    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setArchiveDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "-1");

    gfsh.executeAndAssertThat(setArchiveDiskSpaceLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"archive-disk-space-limit\" to \"-1\"");

    setArchiveDiskSpaceLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "1000001");

    gfsh.executeAndAssertThat(setArchiveDiskSpaceLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"archive-disk-space-limit\" to \"1000001\"");

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimit_updatesAllServerConfigs(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testLimit = 10;

    CommandStringBuilder setArchiveFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setArchiveFileSizeLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(testLimit);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimitWithMember_updatesSelectedServerConfigs(
      boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testLimit = 10;

    CommandStringBuilder setArchiveFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server1.getName())
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setArchiveFileSizeLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit = server == server1 ? testLimit : 0;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(expectedLimit);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimitWithGroup_updatesSelectedServerConfigs(
      boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    props.setProperty(GROUPS, "G1");
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    int testLimit = 25;

    CommandStringBuilder setArchiveFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(GROUP, "G1")
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, valueOf(testLimit));

    gfsh.executeAndAssertThat(setArchiveFileSizeLimit.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      int expectedLimit = server == server2 ? testLimit : 0;

      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveFileSizeLimit())
            .isEqualTo(expectedLimit);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void alterArchiveFileSizeLimitRangeIsEnforced(boolean connectOverHttp) throws Exception {

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, locator.getPort());

    ignoreIllegalArgumentException("Could not set \"archive-file-size-limit\"");
    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setArchiveFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "-1");

    gfsh.executeAndAssertThat(setArchiveFileSizeLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"archive-file-size-limit\" to \"-1\"");

    setArchiveFileSizeLimit = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "1000001");

    gfsh.executeAndAssertThat(setArchiveFileSizeLimit.toString())
        .statusIsError()
        .containsOutput("Could not set \"archive-file-size-limit\" to \"1000001\"");

    verifyDefaultConfig(new MemberVM[] {server1, server2});
  }

  @Test
  @Parameters({"true", "false"})
  public void alterDisableStatisticSampling(boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder setStatSamplingEnabled = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "false");

    gfsh.executeAndAssertThat(setStatSamplingEnabled.toString())
        .statusIsSuccess();

    for (MemberVM server : new MemberVM[] {server1, server2}) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isFalse();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  /**
   * Test to verify that when 'alter runtime' without relevant options does not change the server's
   * configuration
   */
  @Test
  @Parameters({"true", "false"})
  public void alterGroupWithoutOptions_needsRelevantParameter(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
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

      assertThat(config.getGroups())
          .isEqualTo("G1");
    });

    CommandStringBuilder withGroupOnly = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(GROUP, "G1");

    gfsh.executeAndAssertThat(withGroupOnly.toString())
        .statusIsError()
        .containsOutput(ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

      assertThat(config.getLogFileSizeLimit())
          .isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit())
          .isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit())
          .isEqualTo(0);
      assertThat(config.getStatisticSampleRate())
          .isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName())
          .isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled())
          .isTrue();
    });
  }

  /**
   * Test to verify that when 'alter runtime' without relevant options does not change the server's
   * configuration
   */
  @Test
  @Parameters({"true", "false"})
  public void alterMemberWithoutOptions_needsRelevantParameter(boolean connectOverHttp)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(LOG_LEVEL, "error");

    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    MemberVM server1 = startupRule.startServerVM(1, props, locator.getPort());

    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    CommandStringBuilder withMemberOnly = new CommandStringBuilder(ALTER_RUNTIME_CONFIG)
        .addOption(MEMBER, server1.getName());

    gfsh.executeAndAssertThat(withMemberOnly.toString())
        .statusIsError()
        .containsOutput(ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE);

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

      assertThat(config.getLogFileSizeLimit())
          .isEqualTo(0);
      assertThat(config.getLogDiskSpaceLimit())
          .isEqualTo(0);
      assertThat(config.getArchiveDiskSpaceLimit())
          .isEqualTo(0);
      assertThat(config.getStatisticSampleRate())
          .isEqualTo(1000);
      assertThat(config.getStatisticArchiveFile().getName())
          .isEqualTo("");
      assertThat(config.getStatisticSamplingEnabled())
          .isTrue();
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void testAlterUpdatesSharedConfig(boolean connectOverHttp) throws Exception {
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
    gfsh.executeAndAssertThat(command)
        .statusIsSuccess();

    locator.invoke(() -> {
      Properties properties = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getConfiguration("Group1")
          .getGemfireProperties();

      assertThat(properties.get(LOG_LEVEL))
          .isEqualTo("fine");
    });
  }

  private void verifyDefaultConfig(MemberVM[] servers) {
    for (MemberVM server : servers) {
      server.invoke(() -> {
        InternalCache cache = ClusterStartupRule.getCache();
        DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

        assertThat(config.getLogLevel())
            .isEqualTo(LogWriterLevel.CONFIG.intLevel());
        assertThat(config.getLogFileSizeLimit())
            .isEqualTo(0);
        assertThat(config.getArchiveDiskSpaceLimit())
            .isEqualTo(0);
        assertThat(config.getStatisticSampleRate())
            .isEqualTo(1000);
        assertThat(config.getStatisticArchiveFile().getName())
            .isEqualTo("");
        assertThat(config.getStatisticSamplingEnabled())
            .isTrue();
        assertThat(config.getLogDiskSpaceLimit())
            .isEqualTo(0);
      });
    }
  }

  private File statsFile() throws IOException {
    return temporaryFolder.newFile("stats.gfs");
  }

  private void ignoreIllegalArgumentException(String message) {
    addIgnoredException(IllegalArgumentException.class.getName() + ": " + message);
  }
}
