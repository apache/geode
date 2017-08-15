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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class ConfigCommandDUnitTest {
  @Rule
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();

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
      gfsh.connectAndVerify(server0.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
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
    assertThat(result).contains("groups\\s+: G1");
    assertThat(result).contains("archive-file-size-limit\\s+: 1000");
    assertThat(result).contains("name\\s+: server-0");
    assertThat(result).contains("is-server\\s+: true");
    assertThat(result).doesNotContain("copy-on-read");

    gfsh.executeAndVerifyCommand(
        "describe config --member=" + server0.getName() + " --hide-defaults=false");
    result = gfsh.getGfshOutput();
    assertThat(result).contains("copy-on-read");
  }

  @Test
  @Parameters({"true", "false"})
  public void testExportConfig(final boolean connectOverHttp) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, "Group1");

    MemberVM server0 = startupRule.startServerAsEmbededLocator(0, props);

    if (connectOverHttp) {
      gfsh.connectAndVerify(server0.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server0.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }

    // start server1 and server2 in group2
    props.setProperty(GROUPS, "Group2");
    startupRule.startServerVM(1, props, server0.getEmbeddedLocatorPort());
    startupRule.startServerVM(2, props, server0.getEmbeddedLocatorPort());

    // start server3 that has no group info
    startupRule.startServerVM(3, server0.getEmbeddedLocatorPort());

    // export all members' config into a folder
    File tempDir = temporaryFolder.newFolder("all-members");
    gfsh.executeAndVerifyCommand("export config --dir=" + tempDir.getAbsolutePath());

    List<String> expectedFiles = Arrays.asList("server-0-cache.xml", "server-1-cache.xml",
        "server-2-cache.xml", "server-3-cache.xml", "server-0-gf.properties",
        "server-1-gf.properties", "server-2-gf.properties", "server-3-gf.properties");

    List<String> actualFiles =
        Arrays.stream(tempDir.listFiles()).map(File::getName).collect(Collectors.toList());
    assertThat(actualFiles).hasSameElementsAs(expectedFiles);
    tempDir.delete();

    // export just one member's config
    tempDir = temporaryFolder.newFolder("member0");
    gfsh.executeAndVerifyCommand(
        "export config --member=server-0 --dir=" + tempDir.getAbsolutePath());

    expectedFiles = Arrays.asList("server-0-cache.xml", "server-0-gf.properties");

    actualFiles =
        Arrays.stream(tempDir.listFiles()).map(File::getName).collect(Collectors.toList());
    assertThat(actualFiles).hasSameElementsAs(expectedFiles);
    tempDir.delete();

    // export group2 config into a folder
    tempDir = temporaryFolder.newFolder("group2");
    gfsh.executeAndVerifyCommand("export config --group=Group2 --dir=" + tempDir.getAbsolutePath());

    expectedFiles = Arrays.asList("server-1-cache.xml", "server-2-cache.xml",
        "server-1-gf.properties", "server-2-gf.properties");

    actualFiles =
        Arrays.stream(tempDir.listFiles()).map(File::getName).collect(Collectors.toList());
    assertThat(actualFiles).hasSameElementsAs(expectedFiles);
    tempDir.delete();
  }

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
