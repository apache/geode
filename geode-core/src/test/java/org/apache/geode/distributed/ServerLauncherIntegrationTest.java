/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.Command;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.net.InetAddress;
import java.util.Properties;

import static com.googlecode.catchexception.apis.BDDCatchException.caughtException;
import static com.googlecode.catchexception.apis.BDDCatchException.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssertions.then;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

/**
 * Integration tests for ServerLauncher class. These tests may require file system and/or network I/O.
 */
@Category(IntegrationTest.class)
public class ServerLauncherIntegrationTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  @Test
  public void testBuildWithManyArguments() throws Exception {
    // given
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    
    // when
    ServerLauncher launcher = new Builder()
        .setCommand(Command.STOP)
        .setAssignBuckets(true)
        .setForce(true)
        .setMemberName("serverOne")
        .setRebalance(true)
        .setServerBindAddress(InetAddress.getLocalHost().getHostAddress())
        .setServerPort(11235)
        .setWorkingDirectory(rootFolder)
        .setCriticalHeapPercentage(90.0f)
        .setEvictionHeapPercentage(75.0f)
        .setMaxConnections(100)
        .setMaxMessageCount(512)
        .setMaxThreads(8)
        .setMessageTimeToLive(120000)
        .setSocketBufferSize(32768)
        .build();

    // then
    assertThat(launcher).isNotNull();
    assertThat(launcher.isAssignBuckets()).isTrue();
    assertThat(launcher.isDebugging()).isFalse();
    assertThat(launcher.isDisableDefaultServer()).isFalse();
    assertThat(launcher.isForcing()).isTrue();
    assertThat(launcher.isHelping()).isFalse();
    assertThat(launcher.isRebalancing()).isTrue();
    assertThat(launcher.isRunning()).isFalse();
    assertThat(launcher.getCommand()).isEqualTo(Command.STOP);
    assertThat(launcher.getMemberName()).isEqualTo("serverOne");
    assertThat(launcher.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(launcher.getServerPort().intValue()).isEqualTo(11235);
    assertThat(launcher.getWorkingDirectory()).isEqualTo(rootFolder);
    assertThat(launcher.getCriticalHeapPercentage().floatValue()).isEqualTo(90.0f);
    assertThat(launcher.getEvictionHeapPercentage().floatValue()).isEqualTo(75.0f);
    assertThat(launcher.getMaxConnections().intValue()).isEqualTo(100);
    assertThat(launcher.getMaxMessageCount().intValue()).isEqualTo(512);
    assertThat(launcher.getMaxThreads().intValue()).isEqualTo(8);
    assertThat(launcher.getMessageTimeToLive().intValue()).isEqualTo(120000);
    assertThat(launcher.getSocketBufferSize().intValue()).isEqualTo(32768);
  }

  @Test
  public void testBuilderParseArgumentsWithValuesSeparatedWithCommas() throws Exception {
    // given a new builder and a directory
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    // when: parsing many arguments
    builder.parseArguments(
        "start", 
        "serverOne", 
        "--assign-buckets", 
        "--disable-default-server", 
        "--debug", 
        "--force",
        "--rebalance", 
        "--redirect-output", 
        "--dir", rootFolder, 
        "--pid", "1234",
        "--server-bind-address", InetAddress.getLocalHost().getHostAddress(), 
        "--server-port", "11235", 
        "--hostname-for-clients", "192.168.99.100");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getMemberName()).isEqualTo("serverOne");
    assertThat(builder.getHostNameForClients()).isEqualTo("192.168.99.100");
    assertThat(builder.getAssignBuckets()).isTrue();
    assertThat(builder.getDisableDefaultServer()).isTrue();
    assertThat(builder.getDebug()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getRebalance()).isTrue();
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getWorkingDirectory()).isEqualTo(rootFolder);
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getServerPort().intValue()).isEqualTo(11235);
  }

  @Test
  public void testBuilderParseArgumentsWithValuesSeparatedWithEquals() throws Exception {
    // given a new builder and a directory
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    // when: parsing many arguments
    builder.parseArguments(
        "start", 
        "serverOne", 
        "--assign-buckets", 
        "--disable-default-server", 
        "--debug", 
        "--force",
        "--rebalance", 
        "--redirect-output", 
        "--dir=" + rootFolder, 
        "--pid=1234",
        "--server-bind-address=" + InetAddress.getLocalHost().getHostAddress(), 
        "--server-port=11235", 
        "--hostname-for-clients=192.168.99.100");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getMemberName()).isEqualTo("serverOne");
    assertThat(builder.getHostNameForClients()).isEqualTo("192.168.99.100");
    assertThat(builder.getAssignBuckets()).isTrue();
    assertThat(builder.getDisableDefaultServer()).isTrue();
    assertThat(builder.getDebug()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getRebalance()).isTrue();
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getWorkingDirectory()).isEqualTo(rootFolder);
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getServerPort().intValue()).isEqualTo(11235);
  }

  @Test
  public void testBuildWithMemberNameSetInGemFirePropertiesOnStart() throws Exception {
    // given: gemfire.properties with a name
    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(NAME, "server123");
    useGemFirePropertiesFileInTemporaryFolder(DistributionConfig.GEMFIRE_PREFIX + "properties", gemfireProperties);

    // when: starting with null MemberName
    ServerLauncher launcher = new Builder()
        .setCommand(Command.START)
        .setMemberName(null)
        .build();

    // then: name in gemfire.properties file should be used for MemberName
    assertThat(launcher).isNotNull();
    assertThat(launcher.getCommand()).isEqualTo(Command.START);
    assertThat(launcher.getMemberName()).isNull();
  }
  
  @Test
  public void testBuildWithNoMemberNameOnStart() throws Exception {
    // given: gemfire.properties with no name
    useGemFirePropertiesFileInTemporaryFolder(DistributionConfig.GEMFIRE_PREFIX + "properties", new Properties());

    // when: no MemberName is specified
    when(new Builder()
        .setCommand(Command.START))
        .build();
    
    // then: throw IllegalStateException
    then(caughtException())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE.toLocalizedString("Server"));
  }

  @Test
  public void testBuilderSetAndGetWorkingDirectory() throws Exception {
    // given: a new builder and a directory
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    // when: not setting WorkingDirectory
    // then: getWorkingDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(ServerLauncher.DEFAULT_WORKING_DIRECTORY);
    
    // when: setting WorkingDirectory to null
    assertThat(builder.setWorkingDirectory(null)).isSameAs(builder);
    // then: getWorkingDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(AbstractLauncher.DEFAULT_WORKING_DIRECTORY);

    // when: setting WorkingDirectory to empty string
    assertThat(builder.setWorkingDirectory("")).isSameAs(builder);
    // then: getWorkingDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(AbstractLauncher.DEFAULT_WORKING_DIRECTORY);

    // when: setting WorkingDirectory to white space
    assertThat(builder.setWorkingDirectory("  ")).isSameAs(builder);
    // then: getWorkingDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(AbstractLauncher.DEFAULT_WORKING_DIRECTORY);

    // when: setting WorkingDirectory to a directory
    assertThat(builder.setWorkingDirectory(rootFolder)).isSameAs(builder);
    // then: getWorkingDirectory returns that directory
    assertThat(builder.getWorkingDirectory()).isEqualTo(rootFolder);

    // when: setting WorkingDirectory to null (again)
    assertThat(builder.setWorkingDirectory(null)).isSameAs(builder);
    // then: getWorkingDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(AbstractLauncher.DEFAULT_WORKING_DIRECTORY);
  }

  @Test
  public void testBuilderSetWorkingDirectoryToFile() throws Exception {
    // given: a file instead of a directory
    File tmpFile = this.temporaryFolder.newFile();

    // when: setting WorkingDirectory to that file
    when(new Builder())
        .setWorkingDirectory(tmpFile.getAbsolutePath());
    
    // then: throw IllegalArgumentException
    then(caughtException())
        .hasMessage(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toLocalizedString("Server"))
        .hasCause(new FileNotFoundException(tmpFile.getAbsolutePath()));
  }

  @Test
  public void testBuildSetWorkingDirectoryToNonCurrentDirectoryOnStart() throws Exception {
    // given: using ServerLauncher in-process

    // when: setting WorkingDirectory to non-current directory
    when(new Builder()
        .setCommand(Command.START)
        .setMemberName("serverOne")
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath()))
        .build();
    
    // then: throw IllegalStateException
    then(caughtException())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE.toLocalizedString("Server"));
  }

  @Test
  public void testBuilderSetWorkingDirectoryToNonExistingDirectory() {
    // when: setting WorkingDirectory to non-existing directory
    when(new Builder())
        .setWorkingDirectory("/path/to/non_existing/directory");
    
    // then: throw IllegalArgumentException
    then(caughtException())
        .hasMessage(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toLocalizedString("Server"))
        .hasCause(new FileNotFoundException("/path/to/non_existing/directory"));
  }

  /**
   * Creates a gemfire properties file in temporaryFolder:
   * <li>creates <code>fileName</code> in <code>temporaryFolder</code>
   * <li>sets "gemfirePropertyFile" system property
   * <li>writes <code>gemfireProperties</code> to the file
   */
  private void useGemFirePropertiesFileInTemporaryFolder(final String fileName, final Properties gemfireProperties) throws Exception {
    File propertiesFile = new File(this.temporaryFolder.getRoot().getCanonicalPath(), fileName);
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    
    gemfireProperties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());
    assertThat(propertiesFile.isFile()).isTrue();
    assertThat(propertiesFile.exists()).isTrue();
  }
}
