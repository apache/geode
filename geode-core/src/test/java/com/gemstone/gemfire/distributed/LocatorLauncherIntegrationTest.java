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

import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.Command;
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
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import static com.googlecode.catchexception.apis.BDDCatchException.caughtException;
import static com.googlecode.catchexception.apis.BDDCatchException.when;
import static org.assertj.core.api.BDDAssertions.assertThat;
import static org.assertj.core.api.BDDAssertions.then;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

/**
 * Integration tests for LocatorLauncher. These tests require file system I/O.
 */
@Category(IntegrationTest.class)
public class LocatorLauncherIntegrationTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  @Test
  public void testBuilderParseArgumentsWithValuesSeparatedWithCommas() throws Exception {
    // given: a new builder and working directory
    String expectedWorkingDirectory = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    // when: parsing many arguments
    builder.parseArguments(
        "start", 
        "memberOne", 
        "--bind-address", InetAddress.getLocalHost().getHostAddress(),
        "--dir", expectedWorkingDirectory, 
        "--hostname-for-clients", "Tucows", 
        "--pid", "1234", 
        "--port", "11235",
        "--redirect-output", 
        "--force", 
        "--debug");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getWorkingDirectory()).isEqualTo(expectedWorkingDirectory);
    assertThat(builder.getHostnameForClients()).isEqualTo("Tucows");
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getPort().intValue()).isEqualTo(11235);
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getDebug()).isTrue();
  }

  @Test
  public void testBuilderParseArgumentsWithValuesSeparatedWithEquals() throws Exception {
    // given: a new builder and a directory
    String expectedWorkingDirectory = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    // when: parsing arguments with values separated by equals
    builder.parseArguments(
        "start", 
        "--dir=" + expectedWorkingDirectory, 
        "--port=" + "12345", 
        "memberOne");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getDebug()).isFalse();
    assertThat(builder.getForce()).isFalse();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getBindAddress()).isNull();
    assertThat(builder.getHostnameForClients()).isNull();
    assertThat(builder.getMemberName()).isEqualTo("memberOne");
    assertThat(builder.getPid()).isNull();
    assertThat(builder.getWorkingDirectory()).isEqualTo(expectedWorkingDirectory);
    assertThat(builder.getPort().intValue()).isEqualTo(12345);
  }

  @Test
  public void testBuildWithMemberNameSetInGemFirePropertiesOnStart() throws Exception {
    // given: gemfire.properties with a name
    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(NAME, "locator123");
    useGemFirePropertiesFileInTemporaryFolder(DistributionConfig.GEMFIRE_PREFIX + "properties", gemfireProperties);
    
    // when: starting with null MemberName
    LocatorLauncher launcher = new Builder()
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
        .hasMessage(LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE.toLocalizedString("Locator"));
  }

  @Test
  public void testBuilderSetAndGetWorkingDirectory() throws Exception {
    // given: a new builder and a directory
    String rootFolder = this.temporaryFolder.getRoot().getCanonicalPath();
    Builder builder = new Builder();

    // when: not setting WorkingDirectory
    // then: getWorkingDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(AbstractLauncher.DEFAULT_WORKING_DIRECTORY);
    
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
  public void testBuilderSetWorkingDirectoryToFile() throws IOException {
    // given: a file instead of a directory
    File tmpFile = this.temporaryFolder.newFile();

    // when: setting WorkingDirectory to that file
    when(new Builder())
        .setWorkingDirectory(tmpFile.getCanonicalPath());

    // then: throw IllegalArgumentException
    then(caughtException())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toLocalizedString("Locator"))
        .hasCause(new FileNotFoundException(tmpFile.getCanonicalPath()));
  }

  @Test
  public void testBuildSetWorkingDirectoryToNonCurrentDirectoryOnStart() throws Exception {
    // given: using LocatorLauncher in-process
    
    // when: setting WorkingDirectory to non-current directory
    when(new Builder()
        .setCommand(Command.START)
        .setMemberName("memberOne")
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath()))
        .build();
    
    // then: throw IllegalStateException
    then(caughtException())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE.toLocalizedString("Locator"));
  }
  
  @Test
  public void testBuilderSetWorkingDirectoryToNonExistingDirectory() {
    // when: setting WorkingDirectory to non-existing directory
    when(new Builder())
        .setWorkingDirectory("/path/to/non_existing/directory");
    
    // then: throw IllegalArgumentException
    then(caughtException())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toLocalizedString("Locator"))
        .hasCause(new FileNotFoundException("/path/to/non_existing/directory"));
  }

  /**
   * Creates a gemfire properties file in temporaryFolder:
   * <ol>
   * <li>creates <code>fileName</code> in <code>temporaryFolder</code></li>
   * <li>sets "gemfirePropertyFile" system property</li>
   * <li>writes <code>gemfireProperties</code> to the file</li>
   * </ol>
   */
  private void useGemFirePropertiesFileInTemporaryFolder(final String fileName, final Properties gemfireProperties) throws Exception {
    File propertiesFile = new File(this.temporaryFolder.getRoot().getCanonicalPath(), fileName);
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, propertiesFile.getCanonicalPath());
    
    gemfireProperties.store(new FileWriter(propertiesFile, false), this.testName.getMethodName());
    assertThat(propertiesFile.isFile()).isTrue();
    assertThat(propertiesFile.exists()).isTrue();
  }
}
