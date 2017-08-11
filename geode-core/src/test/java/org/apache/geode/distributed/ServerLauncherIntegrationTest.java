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
package org.apache.geode.distributed;

import static com.googlecode.catchexception.apis.BDDCatchException.caughtException;
import static com.googlecode.catchexception.apis.BDDCatchException.when;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.DistributedSystem.PROPERTIES_FILE_PROPERTY;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.cache.AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssertions.then;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.ServerLauncher.Command;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for using {@link ServerLauncher} as an in-process API within an existing JVM.
 */
@Category(IntegrationTest.class)
public class ServerLauncherIntegrationTest {

  private static final String CURRENT_DIRECTORY = System.getProperty("user.dir");

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void buildWithMemberNameSetInGemFireProperties() throws Exception {
    // given: gemfire.properties containing a name
    givenGemFirePropertiesFile(withMemberName());

    // when: starting with null MemberName
    ServerLauncher launcher = new Builder().setCommand(Command.START).build();

    // then: name in gemfire.properties file should be used for MemberName
    assertThat(launcher.getMemberName()).isNull(); // name will be read during start()
  }

  @Test
  public void buildWithoutMemberNameThrowsIllegalStateException() throws Exception {
    // given: gemfire.properties with no name
    givenGemFirePropertiesFile(withoutMemberName());

    // when: no MemberName is specified
    when(new Builder().setCommand(Command.START)).build();

    // then: throw IllegalStateException
    then(caughtException()).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(memberNameValidationErrorMessage());
  }

  @Test
  public void buildWithWorkingDirectoryNotEqualToCurrentDirectoryThrowsIllegalStateException()
      throws Exception {
    // given: using LocatorLauncher in-process

    // when: setting WorkingDirectory to non-current directory
    when(new Builder().setCommand(Command.START).setMemberName("memberOne")
        .setWorkingDirectory(getWorkingDirectoryPath())).build();

    // then: throw IllegalStateException
    then(caughtException()).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(workingDirectoryOptionNotValidErrorMessage());
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedByCommas() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: parsing many arguments
    builder.parseArguments("start", "memberOne", "--server-bind-address",
        InetAddress.getLocalHost().getHostAddress(), "--dir", getWorkingDirectoryPath(),
        "--hostname-for-clients", "Tucows", "--pid", "1234", "--server-port", "11235",
        "--redirect-output", "--force", "--debug");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getDebug()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getHostNameForClients()).isEqualTo("Tucows");
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getServerPort().intValue()).isEqualTo(11235);
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedByEquals() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: parsing arguments with values separated by equals
    builder.parseArguments("start", "--dir=" + getWorkingDirectoryPath(),
        "--server-port=" + "12345", "memberOne");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getDebug()).isFalse();
    assertThat(builder.getForce()).isFalse();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getHostNameForClients()).isNull();
    assertThat(builder.getMemberName()).isEqualTo("memberOne");
    assertThat(builder.getPid()).isNull();
    assertThat(builder.getServerBindAddress()).isNull();
    assertThat(builder.getServerPort().intValue()).isEqualTo(12345);
    assertThat(builder.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void getWorkingDirectoryReturnsCurrentDirectoryByDefault() throws Exception {
    // given:

    // when: not setting WorkingDirectory

    // then: getDirectory returns default
    assertThat(new Builder().getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToNullUsesCurrentDirectory() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to null
    assertThat(builder.setWorkingDirectory(null)).isSameAs(builder);

    // then: getDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToEmptyStringUsesCurrentDirectory() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to empty string
    assertThat(builder.setWorkingDirectory("")).isSameAs(builder);

    // then: getDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToBlankStringUsesCurrentDirectory() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to white space
    assertThat(builder.setWorkingDirectory("  ")).isSameAs(builder);

    // then: getDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToExistingDirectory() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to a directory
    assertThat(builder.setWorkingDirectory(getWorkingDirectoryPath())).isSameAs(builder);

    // then: getDirectory returns that directory
    assertThat(builder.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void setWorkingDirectoryToExistingFileThrowsIllegalArgumentException() throws Exception {
    // given: a file instead of a directory
    File nonDirectory = temporaryFolder.newFile();

    // when: setting WorkingDirectory to that file
    when(new Builder()).setWorkingDirectory(nonDirectory.getCanonicalPath());

    // then: throw IllegalArgumentException
    then(caughtException()).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(workingDirectoryNotFoundErrorMessage())
        .hasCause(new FileNotFoundException(nonDirectory.getCanonicalPath()));
  }

  @Test
  public void setWorkingDirectoryToNonExistingDirectory() throws Exception {
    // given:

    // when: setting WorkingDirectory to non-existing directory
    when(new Builder()).setWorkingDirectory("/path/to/non_existing/directory");

    // then: throw IllegalArgumentException
    then(caughtException()).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(workingDirectoryNotFoundErrorMessage())
        .hasCause(new FileNotFoundException("/path/to/non_existing/directory"));
  }

  @Test
  public void serverPortCanBeOverriddenBySystemProperty() throws Exception {
    // given: overridden default port
    int overriddenPort = AvailablePortHelper.getRandomAvailableTCPPort();
    System.setProperty(TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(overriddenPort));

    // when: creating new ServerLauncher
    ServerLauncher launcher = new Builder().build();

    // then: server port should be the overridden default port
    assertThat(launcher.getServerPort()).isEqualTo(overriddenPort);
  }

  private String memberNameValidationErrorMessage() {
    return LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE
        .toLocalizedString("Server");
  }

  private String workingDirectoryOptionNotValidErrorMessage() {
    return LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE
        .toLocalizedString("Server");
  }

  private String workingDirectoryNotFoundErrorMessage() {
    return LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE
        .toLocalizedString("Server");
  }

  private File getWorkingDirectory() {
    return temporaryFolder.getRoot();
  }

  private String getWorkingDirectoryPath() {
    try {
      return temporaryFolder.getRoot().getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Properties withoutMemberName() {
    return new Properties();
  }

  private Properties withMemberName() {
    Properties properties = new Properties();
    properties.setProperty(NAME, "locator123");
    return properties;
  }

  /**
   * Creates a gemfire properties file in temporaryFolder:
   * <ol>
   * <li>creates gemfire.properties in <code>temporaryFolder</code></li>
   * <li>writes config to the file</li>
   * <li>sets "gemfirePropertyFile" system property</li>
   * </ol>
   */
  private void givenGemFirePropertiesFile(final Properties config) {
    try {
      String name = GEMFIRE_PREFIX + "properties";
      File file = new File(getWorkingDirectory(), name);
      config.store(new FileWriter(file, false), testName.getMethodName());
      assertThat(file).isFile().exists();

      System.setProperty(PROPERTIES_FILE_PROPERTY, file.getCanonicalPath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
