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

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.DistributedSystem.PROPERTIES_FILE_PROPERTY;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.assertThat;
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
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.Command;

/**
 * Integration tests for using {@link LocatorLauncher} as an in-process API within an existing JVM.
 */
public class LocatorLauncherIntegrationTest {

  private static final String CURRENT_DIRECTORY = System.getProperty("user.dir");

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void buildWithMemberNameSetInGemFireProperties() {
    // given: gemfire.properties with a name
    givenGemFirePropertiesFile(withMemberName());

    // when: starting with null MemberName
    LocatorLauncher launcher = new Builder().setCommand(Command.START).build();

    // then: name in gemfire.properties file should be used for MemberName
    assertThat(launcher.getMemberName()).isNull(); // name will be read during start()
  }

  @Test
  public void buildWithNoMemberNameThrowsIllegalStateException() {
    // given: gemfire.properties with no name
    givenGemFirePropertiesFile(withoutMemberName());

    // when: no MemberName is specified
    Throwable thrown = catchThrowable(() -> new Builder().setCommand(Command.START).build());

    // then: throw IllegalStateException
    then(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(memberNameValidationErrorMessage());
  }

  @Test
  public void buildWithWorkingDirectoryNotEqualToCurrentDirectoryThrowsIllegalStateException() {
    // given: using LocatorLauncher in-process

    // when: setting WorkingDirectory to non-current directory
    Throwable thrown =
        catchThrowable(() -> new Builder().setCommand(Command.START).setMemberName("memberOne")
            .setWorkingDirectory(getWorkingDirectoryPath()).build());

    // then: throw IllegalStateException
    then(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(workingDirectoryOptionNotValidErrorMessage());
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedByCommas() throws Exception {
    // given: a new builder
    Builder builder = new Builder();

    // when: parsing many arguments
    builder.parseArguments("start", "memberOne", "--bind-address",
        InetAddress.getLocalHost().getHostAddress(), "--dir", getWorkingDirectoryPath(),
        "--hostname-for-clients", "Tucows", "--pid", "1234", "--port", "11235", "--redirect-output",
        "--force", "--debug");

    // then: the getters should return properly parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getDebug()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getHostnameForClients()).isEqualTo("Tucows");
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getPort().intValue()).isEqualTo(11235);
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedByEquals() {
    // given: a new builder
    Builder builder = new Builder();

    // when: parsing arguments with values separated by equals
    builder.parseArguments("start", "--dir=" + getWorkingDirectoryPath(), "--port=" + "12345",
        "memberOne");

    // then: the getters should return properly parsed values
    assertThat(builder.getBindAddress()).isNull();
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getDebug()).isFalse();
    assertThat(builder.getForce()).isFalse();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getHostnameForClients()).isNull();
    assertThat(builder.getMemberName()).isEqualTo("memberOne");
    assertThat(builder.getPid()).isNull();
    assertThat(builder.getPort().intValue()).isEqualTo(12345);
    assertThat(builder.getWorkingDirectory()).isEqualTo(getWorkingDirectoryPath());
  }

  @Test
  public void getWorkingDirectoryReturnsCurrentDirectoryByDefault() {
    // given:

    // when: not setting WorkingDirectory

    // then: getDirectory returns default
    assertThat(new Builder().getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToNullUsesCurrentDirectory() {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to null
    assertThat(builder.setWorkingDirectory(null)).isSameAs(builder);

    // then: getDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToEmptyStringUsesCurrentDirectory() {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to empty string
    assertThat(builder.setWorkingDirectory("")).isSameAs(builder);

    // then: getDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToBlankStringUsesCurrentDirectory() {
    // given: a new builder
    Builder builder = new Builder();

    // when: setting WorkingDirectory to white space
    assertThat(builder.setWorkingDirectory("  ")).isSameAs(builder);

    // then: getDirectory returns default
    assertThat(builder.getWorkingDirectory()).isEqualTo(CURRENT_DIRECTORY);
  }

  @Test
  public void setWorkingDirectoryToExistingDirectory() {
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
    Throwable thrown =
        catchThrowable(() -> new Builder().setWorkingDirectory(nonDirectory.getCanonicalPath()));

    // then: throw IllegalArgumentException
    then(thrown).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(workingDirectoryNotFoundErrorMessage())
        .hasCause(new FileNotFoundException(nonDirectory.getCanonicalPath()));
  }

  @Test
  public void setWorkingDirectoryToNonExistingDirectory() {
    // given:

    // when: setting WorkingDirectory to non-existing directory
    Throwable thrown =
        catchThrowable(() -> new Builder().setWorkingDirectory("/path/to/non_existing/directory"));

    // then: throw IllegalArgumentException
    then(thrown).isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(workingDirectoryNotFoundErrorMessage())
        .hasCause(new FileNotFoundException("/path/to/non_existing/directory"));
  }

  @Test
  public void portCanBeOverriddenBySystemProperty() {
    // given: overridden default port
    int overriddenPort = getRandomAvailableTCPPort();
    System.setProperty(TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(overriddenPort));

    // when: creating new LocatorLauncher
    LocatorLauncher launcher = new Builder().build();

    // then: locator port should be the overridden default port
    assertThat(launcher.getPort()).isEqualTo(overriddenPort);
  }

  private String memberNameValidationErrorMessage() {
    return String.format(
        AbstractLauncher.MEMBER_NAME_ERROR_MESSAGE,
        "Locator", "Locator");
  }

  private String workingDirectoryOptionNotValidErrorMessage() {
    return String.format(
        AbstractLauncher.WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE,
        "Locator", "Locator");
  }

  private String workingDirectoryNotFoundErrorMessage() {
    return String.format(AbstractLauncher.WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE,
        "Locator");
  }

  private File getWorkingDirectory() {
    return temporaryFolder.getRoot();
  }

  private String getWorkingDirectoryPath() {
    try {
      return getWorkingDirectory().getCanonicalPath();
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
