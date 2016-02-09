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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Unit tests for configuring user commands.
 *
 * @author David Hoots
 * @since 8.0
 */
public class UserCommandsDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;
  final File jarDirectory = new File(
      (new File(ClassPathLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath())).getParent(),
      "ext");
  final File jarFile = new File(this.jarDirectory, "UserCommandsDUnit.jar");
  boolean deleteJarDirectory = false;

  public UserCommandsDUnitTest(String name) throws Exception {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createUserCommandJarFile();
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    if (this.deleteJarDirectory) {
      FileUtil.delete(this.jarDirectory);
    } else {
      FileUtil.delete(this.jarFile);
    }

    System.clearProperty(CommandManager.USER_CMD_PACKAGES_PROPERTY);
    ClassPathLoader.setLatestToDefault();
    CommandManager.clearInstance();

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        System.clearProperty(CommandManager.USER_CMD_PACKAGES_PROPERTY);
        ClassPathLoader.setLatestToDefault();
        CommandManager.clearInstance();
      }
    });
  }

  public void createUserCommandJarFile() throws IOException {
    this.deleteJarDirectory = this.jarDirectory.mkdir();

    StringBuffer stringBuffer = new StringBuffer();

    stringBuffer.append("package junit.ucdunit;");
    stringBuffer.append("import org.springframework.shell.core.CommandMarker;");
    stringBuffer.append("import org.springframework.shell.core.annotation.CliAvailabilityIndicator;");
    stringBuffer.append("import org.springframework.shell.core.annotation.CliCommand;");
    stringBuffer.append("import org.springframework.shell.core.annotation.CliOption;");
    stringBuffer.append("import com.gemstone.gemfire.management.cli.Result;");
    stringBuffer.append("import com.gemstone.gemfire.management.internal.cli.CliUtil;");
    stringBuffer.append("import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;");
    stringBuffer.append("import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;");

    stringBuffer.append("public final class UCDunitClass implements CommandMarker { public UCDunitClass() {}");
    stringBuffer.append("@CliCommand(value = { \"ucdunitcmd\" }, help = \"ucdunitcmd help\")");
    stringBuffer.append(
        "public final Result ucdunitcmd(@CliOption(key = { \"name\" }, help = \"ucdunitcmd name help\") String name) {");
    stringBuffer.append("return ResultBuilder.createInfoResult(\"ucdunitcmd \" + name); }");
    stringBuffer.append("@CliAvailabilityIndicator({ \"ucdunitcmd\" })");
    stringBuffer.append("public final boolean isAvailable() { return true; } }");

    ClassBuilder classBuilder = new ClassBuilder();
    final byte[] jarBytes = classBuilder.createJarFromClassContent("junit/ucdunit/UCDunitClass",
        stringBuffer.toString());

    final FileOutputStream outStream = new FileOutputStream(this.jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }

  @Test
  public void testCommandLineProperty() {
    System.setProperty(CommandManager.USER_CMD_PACKAGES_PROPERTY, "junit.ucdunit");

    ClassPathLoader.setLatestToDefault();
    CommandManager.clearInstance();

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        System.setProperty(CommandManager.USER_CMD_PACKAGES_PROPERTY, "junit.ucdunit");
        ClassPathLoader.setLatestToDefault();
        CommandManager.clearInstance();
      }
    });

    createDefaultSetup(null);

    CommandResult cmdResult = executeCommand("ucdunitcmd");
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  @Test
  public void testGemFireProperty() {
    System.setProperty(CommandManager.USER_CMD_PACKAGES_PROPERTY, "junit.ucdunit");

    ClassPathLoader.setLatestToDefault();
    CommandManager.clearInstance();

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      private static final long serialVersionUID = 1L;

      @Override
      public void run() {
        ClassPathLoader.setLatestToDefault();
        CommandManager.clearInstance();
      }
    });

    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.USER_COMMAND_PACKAGES, "junit.ucdunit");
    createDefaultSetup(properties);

    CommandResult cmdResult = executeCommand("ucdunitcmd");
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }
}
