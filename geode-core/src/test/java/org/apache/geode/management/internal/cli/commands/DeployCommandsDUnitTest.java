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
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;

/**
 * Unit tests for the DeployCommands class
 * 
 * @since GemFire 7.0
 */
@SuppressWarnings("serial")
@Category(DistributedTest.class)
public class DeployCommandsDUnitTest implements Serializable {
  private static final String GROUP1 = "Group1";
  private static final String GROUP2 = "Group2";

  private final String class1 = "DeployCommandsDUnitA";
  private final String class2 = "DeployCommandsDUnitB";
  private final String class3 = "DeployCommandsDUnitC";
  private final String class4 = "DeployCommandsDUnitD";

  private final String jarName1 = "DeployCommandsDUnit1.jar";
  private final String jarName2 = "DeployCommandsDUnit2.jar";
  private final String jarName3 = "DeployCommandsDUnit3.jar";
  private final String jarName4 = "DeployCommandsDUnit4.jar";

  private File jar1;
  private File jar2;
  private File jar3;
  private File jar4;
  private File subdirWithJars3and4;

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public transient GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  @Before
  public void setup() throws Exception {
    getHost(0).getVM(1).bounce();
    getHost(0).getVM(2).bounce();

    ClassBuilder classBuilder = new ClassBuilder();
    File jarsDir = lsRule.getTempFolder().newFolder();
    jar1 = new File(jarsDir, jarName1);
    jar2 = new File(jarsDir, jarName2);

    subdirWithJars3and4 = new File(jarsDir, "subdir");
    subdirWithJars3and4.mkdirs();
    jar3 = new File(subdirWithJars3and4, jarName3);
    jar4 = new File(subdirWithJars3and4, jarName4);

    classBuilder.writeJarFromName(class1, jar1);
    classBuilder.writeJarFromName(class2, jar2);
    classBuilder.writeJarFromName(class3, jar3);
    classBuilder.writeJarFromName(class4, jar4);

    locator = lsRule.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty(GROUPS, GROUP1);
    server1 = lsRule.startServerVM(1, props, locator.getPort());

    props.setProperty(GROUPS, GROUP2);
    server2 = lsRule.startServerVM(2, props, locator.getPort());

    gfshConnector.connectAndVerify(locator);
  }

  @Test
  public void deployJarToOneGroup() throws Exception {
    // Deploy a jar to a single group
    CommandResult cmdResult =
        gfshConnector.executeAndVerifyCommand("deploy --jar=" + jar2 + " --group=" + GROUP1);
    String resultString = gfshConnector.getGfshOutput();

    assertThat(resultString).contains(server1.getName());
    assertThat(resultString).doesNotContain(server2.getName());
    assertThat(resultString).contains(jarName2);

    server1.invoke(() -> assertThatCanLoad(jarName2, class2));
    server2.invoke(() -> assertThatCannotLoad(jarName2, class2));
  }

  @Test
  public void deployJarsInDirToOneGroup() throws Exception {
    // Deploy of multiple JARs to a single group
    gfshConnector.executeAndVerifyCommand(
        "deploy --group=" + GROUP1 + " --dir=" + subdirWithJars3and4.getCanonicalPath());
    String resultString = gfshConnector.getGfshOutput();

    assertThat(resultString).describedAs(resultString).contains(server1.getName());
    assertThat(resultString).doesNotContain(server2.getName());
    assertThat(resultString).contains(jarName3);
    assertThat(resultString).contains(jarName4);

    server1.invoke(() -> {
      assertThatCanLoad(jarName3, class3);
      assertThatCanLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });

    // Undeploy of multiple jars by specifying group
    gfshConnector.executeAndVerifyCommand("undeploy --group=" + GROUP1);
    server1.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
  }

  @Test
  public void deployMultipleJarsToOneGroup() throws Exception {
    // Deploy of multiple JARs to a single group
    gfshConnector.executeAndVerifyCommand("deploy --group=" + GROUP1 + " --jars="
        + jar3.getAbsolutePath() + "," + jar4.getAbsolutePath());
    String resultString = gfshConnector.getGfshOutput();

    assertThat(resultString).describedAs(resultString).contains(server1.getName());
    assertThat(resultString).doesNotContain(server2.getName());
    assertThat(resultString).contains(jarName3);
    assertThat(resultString).contains(jarName4);

    server1.invoke(() -> {
      assertThatCanLoad(jarName3, class3);
      assertThatCanLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });

    // Undeploy of multiple jars by specifying group
    gfshConnector.executeAndVerifyCommand("undeploy --jars=" + jarName3 + "," + jarName4);
    server1.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
  }


  @Test
  public void deployJarToAllServers() throws Exception {
    // Deploy a jar to all servers
    gfshConnector.executeAndVerifyCommand("deploy --jar=" + jar1);
    String resultString = gfshConnector.getGfshOutput();

    assertThat(resultString).contains(server1.getName());
    assertThat(resultString).contains(server2.getName());
    assertThat(resultString).contains(jarName1);

    server1.invoke(() -> assertThatCanLoad(jarName1, class1));
    server2.invoke(() -> assertThatCanLoad(jarName1, class1));

    // Undeploy of jar by specifying group
    gfshConnector.executeAndVerifyCommand("undeploy --group=" + GROUP1);
    server1.invoke(() -> assertThatCannotLoad(jarName1, class1));
    server2.invoke(() -> assertThatCanLoad(jarName1, class1));
  }

  @Test
  public void deployMultipleJarsToAllServers() throws Exception {
    gfshConnector.executeAndVerifyCommand("deploy --dir=" + subdirWithJars3and4.getCanonicalPath());

    server1.invoke(() -> {
      assertThatCanLoad(jarName3, class3);
      assertThatCanLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCanLoad(jarName3, class3);
      assertThatCanLoad(jarName4, class4);
    });

    gfshConnector.executeAndVerifyCommand("undeploy");

    server1.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
  }

  @Test
  public void undeployOfMultipleJars() throws Exception {
    gfshConnector.executeAndVerifyCommand("deploy --dir=" + subdirWithJars3and4.getCanonicalPath());

    server1.invoke(() -> {
      assertThatCanLoad(jarName3, class3);
      assertThatCanLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCanLoad(jarName3, class3);
      assertThatCanLoad(jarName4, class4);
    });

    gfshConnector
        .executeAndVerifyCommand("undeploy --jar=" + jar3.getName() + "," + jar4.getName());
    server1.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
    server2.invoke(() -> {
      assertThatCannotLoad(jarName3, class3);
      assertThatCannotLoad(jarName4, class4);
    });
  }

  private void assertThatCanLoad(String jarName, String className) throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().getJarDeployer().findDeployedJar(jarName)).isNotNull();
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }

  private void assertThatCannotLoad(String jarName, String className) {
    assertThat(ClassPathLoader.getLatest().getJarDeployer().findDeployedJar(jarName)).isNull();
    assertThatThrownBy(() -> ClassPathLoader.getLatest().forName(className))
        .isExactlyInstanceOf(ClassNotFoundException.class);
  }


  @Test
  public void testListDeployed() throws Exception {
    // Deploy a couple of JAR files which can be listed
    gfshConnector
        .executeAndVerifyCommand("deploy --group=" + GROUP1 + " --jar=" + jar1.getCanonicalPath());
    gfshConnector
        .executeAndVerifyCommand("deploy --group=" + GROUP2 + " --jar=" + jar2.getCanonicalPath());

    // List for all members
    gfshConnector.executeAndVerifyCommand("list deployed");
    String resultString = gfshConnector.getGfshOutput();
    assertThat(resultString).contains(server1.getName());
    assertThat(resultString).contains(server2.getName());
    assertThat(resultString).contains(jarName1);
    assertThat(resultString).contains(jarName2);

    // List for members in Group1
    gfshConnector.executeAndVerifyCommand("list deployed --group=" + GROUP1);
    resultString = gfshConnector.getGfshOutput();
    assertThat(resultString).contains(server1.getName());
    assertThat(resultString).doesNotContain(server2.getName());

    assertThat(resultString).contains(jarName1);
    assertThat(resultString).doesNotContain(jarName2);

    // List for members in Group2
    gfshConnector.executeAndVerifyCommand("list deployed --group=" + GROUP2);
    resultString = gfshConnector.getGfshOutput();
    assertThat(resultString).doesNotContain(server1.getName());
    assertThat(resultString).contains(server2.getName());

    assertThat(resultString).doesNotContain(jarName1);
    assertThat(resultString).contains(jarName2);
  }
}
