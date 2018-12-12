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
package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class, SecurityTest.class, LoggingTest.class})
public class LogNoPasswordIntegrationTest {

  private static final String PASSWORD = "abcdefghijklmn";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule().withLogFile();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void testPasswordInLogs() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(LOG_LEVEL, "debug");
    properties.setProperty(SECURITY_MANAGER, MySecurityManager.class.getName());
    MemberVM locator =
        lsRule.startLocatorVM(0, l -> l.withHttpService().withProperties(properties));
    gfsh.secureConnectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http, "any",
        PASSWORD);
    gfsh.executeAndAssertThat("list members").statusIsSuccess();

    // scan all locator log files to find any occurrences of password
    File[] serverLogFiles =
        locator.getWorkingDir().listFiles(file -> file.toString().endsWith(".log"));
    File[] gfshLogFiles = gfsh.getWorkingDir().listFiles(file -> file.toString().endsWith(".log"));

    File[] logFiles = (File[]) ArrayUtils.addAll(serverLogFiles, gfshLogFiles);

    for (File logFile : logFiles) {
      Scanner scanner = new Scanner(logFile);
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        assertThat(line).describedAs("File: %s, Line: %s", logFile.getAbsolutePath(), line)
            .doesNotContain(PASSWORD);
      }
    }
  }

  public static class MySecurityManager implements SecurityManager {

    @Override
    public Object authenticate(Properties properties) throws AuthenticationFailedException {
      String user = properties.getProperty("security-username");
      String password = properties.getProperty("security-password");
      if (PASSWORD.equals(password)) {
        return user;
      }

      throw new AuthenticationFailedException("Not authenticated.");
    }
  }
}
