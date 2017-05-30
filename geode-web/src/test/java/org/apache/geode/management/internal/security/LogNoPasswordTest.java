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
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

@Category(IntegrationTest.class)
public class LogNoPasswordTest {

  private static String PASSWORD = "abcdefghijklmn";
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withProperty(LOG_LEVEL, "DEBUG")
      .withSecurityManager(MySecurityManager.class);

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Test
  public void testPasswordInLogs() throws Exception {
    locator.startLocator();
    gfsh.secureConnectAndVerify(locator.getHttpPort(), GfshShellConnectionRule.PortType.http, "any",
        PASSWORD);
    gfsh.executeAndVerifyCommand("list members");

    // scan all log files to find any occurrences of password
    File[] logFiles = locator.getWorkingDir().listFiles(file -> file.toString().endsWith(".log"));
    for (File logFile : logFiles) {
      Scanner scanner = new Scanner(logFile);
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        assertThat(line).doesNotContain(PASSWORD);
      }
    }
  }

  public static class MySecurityManager implements SecurityManager {
    @Override
    public Object authenticate(Properties credentials) throws AuthenticationFailedException {
      String user = credentials.getProperty("security-username");
      String password = credentials.getProperty("security-password");
      if (PASSWORD.equals(password)) {
        return user;
      }

      throw new AuthenticationFailedException("Not authenticated.");
    }
  }
}
