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

import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;

import java.io.File;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


public class JmxPasswordFileTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void connectWhenJmxManagerSecuredWithPasswordFile() throws Exception {
    File passwordFile = temporaryFolder.newFile("password.properties");
    FileUtils.writeLines(passwordFile, Collections.singleton("user user"));
    locator.withProperty("jmx-manager-password-file", passwordFile.getAbsolutePath());

    locator.startLocator();

    gfsh.secureConnectAndVerify(locator.getJmxPort(), jmxManager, "user", "user");
  }
}
