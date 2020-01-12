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

package org.apache.geode.tools.pulse;

import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;

import java.io.File;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


@Category({SecurityTest.class, PulseTest.class})
public class PulseJmxPasswordFileTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withHttpService();

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(locator::getHttpPort);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    File passwordFile = temporaryFolder.newFile("password.properties");
    FileUtils.writeLines(passwordFile, Collections.singleton("user user"));
    locator.withProperty("jmx-manager-password-file", passwordFile.getAbsolutePath());
    locator.startLocator();
  }

  @Test
  public void testLogin() throws Exception {
    client.loginToPulseAndVerify("user", "user");

    assertResponse(client.post("/pulse/pulseUpdate")).hasStatusCode(200);
  }
}
