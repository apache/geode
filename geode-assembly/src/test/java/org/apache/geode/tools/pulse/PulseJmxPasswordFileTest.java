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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.HttpClientRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;


@Category({IntegrationTest.class, SecurityTest.class, PulseTest.class})
public class PulseJmxPasswordFileTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule();

  @Rule
  public HttpClientRule client = new HttpClientRule(locator::getHttpPort);

  @Rule
  public TemporaryFolder temporaryFolder =
      new TemporaryFolder(StringUtils.isBlank(System.getProperty("java.io.tmpdir")) ? null
          : new File(System.getProperty("java.io.tmpdir")));

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

    HttpResponse response = client.post("/pulse/pulseUpdate");
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
  }
}
