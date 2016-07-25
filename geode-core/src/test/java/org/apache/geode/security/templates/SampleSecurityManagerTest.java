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
package org.apache.geode.security.templates;

import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.apache.geode.security.templates.SampleSecurityManager.Role;
import org.apache.geode.security.templates.SampleSecurityManager.User;

@Category({ IntegrationTest.class, SecurityTest.class })
public class SampleSecurityManagerTest {

  private SampleSecurityManager sampleSecurityManager;
  private String jsonResource;
  private File jsonFile;
  private String json;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    // resource file
    this.jsonResource = "org/apache/geode/security/templates/security.json";
    InputStream inputStream = ClassLoader.getSystemResourceAsStream(this.jsonResource);

    assertThat(inputStream).isNotNull();

    // non-resource file
    this.jsonFile = new File(temporaryFolder.getRoot(), "security.json");
    IOUtils.copy(inputStream, new FileOutputStream(this.jsonFile));

    // string
    this.json = FileUtils.readFileToString(this.jsonFile, "UTF-8");
    this.sampleSecurityManager = new SampleSecurityManager();
  }

  @Test
  public void shouldInitializeFromJsonString() throws Exception {
    this.sampleSecurityManager.initializeFromJson(this.json);
    verifySecurityManagerState();
  }

  @Test
  public void shouldInitializeFromJsonResource() throws Exception {
    this.sampleSecurityManager.initializeFromJsonResource(this.jsonResource);
    verifySecurityManagerState();
  }

  @Test
  public void shouldInitializeFromJsonFile() throws Exception {
    this.sampleSecurityManager.initializeFromJsonFile(this.jsonFile);
    verifySecurityManagerState();
  }

  @Test
  public void initShouldUsePropertyAsJsonString() throws Exception {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(SampleSecurityManager.SECURITY_JSON, this.json);
    this.sampleSecurityManager.init(securityProperties);
    verifySecurityManagerState();
  }

  @Test
  public void initShouldUsePropertyAsJsonFile() throws Exception {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(SampleSecurityManager.SECURITY_JSON, this.jsonFile.getAbsolutePath());
    this.sampleSecurityManager.init(securityProperties);
    verifySecurityManagerState();
  }

  @Test
  public void initShouldUsePropertyAsJsonResource() throws Exception {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(SampleSecurityManager.SECURITY_JSON, this.jsonResource);
    this.sampleSecurityManager.init(securityProperties);
    verifySecurityManagerState();
  }

  private void verifySecurityManagerState() {
    User adminUser = this.sampleSecurityManager.getUser("admin");
    assertThat(adminUser).isNotNull();
    assertThat(adminUser.name).isEqualTo("admin");
    assertThat(adminUser.password).isEqualTo("secret");
    assertThat(adminUser.roles).hasSize(1);

    User guestUser = this.sampleSecurityManager.getUser("guest");
    assertThat(guestUser).isNotNull();
    assertThat(guestUser.name).isEqualTo("guest");
    assertThat(guestUser.password).isEqualTo("guest");
    assertThat(guestUser.roles).hasSize(1);
    // TODO: need to do more verification
  }
}
