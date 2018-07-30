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
package org.apache.geode.security;

import static org.assertj.core.api.Assertions.assertThat;

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

import org.apache.geode.examples.security.ExampleSecurityManager;
import org.apache.geode.examples.security.ExampleSecurityManager.User;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class ExampleSecurityManagerTest {

  private ExampleSecurityManager exampleSecurityManager;
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
    this.exampleSecurityManager = new ExampleSecurityManager();
  }

  @Test
  public void shouldDefaultToSecurityJsonInClasspathIfNullProperties() throws Exception {
    this.exampleSecurityManager.init(null);
    verifySecurityManagerState();
  }

  @Test
  public void shouldDefaultToSecurityJsonInClasspathIfEmptyProperties() throws Exception {
    this.exampleSecurityManager.init(new Properties());
    verifySecurityManagerState();
  }

  @Test
  public void shouldInitializeFromJsonResource() throws Exception {
    this.exampleSecurityManager.initializeFromJsonResource(this.jsonResource);
    verifySecurityManagerState();
  }

  @Test
  public void initShouldUsePropertyAsJsonResource() throws Exception {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(TestSecurityManager.SECURITY_JSON, this.jsonResource);
    this.exampleSecurityManager.init(securityProperties);
    verifySecurityManagerState();
  }

  @Test
  public void userThatDoesNotExistInJson() throws Exception {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(TestSecurityManager.SECURITY_JSON, this.jsonResource);
    this.exampleSecurityManager.init(securityProperties);
    ResourcePermission permission =
        new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DISK);
    assertThat(exampleSecurityManager.authorize("diskWriter", permission)).isFalse();
  }

  @Test
  public void roleThatDoesNotExistInJson() throws Exception {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(TestSecurityManager.SECURITY_JSON, this.jsonResource);
    this.exampleSecurityManager.init(securityProperties);
    ResourcePermission permission =
        new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, Target.DISK);
    assertThat(exampleSecurityManager.authorize("phantom", permission)).isFalse();
  }

  private void verifySecurityManagerState() {
    User adminUser = this.exampleSecurityManager.getUser("admin");
    assertThat(adminUser).isNotNull();
    assertThat(adminUser.getName()).isEqualTo("admin");
    assertThat(adminUser.getPassword()).isEqualTo("secret");
    assertThat(adminUser.getRoles()).hasSize(1);

    User guestUser = this.exampleSecurityManager.getUser("guest");
    assertThat(guestUser).isNotNull();
    assertThat(guestUser.getName()).isEqualTo("guest");
    assertThat(guestUser.getPassword()).isEqualTo("guest");
    assertThat(guestUser.getRoles()).hasSize(1);
    // TODO: need to do more verification
  }
}
