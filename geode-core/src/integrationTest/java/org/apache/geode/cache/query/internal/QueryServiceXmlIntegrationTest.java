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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.cli.util.TestMethodAuthorizer;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class QueryServiceXmlIntegrationTest {

  @Rule
  public ServerStarterRule serverRule = new ServerStarterRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private static final String RESTRICTED_AUTHORIZER_XML =
      "QueryServiceWithRestrictedAuthorizer.xml";
  private static final String UNRESTRICTED_AUTHORIZER_XML =
      "QueryServiceWithUnrestrictedAuthorizer.xml";
  private static final String JAVA_BEAN_AUTHORIZER_XML = "QueryServiceWithJavaBeanAuthorizer.xml";
  private static final String REGEX_AUTHORIZER_XML = "QueryServiceWithRegExAuthorizer.xml";
  private static final String USER_AUTHORIZER_XML = "QueryServiceWithUserDefinedAuthorizer.xml";
  private static final String INVALID_AUTHORIZER_XML = "QueryServiceWithInvalidAuthorizer.xml";

  @Test
  public void queryServiceUsesRestrictedMethodAuthorizerWithNoQueryServiceInXmlAndSecurityEnabled() {
    serverRule.withSecurityManager(SimpleSecurityManager.class).startServer();

    MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();

    assertThat(authorizer).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  @Parameters({RESTRICTED_AUTHORIZER_XML, UNRESTRICTED_AUTHORIZER_XML, JAVA_BEAN_AUTHORIZER_XML,
      REGEX_AUTHORIZER_XML})
  @TestCaseName("{method} Authorizer Xml={0}")
  public void queryServiceUsesNoOpAuthorizerWithAuthorizerSpecifiedInXmlAndSecurityDisabled(
      final String xmlFile) throws IOException {
    String cacheXmlFilePath = getFilePath(xmlFile);
    serverRule.withProperty(CACHE_XML_FILE, cacheXmlFilePath).startServer();

    MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();
    assertThat(authorizer).isEqualTo(QueryConfigurationServiceImpl.getNoOpAuthorizer());
  }

  @Test
  @TestCaseName("{method} Authorizer={1}")
  @Parameters(method = "authorizerXmlAndClassParams")
  public void queryServiceWithAuthorizerCanBeLoadedFromXml(final String xmlFile,
      final Class expectedAuthorizerClass) throws IOException {
    String cacheXmlFilePath = getFilePath(xmlFile);
    serverRule.withProperty(CACHE_XML_FILE, cacheXmlFilePath)
        .withSecurityManager(SimpleSecurityManager.class).startServer();

    MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();
    assertThat(authorizer.getClass()).isEqualTo(expectedAuthorizerClass);
  }

  @Test
  @Parameters({JAVA_BEAN_AUTHORIZER_XML, REGEX_AUTHORIZER_XML})
  public void queryServiceWithParameterCanBeLoadedFromXml(final String xmlFile)
      throws IOException, NoSuchMethodException {
    String cacheXmlFilePath = getFilePath(xmlFile);
    serverRule.withProperty(CACHE_XML_FILE, cacheXmlFilePath)
        .withSecurityManager(SimpleSecurityManager.class).startServer();

    MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();

    // Verify that the parameters specified in the xml have been loaded correctly.
    String testString = "testString";
    Method allowedMethod = String.class.getMethod("isEmpty");

    assertThat(authorizer.authorize(allowedMethod, testString)).isTrue();

    List testList = new ArrayList();
    Method disallowedMethod = List.class.getMethod("isEmpty");

    assertThat(authorizer.authorize(disallowedMethod, testList)).isFalse();
  }

  @Test
  public void queryServiceWithUserDefinedAuthorizerCanBeLoadedFromXml()
      throws IOException, NoSuchMethodException {
    String cacheXmlFilePath = getFilePath(USER_AUTHORIZER_XML);
    serverRule.withProperty(CACHE_XML_FILE, cacheXmlFilePath)
        .withSecurityManager(SimpleSecurityManager.class).startServer();

    MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();

    assertThat(authorizer).isInstanceOf(TestMethodAuthorizer.class);

    String testString = "testString";
    Method allowedMethod = String.class.getMethod("toString");
    assertThat(authorizer.authorize(allowedMethod, testString)).isTrue();

    Method disallowedMethod = String.class.getMethod("isEmpty");
    assertThat(authorizer.authorize(disallowedMethod, testString)).isFalse();
  }

  @Test
  public void queryServiceXmlWithInvalidAuthorizerDoesNotChangeAuthorizer() throws IOException {
    String cacheXmlFilePath = getFilePath(INVALID_AUTHORIZER_XML);
    assertThatThrownBy(() -> serverRule.withProperty(CACHE_XML_FILE, cacheXmlFilePath)
        .withSecurityManager(SimpleSecurityManager.class).startServer())
            .isInstanceOf(QueryConfigurationServiceException.class);
  }

  private MethodInvocationAuthorizer getMethodInvocationAuthorizer() {
    return serverRule.getCache().getInternalQueryService().getMethodInvocationAuthorizer();
  }

  private String getFilePath(String fileName) throws IOException {
    URL url = getClass().getResource(fileName);
    File cacheXmlFile = this.temporaryFolder.newFile(fileName);
    FileUtils.copyURLToFile(url, cacheXmlFile);

    return cacheXmlFile.getAbsolutePath();
  }

  @SuppressWarnings("unused")
  private Object[] authorizerXmlAndClassParams() {
    return new Object[] {
        new Object[] {RESTRICTED_AUTHORIZER_XML, RestrictedMethodAuthorizer.class},
        new Object[] {UNRESTRICTED_AUTHORIZER_XML, UnrestrictedMethodAuthorizer.class},
        new Object[] {JAVA_BEAN_AUTHORIZER_XML, JavaBeanAccessorMethodAuthorizer.class},
        new Object[] {REGEX_AUTHORIZER_XML, RegExMethodAuthorizer.class},
    };
  }
}
