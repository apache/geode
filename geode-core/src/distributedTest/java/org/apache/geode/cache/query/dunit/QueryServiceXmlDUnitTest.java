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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.cache.util.TestMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@RunWith(JUnitParamsRunner.class)
public class QueryServiceXmlDUnitTest {
  @Rule
  public ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private static final String RESTRICTED_AUTHORIZER_XML =
      "QueryServiceWithRestrictedAuthorizer.xml";
  private static final String UNRESTRICTED_AUTHORIZER_XML =
      "QueryServiceWithUnrestrictedAuthorizer.xml";
  private static final String JAVA_BEAN_AUTHORIZER_XML = "QueryServiceWithJavaBeanAuthorizer.xml";
  private static final String REGEX_AUTHORIZER_XML = "QueryServiceWithRegExAuthorizer.xml";
  private static final String USER_AUTHORIZER_XML = "QueryServiceWithUserDefinedAuthorizer.xml";
  private static final String TEST_AUTHORIZER_TXT = "TestMethodAuthorizer.txt";

  @Test
  public void queryServiceUsesRestrictedMethodAuthorizerWithNoQueryServiceInXmlAndSecurityEnabled() {
    MemberVM server = clusterRule.startServerVM(1,
        s -> s.withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName()));

    server.invoke(() -> {
      MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();
      assertThat(authorizer).isInstanceOf(RestrictedMethodAuthorizer.class);
    });
  }

  @Test
  @Parameters({RESTRICTED_AUTHORIZER_XML, UNRESTRICTED_AUTHORIZER_XML, JAVA_BEAN_AUTHORIZER_XML,
      REGEX_AUTHORIZER_XML})
  @TestCaseName("{method} Authorizer Xml={0}")
  public void queryServiceUsesNoOpAuthorizerWithAuthorizerSpecifiedInXmlAndSecurityDisabled(
      final String xmlFile) throws IOException {
    String cacheXmlFilePath = getFilePath(xmlFile);
    MemberVM server =
        clusterRule.startServerVM(1, s -> s.withProperty(CACHE_XML_FILE, cacheXmlFilePath));

    server.invoke(() -> {
      MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();
      assertThat(authorizer).isEqualTo(QueryConfigurationServiceImpl.getNoOpAuthorizer());
    });
  }

  @Test
  @TestCaseName("{method} Authorizer={1}")
  @Parameters(method = "authorizerXmlAndClassParams")
  public void queryServiceWithAuthorizerCanBeLoadedFromXml(final String xmlFile,
      final Class expectedAuthorizerClass) throws IOException {
    String cacheXmlFilePath = getFilePath(xmlFile);
    MemberVM server =
        clusterRule.startServerVM(1, s -> s.withProperty(CACHE_XML_FILE, cacheXmlFilePath)
            .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName()));

    server.invoke(() -> {
      MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();
      assertThat(authorizer.getClass()).isEqualTo(expectedAuthorizerClass);
    });
  }

  @Test
  @Parameters({JAVA_BEAN_AUTHORIZER_XML, REGEX_AUTHORIZER_XML})
  public void queryServiceWithParameterCanBeLoadedFromXml(final String xmlFile) throws IOException {
    String cacheXmlFilePath = getFilePath(xmlFile);
    MemberVM server =
        clusterRule.startServerVM(1, s -> s.withProperty(CACHE_XML_FILE, cacheXmlFilePath)
            .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName()));

    server.invoke(() -> {
      MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();

      // Verify that the parameters specified in the xml have been loaded correctly.
      String testString = "testString";
      Method allowedMethod = String.class.getMethod("isEmpty");

      assertThat(authorizer.authorize(allowedMethod, testString)).isTrue();

      List testList = new ArrayList();
      Method disallowedMethod = List.class.getMethod("isEmpty");

      assertThat(authorizer.authorize(disallowedMethod, testList)).isFalse();
    });
  }

  @Test
  public void queryServiceWithUserDefinedAuthorizerCanBeLoadedFromXml() throws IOException {
    // First start a locator so that the server will be able to obtain the TestMethodAuthorizer
    // class from it when it starts
    MemberVM locator =
        clusterRule.startLocatorVM(0, LocatorStarterRule::withoutClusterConfigurationService);

    String className = TestMethodAuthorizer.class.getName();

    String classContent =
        new String(Files.readAllBytes(Paths.get(getFilePath(TEST_AUTHORIZER_TXT))));

    File workingDir = locator.getWorkingDir();
    File jarFile = new File(workingDir.getAbsolutePath() + "testJar.jar");

    // Write a jar containing the TestMethodAuthorizer class (parsed from TestMthodAuthorizer.txt)
    // to the locator's working directory
    new ClassBuilder().writeJarFromContent(className, classContent, jarFile);

    String cacheXmlFilePath = getFilePath(USER_AUTHORIZER_XML);

    MemberVM server = clusterRule.startServerVM(2, s -> s.withConnectionToLocator(locator.getPort())
        .withProperty(CACHE_XML_FILE, cacheXmlFilePath)
        .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName()));

    server.invoke(() -> {
      MethodInvocationAuthorizer authorizer = getMethodInvocationAuthorizer();

      assertThat(authorizer).isInstanceOf(TestMethodAuthorizer.class);

      String testString = "testString";
      Method allowedMethod = String.class.getMethod("toString");
      assertThat(authorizer.authorize(allowedMethod, testString)).isTrue();

      Method disallowedMethod = String.class.getMethod("isEmpty");
      assertThat(authorizer.authorize(disallowedMethod, testString)).isFalse();
    });
  }

  private static MethodInvocationAuthorizer getMethodInvocationAuthorizer() {
    return Objects.requireNonNull(ClusterStartupRule.getCache()).getQueryService()
        .getMethodInvocationAuthorizer();
  }

  private String getFilePath(String fileName) throws IOException {
    URL url = getClass().getResource(fileName);
    File cacheXmlFile = this.temporaryFolder.newFile(fileName);
    FileUtils.copyURLToFile(url, cacheXmlFile);

    return cacheXmlFile.getAbsolutePath();
  }

  private Object[] authorizerXmlAndClassParams() {
    return new Object[] {
        new Object[] {RESTRICTED_AUTHORIZER_XML, RestrictedMethodAuthorizer.class},
        new Object[] {UNRESTRICTED_AUTHORIZER_XML, UnrestrictedMethodAuthorizer.class},
        new Object[] {JAVA_BEAN_AUTHORIZER_XML, JavaBeanAccessorMethodAuthorizer.class},
        new Object[] {REGEX_AUTHORIZER_XML, RegExMethodAuthorizer.class},
    };
  }
}
