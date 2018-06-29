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

package org.apache.geode.rest.internal.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({RestAPITest.class, DistributedTest.class})
public class RestFunctionExecuteDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(3);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static JarBuilder jarBuilder = new JarBuilder();
  private static MemberVM locator, server1, server2;

  private GeodeRestClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // prepare the jar to deploy
    File jarsToDeploy = new File(gfsh.getWorkingDir(), "function.jar");
    jarBuilder.buildJar(jarsToDeploy, loadClassToFile());


    Properties locatorProps = new Properties();
    locatorProps.put(ConfigurationProperties.SECURITY_MANAGER,
        SimpleSecurityManager.class.getName());
    locator = cluster.startLocatorVM(0, locatorProps);

    Properties props = new Properties();
    props.put(ConfigurationProperties.START_DEV_REST_API, "true");
    props.put("security-username", "cluster");
    props.put("security-password", "cluster");
    props.put(ConfigurationProperties.GROUPS, "group1");
    server1 = cluster.startServerVM(1, props, locator.getPort());

    props.put(ConfigurationProperties.GROUPS, "group2");
    server2 = cluster.startServerVM(2, props, locator.getPort());

    gfsh.connectAndVerify(locator);

    // deploy the function only to server1
    gfsh.executeAndAssertThat("deploy --jar=" + jarsToDeploy.getAbsolutePath() + " --group=group1")
        .statusIsSuccess();
  }

  @Test
  public void connectToServer1() throws Exception {
    client = new GeodeRestClient("localhost", server1.getHttpPort());
    HttpResponse response = client.doPost("/functions/myTestFunction", "dataRead", "dataRead", "");
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(403);

    // function can't be executed on all members since it's only deployed on server1
    response = client.doPost("/functions/myTestFunction", "dataManage", "dataManage", "");
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(500);

    // function can't be executed on server2
    response = client.doPost("/functions/myTestFunction?onMembers=server-2", "dataManage",
        "dataManage", "");
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(500);

    // function can only be executed on server1 only
    response = client.doPost("/functions/myTestFunction?onMembers=server-1", "dataManage",
        "dataManage", "");
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(200);
  }

  @Test
  public void connectToServer2() throws Exception {
    // function is deployed on server1
    client = new GeodeRestClient("localhost", server2.getHttpPort());
    HttpResponse response = client.doPost("/functions/myTestFunction", "dataRead", "dataRead", "");
    assertThat(GeodeRestClient.getCode(response)).isEqualTo(404);
  }

  // find ImplementsFunction.java in the geode-core resource
  private static File loadClassToFile() throws URISyntaxException {
    URL resourceFileURL = Function.class.getClassLoader()
        .getResource("org/apache/geode/management/internal/deployment/ImplementsFunction.java");
    assertThat(resourceFileURL).isNotNull();

    URI resourceUri = resourceFileURL.toURI();
    return new File(resourceUri);
  }


}
