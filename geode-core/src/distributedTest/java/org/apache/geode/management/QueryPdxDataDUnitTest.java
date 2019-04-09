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
package org.apache.geode.management;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;

import net.openhft.compiler.CompilerUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class})
public class QueryPdxDataDUnitTest {

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static ClientVM client;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Invoke.invokeInEveryVM(
        () -> System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true"));

    locator = cluster.startLocatorVM(0);
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        "configure pdx --read-serialized=true --auto-serializable-classes=org.apache.geode.management.*")
        .statusIsSuccess();

    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    gfsh.executeAndAssertThat("create region --name=BOZ --type=PARTITION");

    int port = locator.getPort();
    client = cluster.startClientVM(3, cf -> {
      cf.withLocatorConnection(port);
      cf.withCacheSetup(c -> c
          .setPdxSerializer(new ReflectionBasedAutoSerializer("org.apache.geode.management.*")));
    });

    client.invoke(() -> {
      ClientRegionFactory factory =
          ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<Object, Object> region = factory.create("BOZ");

      buildClass("org.apache.geode.management.Address",
          "/org/apache/geode/management/Address.java");
      Class<?> customerClass =
          buildClass("org.apache.geode.management.Customer",
              "/org/apache/geode/management/Customer.java");

      Constructor<?> constructor =
          customerClass.getConstructor(String.class, String.class, String.class);
      for (int i = 0; i < 100; i++) {
        Object customer = constructor.newInstance("name_" + i, "Main " + i, "City " + i);
        region.put(i + "", customer);
      }
    });
  }

  private static Class<?> buildClass(String className, String javaResourceName) throws Exception {
    URL resourceFileURL = QueryPdxDataDUnitTest.class.getResource(javaResourceName);
    assertThat(resourceFileURL).isNotNull();

    URI resourceUri = resourceFileURL.toURI();
    String javaCode = new String(Files.readAllBytes(new File(resourceUri).toPath()));

    return CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
  }

  @Test
  public void queryForPdxBackedEntriesShouldSucceed() {
    gfsh.executeAndAssertThat("query --query=\"select * from /BOZ.values\"").statusIsSuccess();
  }

}
