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
package org.apache.geode.session.tests;

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class TomcatSessionBackwardsCompatibilityTomcat9WithOldModulesMixedWithCurrentCanDoPutFromCurrentModuleTest
    extends TomcatSessionBackwardsCompatibilityTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    // The Tomcat 9 module was not added until Geode 1.8.0
    String minimumVersion = "1.8.0";
    result.removeIf(s -> TestVersion.compare(s, minimumVersion) < 0);
    return result;
  }

  public TomcatSessionBackwardsCompatibilityTomcat9WithOldModulesMixedWithCurrentCanDoPutFromCurrentModuleTest(
      String version) {
    super(version);
  }

  @Test
  public void test() throws Exception {
    startClusterWithTomcat(classPathTomcat9);
    manager.addContainer(tomcat9AndCurrentModules);
    manager.addContainer(tomcat9AndOldModules);
    doPutAndGetSessionOnAllClients();
  }
}
