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

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class TomcatSessionBackwardsCompatibilityTomcat8WithOldModulesMixedWithCurrentCanDoPutFromCurrentModuleTest
    extends TomcatSessionBackwardsCompatibilityTestBase {

  public TomcatSessionBackwardsCompatibilityTomcat8WithOldModulesMixedWithCurrentCanDoPutFromCurrentModuleTest(
      String version) {
    super(version);
  }

  @Test
  public void test() throws Exception {
    startClusterWithTomcat(classPathTomcat8);
    manager.addContainer(tomcat8AndCurrentModules);
    manager.addContainer(tomcat8AndOldModules);
    doPutAndGetSessionOnAllClients();
  }
}
