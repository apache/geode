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
package org.apache.geode.codeAnalysis;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.test.junit.rules.ClassAnalysisRule;

public class RestrictUseOfInetAddressJUnitTest {

  @Rule
  public ClassAnalysisRule classProvider = new ClassAnalysisRule(getModuleName());


  public String getModuleName() {
    return "geode-core";
  }

  @AfterClass
  public static void afterClass() {
    ClassAnalysisRule.clearCache();
  }


  @Test
  public void restrictUseOfInetAddressMethods() {
    Map<String, CompiledClass> classes = classProvider.getClasses();
    Set<String> exclusions = getExclusions();

    assertExcludedClassesExist(classes, exclusions);

    StringWriter writer = new StringWriter();

    final String className = "java/net/InetAddress";
    List<String> methodNames = Arrays.asList(
        "getByName",
        "getAllByName");

    assertExclusionsUseMethods(classes, exclusions, className, methodNames);
    searchForMethodUse(classes, exclusions, writer, className, methodNames);

    // use an assertion on the StringWriter rather than the failure count so folks can
    // tell what failed
    String actual = writer.toString();
    String expected = "";
    assertThat(actual)
        .withFailMessage(
            "Unexpected use of restricted InetAddress methods need to be sanctioned by this test.\n"
                + "Use of these methods can cause off-platform errors when using a service gateway.\n"
                + actual)
        .isEqualTo(expected);
  }

  /**
   * These classes are known to use restricted methods and references to those methods
   * will not cause the test to fail.
   */
  private HashSet<String> getExclusions() {
    return new HashSet<>(Arrays.asList(
        // old admin API
        "org/apache/geode/admin/GemFireMemberStatus",
        "org/apache/geode/admin/internal/DistributionLocatorImpl",
        "org/apache/geode/admin/internal/GemFireHealthImpl",
        "org/apache/geode/admin/internal/InetAddressUtils",
        "org/apache/geode/admin/jmx/internal/AdminDistributedSystemJmxImpl",
        "org/apache/geode/admin/jmx/internal/RMIServerSocketFactoryImpl",
        "org/apache/geode/internal/admin/remote/FetchHostResponse",
        // gfsh server-side launchers
        "org/apache/geode/distributed/LocatorLauncher$Builder",
        "org/apache/geode/distributed/ServerLauncher$Builder",
        // server-side locators/mcast-port processing
        "org/apache/geode/distributed/internal/AbstractDistributionConfig",
        "org/apache/geode/internal/AbstractConfig",
        // server-side communications
        "org/apache/geode/distributed/internal/direct/DirectChannel",
        "org/apache/geode/internal/AvailablePort",
        "org/apache/geode/internal/cache/tier/sockets/AcceptorImpl",
        // old command-line tool replaced by gfsh
        "org/apache/geode/internal/DistributionLocator",
        "org/apache/geode/internal/SystemAdmin",
        // server-side locator parsing
        "org/apache/geode/internal/admin/remote/DistributionLocatorId",
        // sanctioned comms class used by every communications component
        "org/apache/geode/internal/net/SCAdvancedSocketCreator",
        // new management API
        "org/apache/geode/management/internal/JmxManagerLocatorResponse",
        "org/apache/geode/management/internal/ManagementAgent"));
  }

  /**
   * Ensure that all the CompiledClass names in the given exclusions list are present
   * in the given map.
   */
  private void assertExcludedClassesExist(Map<String, CompiledClass> classes,
      Set<String> exclusions) {
    for (String exclusion : exclusions) {
      CompiledClass excludedClass = classes.get(exclusion);
      assertThat(excludedClass)
          .withFailMessage("test needs to be updated as " + exclusion + " could not be found")
          .isNotNull();
    }
  }

  /**
   * Ensure that sanctioned exclusions still refer to one or more of the given methods
   */
  private void assertExclusionsUseMethods(Map<String, CompiledClass> classes,
      Set<String> exclusions,
      String classOrInterface,
      List<String> methods) {
    for (String className : exclusions) {
      CompiledClass compiledClass = classes.get(className);
      boolean foundOne = false;
      for (String methodName : methods) {
        if (compiledClass.refersToMethod(classOrInterface, methodName)) {
          foundOne = true;
          break;
        }
      }
      assertThat(foundOne)
          .withFailMessage(
              className + " no longer uses restricted methods for " + classOrInterface);
    }
  }

  /**
   * Search for uses of the given methods in the given classOrInterface. Note these
   * uses with the given StringWriter on separate lines.
   */
  private void searchForMethodUse(Map<String, CompiledClass> classes, Set<String> exclusions,
      StringWriter writer,
      String classOrInterface,
      List<String> methods) {
    System.out
        .println("Looking for unsanctioned uses of " + classOrInterface + " in " + getModuleName());
    final List<String> keys = new ArrayList<>(classes.keySet());
    Collections.sort(keys);
    for (String className : keys) {
      if (exclusions.contains(className)) {
        continue;
      }
      CompiledClass compiledClass = classes.get(className);
      for (String methodName : methods) {
        if (compiledClass.refersToMethod(classOrInterface, methodName)) {
          writer.append(className).append(" uses ").append(methodName).append("\n");
        }
      }
    }
  }
}
