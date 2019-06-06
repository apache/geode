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
package org.apache.geode.internal.statistics;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.InternalGemFireException;

public class OSVerifierTest {

  private static String currentOsName;

  @Rule
  public ExpectedException exceptionGrabber = ExpectedException.none();

  @BeforeClass
  public static void saveOsName() {
    currentOsName = System.getProperty("os.name");
  }

  @After
  public void restoreOsName() {
    System.setProperty("os.name", currentOsName);
  }

  @Test
  public void givenLinuxOs_thenOSVerifierObjectCanBeBuilt() {
    System.setProperty("os.name", "Linux");
    new OSVerifier();
  }

  @Test
  public void givenNonLinuxOs_thenOSVerifierObjectCanBeBuilt() {
    System.setProperty("os.name", "NonLinux");
    exceptionGrabber.expect(InternalGemFireException.class);
    new OSVerifier();
  }

}
