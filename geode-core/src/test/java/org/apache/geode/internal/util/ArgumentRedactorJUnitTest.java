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

package org.apache.geode.internal.util;

import static org.apache.geode.internal.util.ArgumentRedactor.redact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ArgumentRedactor Tester.
 */
@Category(UnitTest.class)
public class ArgumentRedactorJUnitTest {
  @Test
  public void testRedactArgList() throws Exception {
    List<String> argList = new ArrayList<>();
    argList.add("gemfire.security-password=secret");
    argList.add("gemfire.security-properties=./security.properties");
    argList.add("gemfire.sys.security-value=someValue");
    argList.add("gemfire.use-cluster-configuration=true");
    argList.add("someotherstringvalue");
    argList.add("login-password=secret");
    argList.add("login-name=admin");
    argList.add("gemfire-password = super-secret");
    argList.add("geode-password= confidential");
    argList.add("some-other-password =shhhh");
    String redacted = redact(argList);
    assertTrue(redacted.contains("gemfire.security-password=********"));
    assertTrue(redacted.contains("gemfire.security-properties=./security.properties"));
    assertTrue(redacted.contains("gemfire.sys.security-value=someValue"));
    assertTrue(redacted.contains("gemfire.use-cluster-configuration=true"));
    assertTrue(redacted.contains("someotherstringvalue"));
    assertTrue(redacted.contains("login-password=********"));
    assertTrue(redacted.contains("login-name=admin"));
    assertTrue(redacted.contains("gemfire-password=********"));
    assertTrue(redacted.contains("geode-password=********"));
    assertTrue(redacted.contains("some-other-password=********"));
  }

  @Test
  public void testRedactMap() throws Exception {
    Map<String, String> argMap = new HashMap<>();
    argMap.put("gemfire.security-password", "secret");
    argMap.put("gemfire.security-properties", "./security.properties");
    argMap.put("gemfire.sys.security-value", "someValue");
    argMap.put("gemfire.use-cluster-configuration", "true");
    argMap.put("login-password", "secret");
    argMap.put("login-name", "admin");
    String redacted = redact(argMap);
    assertTrue(redacted.contains("gemfire.security-password=********"));
    assertTrue(redacted.contains("gemfire.security-properties=./security.properties"));
    assertTrue(redacted.contains("gemfire.sys.security-value=someValue"));
    assertTrue(redacted.contains("gemfire.use-cluster-configuration=true"));
    assertTrue(redacted.contains("login-password=********"));
    assertTrue(redacted.contains("login-name=admin"));
  }

  @Test
  public void testRedactArg() throws Exception {
    String arg = "-Dgemfire.security-password=secret";
    assertTrue(redact(arg).endsWith("password=********"));

    arg = "-Dgemfire.security-properties=./security-properties";
    assertEquals(arg, (redact(arg)));

    arg = "-J-Dgemfire.sys.security-value=someValue";
    assertEquals(arg, (redact(arg)));

    arg = "-Dgemfire.sys.value=printable";
    assertEquals(arg, redact(arg));

    arg = "-Dgemfire.use-cluster-configuration=true";
    assertEquals(arg, redact(arg));

    arg = "someotherstringvalue";
    assertEquals(arg, redact(arg));

    arg = "--password=foo";
    assertEquals("--password=********", redact(arg));

    arg = "--classpath=.";
    assertEquals(arg, redact(arg));

    arg = "-DmyArg -Duser-password=foo --classpath=.";
    assertEquals("-DmyArg -Duser-password=******** --classpath=.", redact(arg));

    arg = "-DmyArg -Duser-password=foo -DOtherArg -Dsystem-password=bar";
    assertEquals("-DmyArg -Duser-password=******** -DOtherArg -Dsystem-password=********",
        redact(arg));

    arg =
        "-Dlogin-password=secret -Dlogin-name=admin -Dgemfire-password = super-secret --geode-password= confidential -J-Dsome-other-password =shhhh";
    String redacted = redact(arg);
    assertTrue(redacted.contains("login-password=********"));
    assertTrue(redacted.contains("login-name=admin"));
    assertTrue(redacted.contains("gemfire-password=********"));
    assertTrue(redacted.contains("geode-password=********"));
    assertTrue(redacted.contains("some-other-password=********"));

    arg = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";
    assertEquals(arg, (redact(arg)));
  }
}
