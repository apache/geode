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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.Banner;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * ArgumentRedactor Tester.
 */
@Category(UnitTest.class)
public class ArgumentRedactorJUnitTest {
  private static final Logger logger = LogService.getLogger();
  private static final String someProperty = "redactorTest.someProperty";
  private static final String somePasswordProperty = "redactorTest.aPassword";
  private static final String someOtherPasswordProperty =
      "redactorTest.aPassword-withCharactersAfterward";

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
    assertThat(redacted).contains("gemfire.security-password=********");
    assertThat(redacted).contains("gemfire.security-properties=./security.properties");
    assertThat(redacted).contains("gemfire.sys.security-value=someValue");
    assertThat(redacted).contains("gemfire.use-cluster-configuration=true");
    assertThat(redacted).contains("someotherstringvalue");
    assertThat(redacted).contains("login-password=********");
    assertThat(redacted).contains("login-name=admin");
    assertThat(redacted).contains("gemfire-password = ********");
    assertThat(redacted).contains("geode-password= ********");
    assertThat(redacted).contains("some-other-password =********");
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
    assertThat(redacted).contains("login-password=********");
    assertThat(redacted).contains("login-name=admin");
    assertThat(redacted).contains("gemfire-password = ********");
    assertThat(redacted).contains("geode-password= ********");
    assertThat(redacted).contains("some-other-password =********");

    arg = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";
    assertEquals(arg, (redact(arg)));
  }

  @Test
  public void redactScriptLine() throws Exception {
    assertThat(redact("connect --password=test --user=test"))
        .isEqualTo("connect --password=******** --user=test");

    assertThat(redact("connect --test-password=test --product-password=test1"))
        .isEqualTo("connect --test-password=******** --product-password=********");
  }

  @Test
  public void systemPropertiesGetRedactedInBanner() throws Exception {
    try {
      System.setProperty(someProperty, "isNotRedacted");
      System.setProperty(somePasswordProperty, "isRedacted");
      System.setProperty(someOtherPasswordProperty, "isRedacted");

      List<String> args = ArrayUtils.asList("--user=me", "--password=isRedacted",
          "--another-password-for-some-reason =isRedacted", "--yet-another-password = isRedacted");
      String banner = Banner.getString(args.toArray(new String[0]));
      assertThat(banner).doesNotContain("isRedacted");
    } finally {
      System.clearProperty(someProperty);
      System.clearProperty(somePasswordProperty);
      System.clearProperty(someOtherPasswordProperty);
    }
  }
}
