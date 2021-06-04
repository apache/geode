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

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.internal.util.ArgumentRedactor.isTaboo;
import static org.apache.geode.internal.util.ArgumentRedactor.redact;
import static org.apache.geode.internal.util.ArgumentRedactor.redactEachInList;
import static org.apache.geode.internal.util.ArgumentRedactor.redacted;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.geode.internal.logging.Banner;

public class ArgumentRedactorTest {
  private static final String someProperty = "redactorTest.someProperty";
  private static final String somePasswordProperty = "redactorTest.aPassword";
  private static final String someOtherPasswordProperty =
      "redactorTest.aPassword-withCharactersAfterward";

  @Test
  public void theseLinesShouldRedact() {
    String argumentThatShouldBeRedacted = "__this_should_be_redacted__";
    List<String> someTabooOptions =
        Arrays.asList("-Dgemfire.password=" + argumentThatShouldBeRedacted,
            "--password=" + argumentThatShouldBeRedacted,
            "--J=-Dgemfire.some.very.qualified.item.password=" + argumentThatShouldBeRedacted,
            "--J=-Dsysprop-secret.information=" + argumentThatShouldBeRedacted);

    List<String> fullyRedacted = redactEachInList(someTabooOptions);
    assertThat(fullyRedacted).doesNotContainAnyElementsOf(someTabooOptions);
  }

  @Test
  public void redactorWillIdentifySampleTabooProperties() {
    List<String> shouldBeRedacted = Arrays.asList("gemfire.security-password", "password",
        "other-password-option", CLUSTER_SSL_TRUSTSTORE_PASSWORD, GATEWAY_SSL_TRUSTSTORE_PASSWORD,
        SERVER_SSL_KEYSTORE_PASSWORD, "security-username", "security-manager",
        "security-important-property", "javax.net.ssl.keyStorePassword",
        "javax.net.ssl.some.security.item", "javax.net.ssl.keyStoreType", "sysprop-secret-prop");
    for (String option : shouldBeRedacted) {
      assertThat(isTaboo(option))
          .describedAs("This option should be identified as taboo: " + option).isTrue();
    }
  }

  @Test
  public void redactorWillAllowSampleMiscProperties() {
    List<String> shouldNotBeRedacted = Arrays.asList("gemfire.security-manager",
        CLUSTER_SSL_ENABLED, CONSERVE_SOCKETS, "username", "just-an-option");
    for (String option : shouldNotBeRedacted) {
      assertThat(isTaboo(option))
          .describedAs("This option should not be identified as taboo: " + option).isFalse();
    }
  }

  @Test
  public void argListOfPasswordsAllRedact() {
    List<String> argList = new ArrayList<>();
    argList.add("--gemfire.security-password=secret");
    argList.add("--login-password=secret");
    argList.add("--gemfire-password = super-secret");
    argList.add("--geode-password= confidential");
    argList.add("--some-other-password =shhhh");
    argList.add("--justapassword =failed");
    String redacted = redact(argList);
    assertThat(redacted).contains("--gemfire.security-password=********");
    assertThat(redacted).contains("--login-password=********");
    assertThat(redacted).contains("--gemfire-password = ********");
    assertThat(redacted).contains("--geode-password= ********");
    assertThat(redacted).contains("--some-other-password =********");
    assertThat(redacted).contains("--justapassword =********");
  }

  @Test
  public void argListOfPasswordsAllRedactViaRedactEachInList() {
    List<String> argList = new ArrayList<>();
    argList.add("--gemfire.security-password=secret");
    argList.add("--login-password=secret");
    argList.add("--gemfire-password = super-secret");
    argList.add("--geode-password= confidential");
    argList.add("--some-other-password =shhhh");
    argList.add("--justapassword =failed");
    List<String> redacted = redactEachInList(argList);
    assertThat(redacted).contains("--gemfire.security-password=********");
    assertThat(redacted).contains("--login-password=********");
    assertThat(redacted).contains("--gemfire-password = ********");
    assertThat(redacted).contains("--geode-password= ********");
    assertThat(redacted).contains("--some-other-password =********");
    assertThat(redacted).contains("--justapassword =********");
  }


  @Test
  public void argListOfMiscOptionsDoNotRedact() {
    List<String> argList = new ArrayList<>();
    argList.add("--gemfire.security-properties=./security.properties");
    argList.add("--gemfire.sys.security-option=someArg");
    argList.add("--gemfire.use-cluster-configuration=true");
    argList.add("--someotherstringoption");
    argList.add("--login-name=admin");
    argList.add("--myArg --myArg2 --myArg3=-arg4");
    argList.add("--myArg --myArg2 --myArg3=\"-arg4\"");
    String redacted = redact(argList);
    assertThat(redacted).contains("--gemfire.security-properties=./security.properties");
    assertThat(redacted).contains("--gemfire.sys.security-option=someArg");
    assertThat(redacted).contains("--gemfire.use-cluster-configuration=true");
    assertThat(redacted).contains("--someotherstringoption");
    assertThat(redacted).contains("--login-name=admin");
    assertThat(redacted).contains("--myArg --myArg2 --myArg3=-arg4");
    assertThat(redacted).contains("--myArg --myArg2 --myArg3=\"-arg4\"");
  }

  @Test
  public void protectedIndividualOptionsRedact() {
    String arg;

    arg = "-Dgemfire.security-password=secret";
    assertThat(redact(arg)).endsWith("password=********");

    arg = "--J=-Dsome.highly.qualified.password=secret";
    assertThat(redact(arg)).endsWith("password=********");

    arg = "--password=foo";
    assertThat(redact(arg)).isEqualToIgnoringWhitespace("--password=********");

    arg = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";
    assertThat(redact(arg)).isEqualTo(arg);
  }

  @Test
  public void miscIndividualOptionsDoNotRedact() {
    String arg;

    arg = "-Dgemfire.security-properties=./security-properties";
    assertThat(redact(arg)).isEqualTo(arg);

    arg = "-J-Dgemfire.sys.security-option=someArg";
    assertThat(redact(arg)).isEqualTo(arg);

    arg = "-Dgemfire.sys.option=printable";
    assertThat(redact(arg)).isEqualTo(arg);

    arg = "-Dgemfire.use-cluster-configuration=true";
    assertThat(redact(arg)).isEqualTo(arg);

    arg = "someotherstringoption";
    assertThat(redact(arg)).isEqualTo(arg);

    arg = "--classpath=.";
    assertThat(redact(arg)).isEqualTo(arg);
  }

  @Test
  public void wholeLinesAreProperlyRedacted() {
    String arg;
    arg = "-DmyArg -Duser-password=foo --classpath=.";
    assertThat(redact(arg)).isEqualTo("-DmyArg -Duser-password=******** --classpath=.");

    arg = "-DmyArg -Duser-password=foo -DOtherArg -Dsystem-password=bar";
    assertThat(redact(arg))
        .isEqualTo("-DmyArg -Duser-password=******** -DOtherArg -Dsystem-password=********");

    arg =
        "-Dlogin-password=secret -Dlogin-name=admin -Dgemfire-password = super-secret --geode-password= confidential -J-Dsome-other-password =shhhh";
    String redacted = redact(arg);
    assertThat(redacted).contains("login-password=********");
    assertThat(redacted).contains("login-name=admin");
    assertThat(redacted).contains("gemfire-password = ********");
    assertThat(redacted).contains("geode-password= ********");
    assertThat(redacted).contains("some-other-password =********");
  }

  @Test
  public void redactScriptLine() {
    assertThat(redact("connect --password=test --user=test"))
        .isEqualTo("connect --password=******** --user=test");

    assertThat(redact("connect --test-password=test --product-password=test1"))
        .isEqualTo("connect --test-password=******** --product-password=********");
  }

  @Test
  public void systemPropertiesGetRedactedInBanner() {
    try {
      System.setProperty(someProperty, "isNotRedacted");
      System.setProperty(somePasswordProperty, "isRedacted");
      System.setProperty(someOtherPasswordProperty, "isRedacted");

      List<String> args = ArrayUtils.asList("--user=me", "--password=isRedacted",
          "--another-password-for-some-reason =isRedacted", "--yet-another-password = isRedacted");
      String banner = new Banner().getString(args.toArray(new String[0]));
      assertThat(banner).doesNotContain("isRedacted");
    } finally {
      System.clearProperty(someProperty);
      System.clearProperty(somePasswordProperty);
      System.clearProperty(someOtherPasswordProperty);
    }
  }

  @Test
  public void redactsArgumentForSslTruststorePassword() {
    String prefix = "-Dgemfire.ssl-truststore-password=";
    String password = "gibberish";
    String value = redact(prefix + password);

    assertThat(value).isEqualTo(prefix + redacted);
  }

  @Test
  public void redactsArgumentForSslKeystorePassword() {
    String prefix = "-Dgemfire.ssl-keystore-password=";
    String password = "gibberish";
    String value = redact(prefix + password);

    assertThat(value).isEqualTo(prefix + redacted);
  }

  @Test
  public void testArgBeginningWithMinus() {
    String argBeginningWithMinus = "-Dgemfire.ssl-keystore-password=-supersecret";
    String redacted = redact(argBeginningWithMinus);
    assertThat(redacted).isEqualTo("-Dgemfire.ssl-keystore-password=********");
  }
}
