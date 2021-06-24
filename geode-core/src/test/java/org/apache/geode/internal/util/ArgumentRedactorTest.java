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

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.internal.util.ArgumentRedactor.REDACTED;
import static org.apache.geode.internal.util.ArgumentRedactor.isTaboo;
import static org.apache.geode.internal.util.ArgumentRedactor.redact;
import static org.apache.geode.internal.util.ArgumentRedactor.redactEachInList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.internal.logging.Banner;

public class ArgumentRedactorTest {

  private static final String someProperty = "redactorTest.someProperty";
  private static final String somePasswordProperty = "redactorTest.aPassword";
  private static final String someOtherPasswordProperty =
      "redactorTest.aPassword-withCharactersAfterward";

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void theseLinesShouldRedact() {
    String argumentThatShouldBeRedacted = "__this_should_be_redacted__";
    List<String> someTabooOptions =
        asList("-Dgemfire.password=" + argumentThatShouldBeRedacted,
            "--password=" + argumentThatShouldBeRedacted,
            "--J=-Dgemfire.some.very.qualified.item.password=" + argumentThatShouldBeRedacted,
            "--J=-Dsysprop-secret.information=" + argumentThatShouldBeRedacted);

    List<String> fullyRedacted = redactEachInList(someTabooOptions);

    assertThat(fullyRedacted)
        .doesNotContainAnyElementsOf(someTabooOptions);
  }

  @Test
  public void redactorWillIdentifySampleTabooProperties() {
    List<String> shouldBeRedacted = asList("gemfire.security-password", "password",
        "other-password-option", CLUSTER_SSL_TRUSTSTORE_PASSWORD, GATEWAY_SSL_TRUSTSTORE_PASSWORD,
        SERVER_SSL_KEYSTORE_PASSWORD, "security-username", "security-manager",
        "security-important-property", "javax.net.ssl.keyStorePassword",
        "javax.net.ssl.some.security.item", "javax.net.ssl.keyStoreType", "sysprop-secret-prop");

    for (String option : shouldBeRedacted) {
      assertThat(isTaboo(option))
          .describedAs("This option should be identified as taboo: " + option)
          .isTrue();
    }
  }

  @Test
  public void redactorWillAllowSampleMiscProperties() {
    List<String> shouldNotBeRedacted = asList("gemfire.security-manager",
        CLUSTER_SSL_ENABLED, CONSERVE_SOCKETS, "username", "just-an-option");

    for (String option : shouldNotBeRedacted) {
      assertThat(isTaboo(option))
          .describedAs("This option should not be identified as taboo: " + option)
          .isFalse();
    }
  }

  @Test
  public void argListOfPasswordsAllRedact() {
    Collection<String> args = new ArrayList<>();
    args.add("--gemfire.security-password=secret");
    args.add("--login-password=secret");
    args.add("--gemfire-password = super-secret");
    args.add("--geode-password= confidential");
    args.add("--some-other-password =shhhh");
    args.add("--justapassword =failed");

    String redacted = redact(args);

    assertThat(redacted).contains("--gemfire.security-password=********");
    assertThat(redacted).contains("--login-password=********");
    assertThat(redacted).contains("--gemfire-password = ********");
    assertThat(redacted).contains("--geode-password= ********");
    assertThat(redacted).contains("--some-other-password =********");
    assertThat(redacted).contains("--justapassword =********");
  }

  @Test
  public void argListOfPasswordsAllRedactViaRedactEachInList() {
    Collection<String> args = new ArrayList<>();
    args.add("--gemfire.security-password=secret");
    args.add("--login-password=secret");
    args.add("--gemfire-password = super-secret");
    args.add("--geode-password= confidential");
    args.add("--some-other-password =shhhh");
    args.add("--justapassword =failed");

    List<String> redacted = redactEachInList(args);

    assertThat(redacted).contains("--gemfire.security-password=********");
    assertThat(redacted).contains("--login-password=********");
    assertThat(redacted).contains("--gemfire-password = ********");
    assertThat(redacted).contains("--geode-password= ********");
    assertThat(redacted).contains("--some-other-password =********");
    assertThat(redacted).contains("--justapassword =********");
  }

  @Test
  public void argListOfMiscOptionsDoNotRedact() {
    Collection<String> args = new ArrayList<>();
    args.add("--gemfire.security-properties=./security.properties");
    args.add("--gemfire.sys.security-option=someArg");
    args.add("--gemfire.use-cluster-configuration=true");
    args.add("--someotherstringoption");
    args.add("--login-name=admin");
    args.add("--myArg --myArg2 --myArg3=-arg4");
    args.add("--myArg --myArg2 --myArg3=\"-arg4\"");

    String redacted = redact(args);

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
    String arg1 = "-Dgemfire.security-password=secret";
    assertThat(redact(arg1)).endsWith("password=********");

    String arg2 = "--J=-Dsome.highly.qualified.password=secret";
    assertThat(redact(arg2)).endsWith("password=********");

    String arg3 = "--password=foo";
    assertThat(redact(arg3)).isEqualToIgnoringWhitespace("--password=********");

    String arg4 = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";
    assertThat(redact(arg4)).isEqualTo(arg4);
  }

  @Test
  public void miscIndividualOptionsDoNotRedact() {
    String arg1 = "-Dgemfire.security-properties=./security-properties";
    assertThat(redact(arg1)).isEqualTo(arg1);

    String arg2 = "-J-Dgemfire.sys.security-option=someArg";
    assertThat(redact(arg2)).isEqualTo(arg2);

    String arg3 = "-Dgemfire.sys.option=printable";
    assertThat(redact(arg3)).isEqualTo(arg3);

    String arg4 = "-Dgemfire.use-cluster-configuration=true";
    assertThat(redact(arg4)).isEqualTo(arg4);

    String arg5 = "someotherstringoption";
    assertThat(redact(arg5)).isEqualTo(arg5);

    String arg6 = "--classpath=.";
    assertThat(redact(arg6)).isEqualTo(arg6);
  }

  @Test
  public void oneOfManyArgumentsIsRedacted() {
    String arg = "-DmyArg -Duser-password=foo --classpath=.";

    assertThat(redact(arg)).isEqualTo("-DmyArg -Duser-password=******** --classpath=.");
  }

  @Test
  public void twoOfManyArgumentsAreRedacted() {
    String arg = "-DmyArg -Duser-password=foo -DOtherArg -Dsystem-password=bar";

    assertThat(redact(arg))
        .isEqualTo("-DmyArg -Duser-password=******** -DOtherArg -Dsystem-password=********");
  }

  @Test
  public void multipleOfManyArgumentsAreRedacted() {
    String arg =
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
  }

  @Test
  public void redactMultipleScriptLines() {
    assertThat(redact("connect --test-password=test --product-password=test1"))
        .isEqualTo("connect --test-password=******** --product-password=********");
  }

  @Test
  public void systemPropertiesGetRedactedInBanner() {
    System.setProperty(someProperty, "isNotRedacted");
    System.setProperty(somePasswordProperty, "isRedacted");
    System.setProperty(someOtherPasswordProperty, "isRedacted");
    List<String> args = asList("--user=me", "--password=isRedacted",
        "--another-password-for-some-reason =isRedacted", "--yet-another-password = isRedacted");
    String banner = new Banner().getString(args.toArray(new String[0]));

    assertThat(banner).doesNotContain("isRedacted");
  }

  @Test
  public void redactsArgumentForSslTruststorePassword() {
    String prefix = "-Dgemfire.ssl-truststore-password=";
    String password = "gibberish";
    String value = redact(prefix + password);

    assertThat(value).isEqualTo(prefix + REDACTED);
  }

  @Test
  public void redactsArgumentForSslKeystorePassword() {
    String prefix = "-Dgemfire.ssl-keystore-password=";
    String password = "gibberish";
    String value = redact(prefix + password);

    assertThat(value).isEqualTo(prefix + REDACTED);
  }

  @Test
  public void redactsArgumentEndingWithHyphen() {
    String argBeginningWithMinus = "-Dgemfire.ssl-keystore-password=supersecret-";
    String redacted = redact(argBeginningWithMinus);

    assertThat(redacted).isEqualTo("-Dgemfire.ssl-keystore-password=********");
  }

  @Test
  public void redactsArgumentContainingHyphen() {
    String argBeginningWithMinus = "-Dgemfire.ssl-keystore-password=super-secret";
    String redacted = redact(argBeginningWithMinus);

    assertThat(redacted).isEqualTo("-Dgemfire.ssl-keystore-password=********");
  }

  @Test
  public void redactsArgumentContainingMultipleHyphen() {
    String argBeginningWithMinus = "-Dgemfire.ssl-keystore-password=this-is-super-secret";
    String redacted = redact(argBeginningWithMinus);

    assertThat(redacted).isEqualTo("-Dgemfire.ssl-keystore-password=********");
  }

  @Test
  public void doesNotRedactUnquotedArgumentBeginningWithHyphen() {
    String argBeginningWithMinus = "-Dgemfire.ssl-keystore-password=-supersecret";
    String redacted = redact(argBeginningWithMinus);

    assertThat(redacted).isEqualTo(argBeginningWithMinus);
  }
}
