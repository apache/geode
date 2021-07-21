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

import static org.apache.geode.internal.util.ArgumentRedactor.getRedacted;
import static org.apache.geode.internal.util.ArgumentRedactor.isSensitive;
import static org.apache.geode.internal.util.ArgumentRedactor.redact;
import static org.apache.geode.internal.util.ArgumentRedactor.redactEachInList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

public class ArgumentRedactorTest {

  @Test
  public void isSensitive_isTrueForGemfireSecurityPassword() {
    String input = "gemfire.security-password";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForPassword() {
    String input = "password";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionContainingPassword() {
    String input = "other-password-option";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForClusterSslTruststorePassword() {
    String input = "cluster-ssl-truststore-password";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForGatewaySslTruststorePassword() {
    String input = "gateway-ssl-truststore-password";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForServerSslKeystorePassword() {
    String input = "server-ssl-keystore-password";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForSecurityUsername() {
    String input = "security-username";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForSecurityManager() {
    String input = "security-manager";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionStartingWithSecurityHyphen() {
    String input = "security-important-property";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForJavaxNetSslKeyStorePassword() {
    String input = "javax.net.ssl.keyStorePassword";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionStartingWithJavaxNetSsl() {
    String input = "javax.net.ssl.some.security.item";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForJavaxNetSslKeyStoreType() {
    String input = "javax.net.ssl.keyStoreType";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isTrueForOptionStartingWithSyspropHyphen() {
    String input = "sysprop-secret-prop";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isTrue();
  }

  @Test
  public void isSensitive_isFalseForGemfireSecurityManager() {
    String input = "gemfire.security-manager";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForClusterSslEnabled() {
    String input = "cluster-ssl-enabled";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForConserveSockets() {
    String input = "conserve-sockets";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForUsername() {
    String input = "username";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void isSensitive_isFalseForNonMatchingStringContainingHyphens() {
    String input = "just-an-option";

    boolean output = isSensitive(input);

    assertThat(output)
        .as("output of isSensitive(" + input + ")")
        .isFalse();
  }

  @Test
  public void redactString_redactsGemfirePasswordWithHyphenD() {
    String string = "-Dgemfire.password=%s";
    String sensitive = "__this_should_be_redacted__";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsPasswordWithHyphens() {
    String string = "--password=%s";
    String sensitive = "__this_should_be_redacted__";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsOptionEndingWithPasswordWithHyphensJDd() {
    String string = "--J=-Dgemfire.some.very.qualified.item.password=%s";
    String sensitive = "__this_should_be_redacted__";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsOptionStartingWithSyspropHyphenWithHyphensJD() {
    String string = "--J=-Dsysprop-secret.information=%s";
    String sensitive = "__this_should_be_redacted__";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsGemfireSecurityPasswordWithHyphenD() {
    String string = "-Dgemfire.security-password=%s";
    String sensitive = "secret";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(expected);
  }

  @Test
  public void redactString_doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD1() {
    String input = "-Dgemfire.security-properties=argument-value";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD2() {
    String input = "-Dgemfire.security-properties=\"c:\\Program Files (x86)\\My Folder\"";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactOptionEndingWithSecurityPropertiesWithHyphenD3() {
    String input = "-Dgemfire.security-properties=./security-properties";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactOptionContainingSecurityHyphenWithHyphensJD() {
    String input = "--J=-Dgemfire.sys.security-option=someArg";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactNonMatchingGemfireOptionWithHyphenD() {
    String input = "-Dgemfire.sys.option=printable";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_redactsGemfireUseClusterConfigurationWithHyphenD() {
    String input = "-Dgemfire.use-cluster-configuration=true";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_returnsNonMatchingString() {
    String input = "someotherstringoption";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_doesNotRedactClasspathWithHyphens() {
    String input = "--classpath=.";

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .isEqualTo(input);
  }

  @Test
  public void redactString_redactsMatchingOptionWithNonMatchingOptionAndFlagAndMultiplePrefixes() {
    String string = "--J=-Dflag -Duser-password=%s --classpath=.";
    String sensitive = "foo";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMultipleMatchingOptionsWithFlags() {
    String string = "-DmyArg -Duser-password=%s -DOtherArg -Dsystem-password=%s";
    String sensitive1 = "foo";
    String sensitive2 = "bar";
    String input = String.format(string, sensitive1, sensitive2);
    String expected = String.format(string, getRedacted(), getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMultipleMatchingOptionsWithMultipleNonMatchingOptionsAndMultiplePrefixes() {
    String string =
        "-Dlogin-password=%s -Dlogin-name=%s -Dgemfire-password = %s --geode-password= %s --J=-Dsome-other-password =%s";
    String sensitive1 = "secret";
    String nonSensitive = "admin";
    String sensitive2 = "super-secret";
    String sensitive3 = "confidential";
    String sensitive4 = "shhhh";
    String input = String.format(
        string, sensitive1, nonSensitive, sensitive2, sensitive3, sensitive4);
    String expected = String.format(
        string, getRedacted(), nonSensitive, getRedacted(), getRedacted(), getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .contains(nonSensitive)
        .doesNotContain(sensitive2)
        .doesNotContain(sensitive3)
        .doesNotContain(sensitive4)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMatchingOptionWithNonMatchingOptionAfterCommand() {
    String string = "connect --password=%s --user=%s";
    String reusedSensitive = "test";
    String input = String.format(string, reusedSensitive, reusedSensitive);
    String expected = String.format(string, getRedacted(), reusedSensitive);

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains(reusedSensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsMultipleMatchingOptionsButNotKeyUsingSameStringAsValue() {
    String string = "connect --%s-password=%s --product-password=%s";
    String reusedSensitive = "test";
    String sensitive = "test1";
    String input = String.format(string, reusedSensitive, reusedSensitive, sensitive);
    String expected = String.format(string, reusedSensitive, getRedacted(), getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains(reusedSensitive)
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactRedactsGemfireSslTruststorePassword() {
    String string = "-Dgemfire.ssl-truststore-password=%s";
    String sensitive = "gibberish";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsGemfireSslKeystorePassword() {
    String string = "-Dgemfire.ssl-keystore-password=%s";
    String sensitive = "gibberish";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueEndingWithHyphen() {
    String string = "-Dgemfire.ssl-keystore-password=%s";
    String sensitive = "supersecret-";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueContainingHyphen() {
    String string = "-Dgemfire.ssl-keystore-password=%s";
    String sensitive = "super-secret";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueContainingManyHyphens() {
    String string = "-Dgemfire.ssl-keystore-password=%s";
    String sensitive = "this-is-super-secret";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsValueStartingWithHyphen() {
    String string = "-Dgemfire.ssl-keystore-password=%s";
    String sensitive = "-supersecret";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactString_redactsQuotedValueStartingWithHyphen() {
    String string = "-Dgemfire.ssl-keystore-password=%s";
    String sensitive = "\"-supersecret\"";
    String input = String.format(string, sensitive);
    String expected = String.format(string, getRedacted());

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive)
        .isEqualTo(expected);
  }

  @Test
  public void redactIterable_redactsMultipleMatchingOptions() {
    String sensitive1 = "secret";
    String sensitive2 = "super-secret";
    String sensitive3 = "confidential";
    String sensitive4 = "shhhh";
    String sensitive5 = "failed";

    Collection<String> input = new ArrayList<>();
    input.add("--gemfire.security-password=" + sensitive1);
    input.add("--login-password=" + sensitive1);
    input.add("--gemfire-password = " + sensitive2);
    input.add("--geode-password= " + sensitive3);
    input.add("--some-other-password =" + sensitive4);
    input.add("--justapassword =" + sensitive5);

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .doesNotContain(sensitive3)
        .doesNotContain(sensitive4)
        .doesNotContain(sensitive5)
        .contains("--gemfire.security-password=" + getRedacted())
        .contains("--login-password=" + getRedacted())
        .contains("--gemfire-password = " + getRedacted())
        .contains("--geode-password= " + getRedacted())
        .contains("--some-other-password =" + getRedacted())
        .contains("--justapassword =" + getRedacted());
  }

  @Test
  public void redactIterable_doesNotRedactMultipleNonMatchingOptions() {
    Collection<String> input = new ArrayList<>();
    input.add("--gemfire.security-properties=./security.properties");
    input.add("--gemfire.sys.security-option=someArg");
    input.add("--gemfire.use-cluster-configuration=true");
    input.add("--someotherstringoption");
    input.add("--login-name=admin");
    input.add("--myArg --myArg2 --myArg3=-arg4");
    input.add("--myArg --myArg2 --myArg3=\"-arg4\"");

    String output = redact(input);

    assertThat(output)
        .as("output of redact(" + input + ")")
        .contains("--gemfire.security-properties=./security.properties")
        .contains("--gemfire.sys.security-option=someArg")
        .contains("--gemfire.use-cluster-configuration=true")
        .contains("--someotherstringoption")
        .contains("--login-name=admin")
        .contains("--myArg --myArg2 --myArg3=-arg4")
        .contains("--myArg --myArg2 --myArg3=\"-arg4\"");
  }

  @Test
  public void redactEachInList_redactsCollectionOfMatchingOptions() {
    String sensitive1 = "secret";
    String sensitive2 = "super-secret";
    String sensitive3 = "confidential";
    String sensitive4 = "shhhh";
    String sensitive5 = "failed";

    Collection<String> input = new ArrayList<>();
    input.add("--gemfire.security-password=" + sensitive1);
    input.add("--login-password=" + sensitive1);
    input.add("--gemfire-password = " + sensitive2);
    input.add("--geode-password= " + sensitive3);
    input.add("--some-other-password =" + sensitive4);
    input.add("--justapassword =" + sensitive5);

    List<String> output = redactEachInList(input);

    assertThat(output)
        .as("output of redactEachInList(" + input + ")")
        .doesNotContain(sensitive1)
        .doesNotContain(sensitive2)
        .doesNotContain(sensitive3)
        .doesNotContain(sensitive4)
        .doesNotContain(sensitive5)
        .contains("--gemfire.security-password=" + getRedacted())
        .contains("--login-password=" + getRedacted())
        .contains("--gemfire-password = " + getRedacted())
        .contains("--geode-password= " + getRedacted())
        .contains("--some-other-password =" + getRedacted())
        .contains("--justapassword =" + getRedacted());
  }
}
