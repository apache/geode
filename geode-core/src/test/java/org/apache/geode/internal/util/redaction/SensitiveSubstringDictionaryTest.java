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
package org.apache.geode.internal.util.redaction;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class SensitiveSubstringDictionaryTest {

  private SensitiveSubstringDictionary dictionary;

  @Before
  public void setUp() {
    dictionary = new SensitiveSubstringDictionary(RedactionDefaults.SENSITIVE_SUBSTRINGS);
  }

  @Test
  public void passwordLowerCaseIsTrue() {
    assertThat(dictionary.isSensitive("password")).isTrue();
  }

  @Test
  public void passwordUpperCaseIsTrue() {
    assertThat(dictionary.isSensitive("PASSWORD")).isTrue();
  }

  @Test
  public void startsWithPasswordIsTrue() {
    assertThat(dictionary.isSensitive("passwordforsomething")).isTrue();
  }

  @Test
  public void endsWithPasswordIsTrue() {
    assertThat(dictionary.isSensitive("mypassword")).isTrue();
  }

  @Test
  public void containsPasswordIsTrue() {
    assertThat(dictionary.isSensitive("mypasswordforsomething")).isTrue();
  }

  @Test
  public void passwordWithLeadingHyphenIsTrue() {
    assertThat(dictionary.isSensitive("-password")).isTrue();
  }

  @Test
  public void passwordWithTrailingHyphenIsTrue() {
    assertThat(dictionary.isSensitive("password-")).isTrue();
  }

  @Test
  public void nullStringIsFalse() {
    assertThat(dictionary.isSensitive(null)).isFalse();
  }

  @Test
  public void emptyStringIsFalse() {
    assertThat(dictionary.isSensitive("")).isFalse();
  }

  @Test
  public void passwordWithMiddleHyphenIsFalse() {
    assertThat(dictionary.isSensitive("pass-word")).isFalse();
  }

  @Test
  public void startsWithSyspropHyphenIsFalse() {
    assertThat(dictionary.isSensitive("sysprop-something")).isFalse();
  }

  @Test
  public void startsWithSyspropWithoutHyphenIsFalse() {
    assertThat(dictionary.isSensitive("syspropsomething")).isFalse();
  }

  @Test
  public void containsSyspropWithHyphenIsFalse() {
    assertThat(dictionary.isSensitive("my-sysprop-something")).isFalse();
  }

  @Test
  public void endsWithSyspropWithHyphenIsFalse() {
    assertThat(dictionary.isSensitive("my-sysprop-")).isFalse();
  }

  @Test
  public void syspropIsFalse() {
    assertThat(dictionary.isSensitive("sysprop")).isFalse();
  }

  @Test
  public void startsWithJavaxNetSslIsFalse() {
    assertThat(dictionary.isSensitive("javax.net.ssl.something")).isFalse();
  }

  @Test
  public void startsWithJavaxSslIsFalse() {
    assertThat(dictionary.isSensitive("javax.ssl.something")).isFalse();
  }

  @Test
  public void startsWithJavaxNetIsFalse() {
    assertThat(dictionary.isSensitive("javax.net.something")).isFalse();
  }

  @Test
  public void startsWithJavaxWithoutNetWithSslIsFalse() {
    assertThat(dictionary.isSensitive("javax.ssl.something")).isFalse();
  }

  @Test
  public void containsJavaxNetSslIsFalse() {
    assertThat(dictionary.isSensitive("my.javax.net.ssl.something")).isFalse();
  }

  @Test
  public void endsWithJavaxNetSslIsFalse() {
    assertThat(dictionary.isSensitive("my.javax.net.ssl")).isFalse();
  }

  @Test
  public void startsWithSecurityHyphenIsFalse() {
    assertThat(dictionary.isSensitive("security-something")).isFalse();
  }

  @Test
  public void startsWithSecurityWithoutHyphenIsFalse() {
    assertThat(dictionary.isSensitive("securitysomething")).isFalse();
  }

  @Test
  public void containsSecurityWithHyphenIsFalse() {
    assertThat(dictionary.isSensitive("my-security-something")).isFalse();
  }

  @Test
  public void endsWithSecurityWithHyphenIsFalse() {
    assertThat(dictionary.isSensitive("my-security-")).isFalse();
  }

  @Test
  public void securityIsFalse() {
    assertThat(dictionary.isSensitive("security")).isFalse();
  }
}
