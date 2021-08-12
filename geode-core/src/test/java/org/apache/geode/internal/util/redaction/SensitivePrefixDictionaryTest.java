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

public class SensitivePrefixDictionaryTest {

  private SensitivePrefixDictionary dictionary;

  @Before
  public void setUp() {
    dictionary = new SensitivePrefixDictionary(RedactionDefaults.SENSITIVE_PREFIXES);
  }

  @Test
  public void startsWithSyspropHyphenIsTrue() {
    assertThat(dictionary.isSensitive("sysprop-something")).isTrue();
  }

  @Test
  public void startsWithJavaxNetSslIsTrue() {
    assertThat(dictionary.isSensitive("javax.net.ssl.something")).isTrue();
  }

  @Test
  public void startsWithSecurityHyphenIsTrue() {
    assertThat(dictionary.isSensitive("security-something")).isTrue();
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
  public void passwordLowerCaseIsFalse() {
    assertThat(dictionary.isSensitive("password")).isFalse();
  }

  @Test
  public void passwordUpperCaseIsFalse() {
    assertThat(dictionary.isSensitive("PASSWORD")).isFalse();
  }

  @Test
  public void startsWithPasswordIsFalse() {
    assertThat(dictionary.isSensitive("passwordforsomething")).isFalse();
  }

  @Test
  public void endsWithPasswordIsFalse() {
    assertThat(dictionary.isSensitive("mypassword")).isFalse();
  }

  @Test
  public void containsPasswordIsFalse() {
    assertThat(dictionary.isSensitive("mypasswordforsomething")).isFalse();
  }

  @Test
  public void passwordWithLeadingHyphenIsFalse() {
    assertThat(dictionary.isSensitive("-password")).isFalse();
  }

  @Test
  public void passwordWithTrailingHyphenIsFalse() {
    assertThat(dictionary.isSensitive("password-")).isFalse();
  }

  @Test
  public void passwordWithMiddleHyphenIsFalse() {
    assertThat(dictionary.isSensitive("pass-word")).isFalse();
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
