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
package org.apache.geode.test.junit.rules;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;

import org.apache.geode.test.junit.runners.TestRunner;

/**
 * Unit tests for {@link RestoreLocaleRule}.
 */
public class RestoreLocaleRuleTest {

  private static Locale notDefaultLocale;

  @BeforeClass
  public static void setUp() throws Exception {
    Locale[] locales = Locale.getAvailableLocales();
    while (notDefaultLocale == null) {
      Locale l = locales[new Random().nextInt(locales.length)];
      if (l != Locale.getDefault()) {
        notDefaultLocale = l;
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    notDefaultLocale = null;
  }

  @Test
  public void shouldRestoreLocaleInAfter() throws Throwable {
    Locale originalLocale = Locale.getDefault();
    Result result = TestRunner.runTest(ShouldRestoreLocaleInAfter.class);

    assertTrue(result.wasSuccessful());
    assertThat(Locale.getDefault(), is(originalLocale));
    assertThat(ShouldRestoreLocaleInAfter.localeInTest, is(notDefaultLocale));
  }

  @Test
  public void setInitLocaleShouldRestoreLocaleInAfter() throws Throwable {
    Locale originalLocale = Locale.getDefault();
    Result result = TestRunner.runTest(SetInitLocaleShouldRestoreLocaleInAfter.class);

    assertTrue(result.wasSuccessful());
    assertThat(Locale.getDefault(), is(originalLocale));
    assertThat(SetInitLocaleShouldRestoreLocaleInAfter.localeInTest, is(Locale.ENGLISH));
  }

  @Test
  public void shouldRestoreLocaleInAfterWithConsumer() throws Throwable {
    Locale originalLocale = Locale.getDefault();
    Result result = TestRunner.runTest(ShouldRestoreLocaleInAfterWithConsumer.class);

    assertTrue(result.wasSuccessful());
    assertThat(Locale.getDefault(), is(originalLocale));
    assertThat(ShouldRestoreLocaleInAfterWithConsumer.localeInTest, is(notDefaultLocale));
    assertThat(ShouldRestoreLocaleInAfterWithConsumer.locales.size(), is(2));
    assertThat(ShouldRestoreLocaleInAfterWithConsumer.locales,
        contains(originalLocale, originalLocale));
  }

  @Test
  public void setInitLocaleShouldRestoreLocaleInAfterWithConsumer() throws Throwable {
    Locale originalLocale = Locale.getDefault();
    Result result = TestRunner.runTest(SetInitLocaleShouldRestoreLocaleInAfterWithConsumer.class);

    assertTrue(result.wasSuccessful());
    assertThat(Locale.getDefault(), is(originalLocale));
    assertThat(SetInitLocaleShouldRestoreLocaleInAfterWithConsumer.localeInTest,
        is(Locale.CHINESE));
    assertThat(SetInitLocaleShouldRestoreLocaleInAfterWithConsumer.locales.size(), is(2));
    assertThat(SetInitLocaleShouldRestoreLocaleInAfterWithConsumer.locales,
        contains(Locale.CHINESE, originalLocale));
  }

  /**
   * Used by test {@link #shouldRestoreLocaleInAfter()}
   */
  public static class ShouldRestoreLocaleInAfter {

    static Locale localeInTest = notDefaultLocale;

    @Rule
    public final RestoreLocaleRule restoreLocale = new RestoreLocaleRule();

    @Test
    public void doTest() throws Exception {
      Locale.setDefault(localeInTest);
      assertThat(Locale.getDefault(), is(localeInTest));
    }
  }

  /**
   * Used by test {@link #setInitLocaleShouldRestoreLocaleInAfter()}
   */
  public static class SetInitLocaleShouldRestoreLocaleInAfter {

    static Locale localeInTest;

    @Rule
    public final RestoreLocaleRule restoreLocale = new RestoreLocaleRule(Locale.ENGLISH);

    @Test
    public void doTest() throws Exception {
      localeInTest = Locale.getDefault();
      assertThat(Locale.getDefault(), is(Locale.ENGLISH));
    }
  }

  /**
   * Used by test {@link #shouldRestoreLocaleInAfterWithConsumer()}
   */
  public static class ShouldRestoreLocaleInAfterWithConsumer {

    static List<Locale> locales = new ArrayList<>();
    static Locale localeInTest = notDefaultLocale;

    @Rule
    public final RestoreLocaleRule restoreLocale = new RestoreLocaleRule(l -> locales.add(l));

    @Test
    public void doTest() throws Exception {
      Locale originalLocale = Locale.getDefault();
      Locale.setDefault(localeInTest);
      assertThat(Locale.getDefault(), is(localeInTest));
      assertThat(locales.size(), is(1));
      assertThat(locales, contains(originalLocale));
    }
  }

  /**
   * Used by test {@link #setInitLocaleShouldRestoreLocaleInAfterWithConsumer()}
   */
  public static class SetInitLocaleShouldRestoreLocaleInAfterWithConsumer {

    static List<Locale> locales = new ArrayList<>();
    static Locale localeInTest;

    @Rule
    public final RestoreLocaleRule restoreLocale =
        new RestoreLocaleRule(Locale.CHINESE, l -> locales.add(l));

    @Test
    public void doTest() throws Exception {
      localeInTest = Locale.getDefault();
      assertThat(Locale.getDefault(), is(Locale.CHINESE));
      assertThat(locales.size(), is(1));
      assertThat(locales, contains(Locale.CHINESE));
    }
  }
}
