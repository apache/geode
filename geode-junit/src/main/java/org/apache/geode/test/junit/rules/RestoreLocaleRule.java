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

import java.util.Locale;
import java.util.function.Consumer;

import org.junit.rules.ExternalResource;

/**
 * The {@code RestoreLocale} rule undoes changes of locale when the test finishes (whether it passes
 * or fails).
 * <p>
 * Let's assume the locale {@code US}. Now run the test
 *
 * <pre>
 *   public void YourTest {
 *     &#064;Rule
 *     public final TestRule restoreLocale = new RestoreLocale();
 *
 *     &#064;Test
 *     public void overrideLocale() {
 *       Locale.setDefaule(Locale.JAPAN);
 *       assertThat(Locale.getDefault(), is(Locale.JAPAN));
 *     }
 *   }
 * </pre>
 *
 * After running the test, the locale will set to {@code US} again.
 */
public class RestoreLocaleRule extends ExternalResource {
  private Locale originalLocale;

  private final Locale initLocale;
  private final Consumer<Locale> consumer;

  /**
   * Creates a {@code RestoreLocale} rule that restores locale. The initial locale at the time of
   * the test is set to the specified locale. Specify {@code consumer} if you need to modify the
   * Locale associated with it. {@code consumer} will be run using the original {@code Locale} after
   * the test.
   *
   * @param initLocale Initial locale at test run
   * @param consumer Changing Locale associated with
   */
  public RestoreLocaleRule(Locale initLocale, Consumer<Locale> consumer) {
    this.initLocale = initLocale;
    this.consumer = consumer;
  }

  /**
   * Creates a {@code RestoreLocale} rule that restores locale. The initial locale at the time of
   * the test is set to the specified locale.
   *
   * @param initLocale Initial locale at test run
   */
  public RestoreLocaleRule(Locale initLocale) {
    this(initLocale, null);
  }

  /**
   * Creates a {@code RestoreLocale} rule that restores locale. Specify {@code consumer} if you need
   * to modify the Locale associated with it. {@code consumer} will be run using the original
   * {@code Locale} after the test.
   *
   * @param consumer Changing Locale associated with
   */
  public RestoreLocaleRule(Consumer<Locale> consumer) {
    this(Locale.getDefault(), consumer);
  }

  /**
   * Creates a {@code RestoreLocale} rule that restores locale.
   */
  public RestoreLocaleRule() {
    this(Locale.getDefault(), null);
  }

  @Override
  protected void before() throws Throwable {
    originalLocale = Locale.getDefault();

    Locale.setDefault(initLocale);
    if (consumer != null) {
      consumer.accept(initLocale);
    }
  }

  @Override
  protected void after() {
    Locale.setDefault(originalLocale);
    if (consumer != null) {
      consumer.accept(originalLocale);
    }
  }
}
