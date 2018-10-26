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
package org.apache.geode.internal.logging.assertj.impl;

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.internal.CommonValidations.checkIsNotNull;
import static org.assertj.core.internal.CommonValidations.failIfEmptySinceActualIsNotEmpty;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertionInfo;
import org.assertj.core.internal.Files;
import org.assertj.core.internal.Objects;

public abstract class AbstractLogFileAssert<SELF extends AbstractLogFileAssert<SELF>>
    extends AbstractAssert<SELF, File> {

  private Files files = Files.instance();
  private Charset charset = Charset.defaultCharset();

  public AbstractLogFileAssert(File actual, Class<?> selfType) {
    super(actual, selfType);
  }

  public SELF exists() {
    files.assertExists(info, actual);
    return myself;
  }

  public SELF contains(String... value) {
    assertContains(info, actual, charset, value);
    return myself;
  }

  public SELF doesNotContain(String... values) {
    assertDoesNotContain(info, actual, charset, values);
    return myself;
  }

  public SELF containsLine(String value) {
    return containsLines(value);
  }

  public SELF containsLines(String... values) {
    assertContainsLines(info, actual, charset, values);
    return myself;
  }

  public SELF doesNotContainLines(String... values) {
    assertDoesNotContainLines(info, actual, charset, values);
    return myself;
  }

  private void assertContains(AssertionInfo info, File actual, Charset charset, String[] values) {
    if (commonCheckThatLogFileAssertionSucceeds(info, actual, values)) {
      return;
    }
    files.assertIsFile(info, actual);
    try {
      List<String> actualLines = FileUtils.readLines(actual, charset);
      assertThat(StringUtils.join(actualLines, LINE_SEPARATOR)).contains(values);
    } catch (IOException e) {
      String msg = String.format("Unable to verify text contents of file:<%s>", actual);
      throw new UncheckedIOException(msg, e);
    }
  }

  private void assertDoesNotContain(AssertionInfo info, File actual, Charset charset,
      String[] values) {
    if (commonCheckThatLogFileAssertionSucceeds(info, actual, values)) {
      return;
    }
    files.assertIsFile(info, actual);
    try {
      List<String> actualLines = FileUtils.readLines(actual, charset);
      assertThat(StringUtils.join(actualLines, LINE_SEPARATOR)).doesNotContain(values);
    } catch (IOException e) {
      String msg = String.format("Unable to verify text contents of file:<%s>", actual);
      throw new UncheckedIOException(msg, e);
    }
  }

  private void assertContainsLines(AssertionInfo info, File actual, Charset charset,
      String[] values) {
    if (commonCheckThatLogFileAssertionSucceeds(info, actual, values)) {
      return;
    }
    files.assertIsFile(info, actual);
    try {
      List<String> actualLines = FileUtils.readLines(actual, charset);
      assertThat(actualLines).contains(values);
    } catch (IOException e) {
      String msg = String.format("Unable to verify text contents of file:<%s>", actual);
      throw new UncheckedIOException(msg, e);
    }
  }

  private void assertDoesNotContainLines(AssertionInfo info, File actual, Charset charset,
      String[] values) {
    if (commonCheckThatLogFileAssertionSucceeds(info, actual, values)) {
      return;
    }
    files.assertIsFile(info, actual);
    try {
      List<String> actualLines = FileUtils.readLines(actual, charset);
      assertThat(actualLines).doesNotContain(values);
    } catch (IOException e) {
      String msg = String.format("Unable to verify text contents of file:<%s>", actual);
      throw new UncheckedIOException(msg, e);
    }
  }

  private boolean commonCheckThatLogFileAssertionSucceeds(AssertionInfo info, File actual,
      Object[] sequence) {
    checkIsNotNull(sequence);
    assertNotNull(info, actual);
    files.assertIsFile(info, actual);
    files.assertExists(info, actual);
    // if both actual and values are empty, then assertion passes.
    if (FileUtils.sizeOf(actual) == 0 && sequence.length == 0) {
      return true;
    }
    failIfEmptySinceActualIsNotEmpty(sequence);
    return false;
  }

  private void assertNotNull(AssertionInfo info, File actual) {
    Objects.instance().assertNotNull(info, actual);
  }
}
