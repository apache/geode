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
package org.apache.geode.test.assertj.internal;

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.internal.CommonValidations.checkIsNotNull;
import static org.assertj.core.internal.CommonValidations.failIfEmptySinceActualIsNotEmpty;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertionInfo;
import org.assertj.core.internal.Files;
import org.assertj.core.internal.Objects;

public abstract class AbstractLogFileAssert<SELF extends AbstractLogFileAssert<SELF>>
    extends AbstractAssert<SELF, File> {

  private final Files files = Files.instance();
  private final Charset charset = Charset.defaultCharset();

  public AbstractLogFileAssert(File actual, Class<?> selfType) {
    super(actual, selfType);
  }

  public SELF exists() {
    files.assertExists(info, actual);
    return myself;
  }

  public SELF doesNotExist() {
    files.assertDoesNotExist(info, actual);
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

  public SELF containsOnlyOnce(String... value) {
    assertContainsOnlyOnce(info, actual, charset, value);
    return myself;
  }

  private void assertContains(AssertionInfo info, File actual, Charset charset, String[] values) {
    if (commonCheckThatLogFileAssertionSucceeds(info, actual, values)) {
      return;
    }
    files.assertIsFile(info, actual);
    try {
      List<String> actualLines = FileUtils.readLines(actual, charset);
      List<String> expectedLines = nonBlankStrings(Arrays.asList(values));

      List<String> notFound = new ArrayList<>();
      for (String expectedLine : expectedLines) {
        if (!actualLinesContain(actualLines, expectedLine)) {
          notFound.add(expectedLine);
        }
      }

      if (!notFound.isEmpty()) {
        fail("Expecting:" + lineSeparator() + " " + printLines(actualLines) + lineSeparator() +
            "to contain:" + lineSeparator() + " " + printLines(expectedLines) + lineSeparator() +
            "but could not find:" + lineSeparator() + " " + printLines(notFound));
      }
    } catch (IOException e) {
      String msg = String.format("Unable to verify text contents of file:<%s>", actual);
      throw new UncheckedIOException(msg, e);
    }
  }

  private void assertContainsOnlyOnce(AssertionInfo info, File actual, Charset charset,
      String[] values) {
    if (commonCheckThatLogFileAssertionSucceeds(info, actual, values)) {
      return;
    }
    files.assertIsFile(info, actual);
    try {
      List<String> actualLines = FileUtils.readLines(actual, charset);
      List<String> expectedLines = nonBlankStrings(Arrays.asList(values));

      List<String> notFound = new ArrayList<>();
      List<String> moreThanOnce = new ArrayList<>();
      for (String expectedLine : expectedLines) {
        if (actualLinesContain(actualLines, expectedLine)) {
          if (Collections.frequency(actualLines, expectedLine) > 1) {
            moreThanOnce.add(expectedLine);
          }
        } else {
          notFound.add(expectedLine);
        }
      }

      if (!notFound.isEmpty()) {
        fail("Expecting:" + lineSeparator() + " " + printLines(actualLines) + lineSeparator() +
            "to contain:" + lineSeparator() + " " + printLines(expectedLines) + lineSeparator() +
            "but could not find:" + lineSeparator() + " " + printLines(notFound));
      }

      if (!moreThanOnce.isEmpty()) {
        fail("Expecting:" + lineSeparator() + " " + printLines(actualLines) + lineSeparator() +
            "to contain only once:" + lineSeparator() + " " + printLines(expectedLines) +
            lineSeparator() + "but found more than once:" + lineSeparator() + " " +
            printLines(moreThanOnce));
      }

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
      List<String> unexpectedLines = nonBlankStrings(Arrays.asList(values));

      List<String> found = new ArrayList<>();
      for (String actualLine : actualLines) {
        for (String unexpectedLine : unexpectedLines) {
          if (actualLine.contains(unexpectedLine)) {
            found.add(actualLine);
          }
        }
      }

      if (!found.isEmpty()) {
        fail("Expecting:" + lineSeparator() + " " + printLines(actualLines) + lineSeparator() +
            "to not contain:" + lineSeparator() + " " + printLines(unexpectedLines)
            + lineSeparator()
            + "but found:" + lineSeparator() + " " + printLines(found));
      }
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

  private boolean actualLinesContain(List<String> actualLines, String value) {
    for (String actualLine : actualLines) {
      if (actualLine.contains(value)) {
        return true;
      }
    }
    return false;
  }

  private String printLines(List<String> lines) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String line : lines) {
      stringBuilder.append(line).append(lineSeparator());
    }
    return stringBuilder.toString();
  }

  private static List<String> nonBlankStrings(List<String> values) {
    return values.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
  }
}
