/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.greplogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class LogConsumerTest {

  @Rule
  public TestName testName = new TestName();

  private LogConsumer logConsumer;

  @Before
  public void setUp() {
    boolean allowSkipLogMessages = false;
    List<Pattern> expectedStrings = Collections.emptyList();
    String logFileName = getClass().getSimpleName() + "_" + testName.getMethodName();
    int repeatLimit = 2;

    logConsumer = new LogConsumer(allowSkipLogMessages, expectedStrings, logFileName, repeatLimit);
  }

  @Test
  public void consumeReturnsNullIfLineIsOk() {
    StringBuilder value = logConsumer.consume("ok");

    assertThat(value).isNull();
  }

  @Test
  public void consumeReturnsNullIfLineIsEmpty() {
    StringBuilder value = logConsumer.consume("");

    assertThat(value).isNull();
  }

  @Test
  public void consumeThrowsNullPointerExceptionIfLineIsNull() {
    Throwable thrown = catchThrowable(() -> logConsumer.consume(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void closeReturnsNullIfLineIsOk() {
    logConsumer.consume("ok");

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsNullIfLineIsEmpty() {
    logConsumer.consume("");

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsNullIfLineContainsInfoLogStatementWithException() {
    logConsumer.consume("[info 019/06/13 14:41:05.750 PDT <main> tid=0x1] " +
        NullPointerException.class.getName());

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsLineIfLineContainsErrorLogStatement() {
    String line = "[error 019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsNullIfLineContainsWarningLogStatement() {
    logConsumer.consume("[warning 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message");

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsLineIfLineContainsFatalLogStatement() {
    String line = "[fatal 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsLineIfLineContainsSevereLogStatement() {
    String line = "[severe 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsLineIfLineContainsMalformedLog4jStatement() {
    String line = "[info 2019/06/13 14:41:05.750 PDT <main> tid=0x1] contains {}";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsNullIfLineContainsHydraMasterLocatorsWildcard() {
    String line = "hydra.MasterDescription.master.locators={}";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }
}
