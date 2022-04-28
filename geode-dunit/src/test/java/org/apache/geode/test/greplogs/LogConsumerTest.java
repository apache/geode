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

  private static final String EXCEPTION_MESSAGE =
      "java.lang.ClassNotFoundException: does.not.Exist";

  private LogConsumer logConsumer;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    boolean allowSkipLogMessages = false;
    List<Pattern> expectedStrings = Collections.emptyList();
    String logFileName = getClass().getSimpleName() + "_" + testName.getMethodName();
    int repeatLimit = 2;

    logConsumer = new LogConsumer(allowSkipLogMessages, expectedStrings, logFileName, repeatLimit);
  }

  @Test
  public void consume_returnsNull_ifLineIsOk() {
    String value = logConsumer.consume("ok");

    assertThat(value).isNull();
  }

  @Test
  public void consume_returnsNull_ifLineIsEmpty() {
    String value = logConsumer.consume("");

    assertThat(value).isNull();
  }

  @Test
  public void consume_throwsNullPointerException_ifLineIsNull() {
    Throwable thrown = catchThrowable(() -> logConsumer.consume(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void close_returnsNull_ifLineIsOk() {
    logConsumer.consume("ok");

    String value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsNull_ifLineIsEmpty() {
    logConsumer.consume("");

    String value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsNull_ifLineContains_infoLevelMessage_withException() {
    logConsumer.consume("[info 019/06/13 14:41:05.750 PDT <main> tid=0x1] " +
        NullPointerException.class.getName());

    String value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsLine_ifLineContains_errorLevelMessage() {
    String line = "[error 019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    String value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsNull_ifLineContains_warningLevelMessage() {
    logConsumer.consume("[warning 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message");

    String value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsLine_ifLineContains_fatalLevelMessage() {
    String line = "[fatal 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    String value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsLine_ifLineContains_severeLevelMessage() {
    String line = "[severe 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    String value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsLine_ifLineContains_malformedLog4jStatement() {
    String line = "[info 2019/06/13 14:41:05.750 PDT <main> tid=0x1] contains {}";
    logConsumer.consume(line);

    String value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void close_returnsNull_ifLineContains_hydraMasterLocatorsWildcard() {
    String line = "hydra.MasterDescription.master.locators={}";
    logConsumer.consume(line);

    String value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsNull_ifLineContains_ManagementRequest() {
    String managmentRequest =
        "[info 2022/04/16 09:39:11.008 UTC locator-0 <qtp773068785-114> tid=0x72] Management Request: PUT[url=/management/v1/deployments]; user=null; payload=---YMBX204KTK7fmoVc8vVmUZOfJOmATtYGRLlAK\n"
            + "Content-Disposition: form-data; name=\"file\"; filename=\"lib.jar\"\n"
            + "Content-Type: application/java-archive\n"
            + "Content-Length: 426\n"
            + "\n"
            + "PK?????????L?T????????????????Class1.class????;?o?>?????^.?f?.v?nv??F?6?????;F?f\n"
            + "?0F?????TF?~???T????????????Wp~iQr?[&?????X\\l???X??????\n"
            + "4?\"?? ????I?K??O?JM.aPd`?????@?T\n"
            + "$??<Y0???Uk;??F?4;?d????8?4??'?PK??k??6????????PK?????????L?T????????????????timestamp3435040?447????PK??{6}?????\n"
            + "???PK???????????L?Tk??6??????????????????????????Class1.class????PK???????????L?T{6}?????\n"
            + "?????????????????????timestampPK??????????u?????????\n"
            + "---YMBX204KTK7fmoVc8vVmUZOfJOmATtYGRLlAK\n"
            + "Content-Disposition: form-data; name=\"config\"\n"
            + "Content-Type: application/json\n"
            + "\n"
            + "{\"class\":\"org.apache.geode.management.configuration.Deployment\",\"group\":\"group1\",\"deployedTime\":null,\"deployedBy\":null,\"fileName\":\"lib.jar\"}\n"
            + "---YMBX204KTK7fmoVc8vVmUZOfJOmATtYGRLlAK--";
    logConsumer.consume(managmentRequest);
    String value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void close_returnsLine_ifLineContains_ManagementRequest() {
    String managmentRequest =
        "[error 2022/04/16 09:39:11.008 UTC locator-0 <qtp773068785-114> tid=0x72] Management Request: PUT[url=/management/v1/deployments]; user=null; payload=---YMBX204KTK7fmoVc8vVmUZOfJOmATtYGRLlAK\n"
            + "Content-Disposition: form-data; name=\"file\"; filename=\"lib.jar\"\n"
            + "Content-Type: application/java-archive\n"
            + "Content-Length: 426\n"
            + "\n"
            + "PK?????????L?T????????????????Class1.class????;?o?>?????^.?f?.v?nv??F?6?????;F?f\n"
            + "?0F?????TF?~???T????????????Wp~iQr?[&?????X\\l???X??????\n"
            + "4?\"?? ????I?K??O?JM.aPd`?????@?T\n"
            + "$??<Y0???Uk;??F?4;?d????8?4??'?PK??k??6????????PK?????????L?T????????????????timestamp3435040?447????PK??{6}?????\n"
            + "???PK???????????L?Tk??6??????????????????????????Class1.class????PK???????????L?T{6}?????\n"
            + "?????????????????????timestampPK??????????u?????????\n"
            + "---YMBX204KTK7fmoVc8vVmUZOfJOmATtYGRLlAK\n"
            + "Content-Disposition: form-data; name=\"config\"\n"
            + "Content-Type: application/json\n"
            + "\n"
            + "{\"class\":\"org.apache.geode.management.configuration.Deployment\",\"group\":\"group1\",\"deployedTime\":null,\"deployedBy\":null,\"fileName\":\"lib.jar\"}\n"
            + "---YMBX204KTK7fmoVc8vVmUZOfJOmATtYGRLlAK--";
    logConsumer.consume(managmentRequest);
    String value = logConsumer.close();

    assertThat(value).contains(managmentRequest);
  }

  @Test
  public void close_returnsLine_ifLineContainsException() {
    logConsumer.consume(EXCEPTION_MESSAGE);

    String value = logConsumer.close();

    assertThat(value).contains(EXCEPTION_MESSAGE);
  }
}
