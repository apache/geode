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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.FORMAT;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.ONLY_DATE_FORMAT;
import static org.apache.geode.management.internal.i18n.CliStrings.EXPORT_LOGS__ENDTIME;
import static org.apache.geode.management.internal.i18n.CliStrings.EXPORT_LOGS__STARTTIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsInterceptorTest {

  private ExportLogsInterceptor interceptor;
  private GfshParseResult parseResult;
  private ResultModel result;

  @Before
  public void before() {
    interceptor = new ExportLogsInterceptor();
    parseResult = Mockito.mock(GfshParseResult.class);
    when(parseResult.getParamValueAsString("log-level")).thenReturn("info");
    when(parseResult.getParamValue("logs-only")).thenReturn(true);
    when(parseResult.getParamValue("stats-only")).thenReturn(false);
  }

  @Test
  public void testGroupAndMember() {
    when(parseResult.getParamValueAsString("group")).thenReturn("group");
    when(parseResult.getParamValueAsString("member")).thenReturn("group");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent())
        .containsOnly("Can't specify both group and member.");
  }

  @Test
  public void testStartEndFormat() {
    // Correct date only format
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn("2000/01/01");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME)).thenReturn("2000/01/02");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly("");

    // Correct date and time format
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME))
        .thenReturn("2000/01/01/00/00/00/001/PST");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME))
        .thenReturn("2000/01/02/00/00/00/001/GMT");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly("");

    // Incorrect date only format
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn("2000/123/01");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME)).thenReturn(null);
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent())
        .containsOnly("start-time had incorrect format. Valid formats are " + ONLY_DATE_FORMAT
            + " and " + FORMAT);

    // Incorrect date and time format
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn(null);
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME))
        .thenReturn("2000/01/02/00/00/00/001/0");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly(
        "end-time had incorrect format. Valid formats are " + ONLY_DATE_FORMAT + " and " + FORMAT);

    // Empty string
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn("");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME)).thenReturn("");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent())
        .containsOnly("start-time and end-time had incorrect format. Valid formats are "
            + ONLY_DATE_FORMAT + " and " + FORMAT);
  }

  @Test
  public void testStartEndParsing() {
    // Parsable date only input
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn("2000/01/01");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME)).thenReturn("2000/01/02");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly("");

    // Parsable date and time input
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME))
        .thenReturn("2000/01/01/00/00/00/001/PST");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME))
        .thenReturn("2000/01/02/00/00/00/001/GMT+01:00");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly("");

    // Non-parsable input
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn(null);
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME))
        .thenReturn("2000/01/02/00/00/00/001/NOT_A_TIMEZONE");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent())
        .containsOnly("end-time could not be parsed to valid date/time.");
  }

  @Test
  public void testStartEnd() {
    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn("2000/01/01");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME)).thenReturn("2000/01/02");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly("");

    when(parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME)).thenReturn("2000/01/02");
    when(parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME)).thenReturn("2000/01/01");
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent())
        .containsOnly("start-time has to be earlier than end-time.");
  }

  @Test
  public void testIncludeStats() {
    when(parseResult.getParamValue("logs-only")).thenReturn(true);
    when(parseResult.getParamValue("stats-only")).thenReturn(false);
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent()).containsOnly("");

    when(parseResult.getParamValue("logs-only")).thenReturn(true);
    when(parseResult.getParamValue("stats-only")).thenReturn(true);
    result = interceptor.preExecution(parseResult);
    assertThat(result.getInfoSection("info").getContent())
        .containsOnly("logs-only and stats-only can't both be true");
  }
}
