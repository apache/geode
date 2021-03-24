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
package org.apache.geode.management.internal.cli.result.model;

import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.ERROR;
import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.IGNORABLE;
import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.OK;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ResultModelTest {

  private ResultModel result;
  private TabularResultModel table;

  @Before
  public void setUp() throws Exception {
    result = new ResultModel();
  }

  @Test
  public void setContentAllOK() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", OK, "success"));
    table = result.addTableAndSetStatus("table1", results, true, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, OK]");

    table = result.addTableAndSetStatus("table2", results, false, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, OK]");
  }

  @Test
  public void setContentAllError() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", ERROR, "failed"));
    results.add(new CliFunctionResult("member2", ERROR, "failed"));

    table = result.addTableAndSetStatus("table1", results, true, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");

    table = result.addTableAndSetStatus("table2", results, false, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }

  @Test
  public void setContentAllIgnorable() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", IGNORABLE, "can be ignored"));
    results.add(new CliFunctionResult("member2", IGNORABLE, "can be ignored"));

    table = result.addTableAndSetStatus("table1", results, true, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[IGNORED, IGNORED]");

    table = result.addTableAndSetStatus("table2", results, false, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }

  @Test
  public void setContentOKAndError() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", ERROR, "failed"));

    table = result.addTableAndSetStatus("table1", results, true, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");

    table = result.addTableAndSetStatus("table2", results, false, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");

    table = result.addTableAndSetStatus("table3", results, true, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");

    table = result.addTableAndSetStatus("table4", results, false, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");
  }

  @Test
  public void setContentOKAndIgnore() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", IGNORABLE, "can be ignored"));
    table = result.addTableAndSetStatus("table1", results, true, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, IGNORED]");

    table = result.addTableAndSetStatus("table2", results, false, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");

    table = result.addTableAndSetStatus("table3", results, true, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, IGNORED]");

    table = result.addTableAndSetStatus("table4", results, false, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");
  }

  @Test
  public void setContentErrorAndIgnore() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", ERROR, "failed"));
    results.add(new CliFunctionResult("member2", IGNORABLE, "can be ignored"));

    table = result.addTableAndSetStatus("table1", results, true, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, IGNORED]");

    table = result.addTableAndSetStatus("table2", results, false, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");

    table = result.addTableAndSetStatus("table3", results, true, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, IGNORED]");

    table = result.addTableAndSetStatus("table4", results, false, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }

  @Test
  public void getSectionName() {
    result.addInfo("Section1");
    result.addInfo("Section2");
    result.addTable("Section3");
    result.addData("Section4");

    List<String> sectionNames = result.getSectionNames();
    assertThat(sectionNames).containsExactly("Section1", "Section2", "Section3", "Section4");
  }

  @Test
  public void serializeInfoResult() throws Exception {
    InfoResultModel info = result.addInfo();
    info.addLine("line2");
    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    String json = mapper.writeValueAsString(result);
    System.out.println(json);
    ResultModel resultModel = mapper.readValue(json, ResultModel.class);
    assertThat(resultModel.getSectionNames()).containsExactly("info");
  }
}
