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

package org.apache.geode.management.internal.cli.result.model;

import static org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState.ERROR;
import static org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState.IGNORED;
import static org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class TabularResultModelTest {
  private TabularResultModel table1;

  @Before
  public void setUp() throws Exception {
    table1 = new TabularResultModel();
  }

  @Test
  public void accumulateAndAddRowHasTheSameEffect() {
    table1.accumulate("c1", "v1");
    table1.accumulate("c1", "v4");
    table1.accumulate("c2", "v2");
    table1.accumulate("c2", "v5");
    table1.accumulate("c3", "v3");
    table1.accumulate("c3", "v6");

    TabularResultModel table2 = new TabularResultModel();
    table2.setColumnHeader("c1", "c2", "c3");
    table2.addRow("v1", "v2", "v3");
    table2.addRow("v4", "v5", "v6");

    assertThat(table1.getContent()).isEqualToComparingFieldByFieldRecursively(table2.getContent());
  }

  @Test
  public void cannotAddRowWithDifferentSize() {
    table1.setColumnHeader("c1", "c2", "c3");
    assertThatThrownBy(() -> table1.addRow("v1", "v2")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void setContentAllOK() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", OK, "success"));
    assertThat(table1.setContent(results, true)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[OK, OK]");

    assertThat(table1.setContent(results, false)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[OK, OK]");
  }

  @Test
  public void setContentAllError() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", ERROR, "failed"));
    results.add(new CliFunctionResult("member2", ERROR, "failed"));
    assertThat(table1.setContent(results, true)).isEqualTo(false);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");

    assertThat(table1.setContent(results, false)).isEqualTo(false);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }

  @Test
  public void setContentOKAndError() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", ERROR, "failed"));
    assertThat(table1.setContent(results, true)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");

    assertThat(table1.setContent(results, false)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");
  }

  @Test
  public void setContentOKAndIgnore() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", IGNORED, "can be ignored"));
    assertThat(table1.setContent(results, true)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[OK, IGNORED]");

    assertThat(table1.setContent(results, false)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");
  }

  @Test
  public void setContentErrorAndIgnore() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", ERROR, "failed"));
    results.add(new CliFunctionResult("member2", IGNORED, "can be ignored"));
    assertThat(table1.setContent(results, true)).isEqualTo(true);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[ERROR, IGNORED]");

    assertThat(table1.setContent(results, false)).isEqualTo(false);
    assertThat(table1.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }
}
