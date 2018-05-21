package org.apache.geode.management.internal.cli.result.model;

import static org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState.ERROR;
import static org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState.IGNORABLE;
import static org.apache.geode.management.internal.cli.functions.CliFunctionResult.StatusState.OK;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
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
    table = result.addTable("table1", results, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, OK]");

    table = result.addTable("table2", results, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, OK]");
  }

  @Test
  public void setContentAllError() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", ERROR, "failed"));
    results.add(new CliFunctionResult("member2", ERROR, "failed"));

    table = result.addTable("table1", results, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");

    table = result.addTable("table2", results, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }

  @Test
  public void setContentAllIgnorable() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", IGNORABLE, "can be ignored"));
    results.add(new CliFunctionResult("member2", IGNORABLE, "can be ignored"));

    table = result.addTable("table1", results, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[IGNORED, IGNORED]");

    table = result.addTable("table2", results, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }

  @Test
  public void setContentOKAndError() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", ERROR, "failed"));

    table = result.addTable("table1", results, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");

    table = result.addTable("table2", results, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");
  }

  @Test
  public void setContentOKAndIgnore() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", OK, "success"));
    results.add(new CliFunctionResult("member2", IGNORABLE, "can be ignored"));
    table = result.addTable("table1", results, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, IGNORED]");

    table = result.addTable("table2", results, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[OK, ERROR]");
  }

  @Test
  public void setContentErrorAndIgnore() {
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(new CliFunctionResult("member1", ERROR, "failed"));
    results.add(new CliFunctionResult("member2", IGNORABLE, "can be ignored"));

    table = result.addTable("table1", results, true);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, IGNORED]");

    table = result.addTable("table2", results, false);
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(table.getContent().get("Status").toString()).isEqualTo("[ERROR, ERROR]");
  }
}
