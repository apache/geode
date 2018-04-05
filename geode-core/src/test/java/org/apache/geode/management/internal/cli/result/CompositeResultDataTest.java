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

package org.apache.geode.management.internal.cli.result;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, GfshTest.class})
public class CompositeResultDataTest {

  @Test
  public void emptySection() {
    CompositeResultData result = new CompositeResultData();
    result.addSection();

    GfJsonObject json = result.getGfJsonObject();
    assertThat(json.getJSONObject("content").getString("__sections__-0")).isEqualTo("{}");
  }

  @Test
  public void justAHeader() {
    CompositeResultData result = new CompositeResultData();
    String header = "this space left blank";
    result.setHeader(header);

    GfJsonObject json = result.getGfJsonObject();
    assertThat(json.getString("header")).isEqualTo(header);
    assertThat(json.getString("content")).isEqualTo("{}");
  }

  @Test
  public void justAFooter() {
    CompositeResultData result = new CompositeResultData();
    String footer = "this space left blank";
    result.setFooter(footer);

    GfJsonObject json = result.getGfJsonObject();
    assertThat(json.getString("footer")).isEqualTo(footer);
    assertThat(json.getString("content")).isEqualTo("{}");
  }

  @Test
  public void withSection_andHeader_andFooter() {
    CompositeResultData result = new CompositeResultData();
    String header = "this header left blank";
    String footer = "this footer left blank";

    result.addSection();
    result.setHeader(header);
    result.setFooter(footer);

    GfJsonObject json = result.getGfJsonObject();
    assertThat(json.getJSONObject("content").getString("__sections__-0")).isEqualTo("{}");
    assertThat(json.getString("footer")).isEqualTo(footer);
    assertThat(json.getString("header")).isEqualTo(header);
  }

  @Test
  public void withNestedSectionContainingASingleTable() {
    CompositeResultData result = new CompositeResultData();
    String outerHeader = "this outerHeader left blank";
    String outerFooter = "this outerFooter left blank";

    result.setHeader(outerHeader);
    result.setFooter(outerFooter);
    CompositeResultData.SectionResultData section = result.addSection();

    String tableSurround = "this surround left blank";
    section.addTable();
    section.setHeader(tableSurround);
    section.setFooter(tableSurround);

    GfJsonObject json = result.getGfJsonObject();
    assertThat(json.getJSONObject("content").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getString("content")).isEqualTo("{}");
    assertThat(json.getString("footer")).isEqualTo(outerFooter);
    assertThat(json.getString("header")).isEqualTo(outerHeader);
    assertThat(json.getJSONObject("content").getJSONObject("__sections__-0").getString("header"))
        .isEqualTo(tableSurround);
    assertThat(json.getJSONObject("content").getJSONObject("__sections__-0").getString("footer"))
        .isEqualTo(tableSurround);

    CompositeResultData.SectionResultData resultSection = result.retrieveSectionByIndex(0);
    assertThat(resultSection.getSectionGfJsonObject().getString("header")).isEqualTo(tableSurround);
    assertThat(resultSection.getSectionGfJsonObject().getString("footer")).isEqualTo(tableSurround);

    assertThat(resultSection.retrieveTable("0").getGfJsonObject().getString("content"))
        .isEqualTo("{}");
  }

  @Test
  public void withTableContainingData() {
    CompositeResultData result = new CompositeResultData();
    CompositeResultData.SectionResultData section = result.addSection();
    TabularResultData table = section.addTable();
    table.accumulate("column1", 1);
    table.accumulate("column1", "abc");
    table.accumulate("column1", 2);

    assertThat(result.getGfJsonObject().getJSONObject("content").getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content").getString("column1"))
            .isEqualTo("[1,\"abc\",2]");
  }

  @Test
  public void withSectionContainingRawData() {
    CompositeResultData result = new CompositeResultData();
    CompositeResultData.SectionResultData section = result.addSection();
    section.addData("my-data", "some random string");

    assertThat(result.getGfJsonObject().getJSONObject("content").getJSONObject("__sections__-0")
        .getString("my-data")).isEqualTo("some random string");
  }
}
