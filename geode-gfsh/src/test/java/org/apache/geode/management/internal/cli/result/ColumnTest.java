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

public class ColumnTest {

  @Test
  public void getLengthReturnsLengthOfStringValue() {
    String value = "bar";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    int result = column.getLength();

    assertThat(result).isEqualTo(value.length());
  }

  @Test
  public void toStringContainsAlignAndStringValue() {
    String value = "foo";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.toString();

    assertThat(result).isEqualTo("Column{align=" + align + ", stringValue='" + value + "'}");
  }

  @Test
  public void buildColumn_alignRight_colWidthEqualsStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.RIGHT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length(), false);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void buildColumn_alignRight_colWidthEqualsStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.RIGHT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length(), true);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void buildColumn_alignRight_colWidthLessThanStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.RIGHT;
    Column column = new Column(value, align);

    String result = column.buildColumn(6, false);

    assertThat(result).isEqualTo(value.substring(0, 4) + "..");
  }

  @Test
  public void buildColumn_alignRight_colWidthLessThanStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.RIGHT;
    Column column = new Column(value, align);

    String result = column.buildColumn(6, true);

    assertThat(result).isEqualTo(value.substring(0, 4) + "..");
  }

  @Test
  public void buildColumn_alignRight_colWidthGreaterThanStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.RIGHT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length() + 2, false);

    assertThat(result).isEqualTo("  " + value);
  }

  @Test
  public void buildColumn_alignRight_colWidthGreaterThanStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.RIGHT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length() + 2, true);

    assertThat(result).isEqualTo(value);
  }

  // -----------------------------

  @Test
  public void buildColumn_alignLeft_colWidthEqualsStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length(), false);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void buildColumn_alignLeft_colWidthEqualsStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length(), true);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void buildColumn_alignLeft_colWidthLessThanStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.buildColumn(6, false);

    assertThat(result).isEqualTo(value.substring(0, 4) + "..");
  }

  @Test
  public void buildColumn_alignLeft_colWidthLessThanStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.buildColumn(6, true);

    assertThat(result).isEqualTo(value.substring(0, 4) + "..");
  }

  @Test
  public void buildColumn_alignLeft_colWidthGreaterThanStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length() + 2, false);

    assertThat(result).isEqualTo(value + "  ");
  }

  @Test
  public void buildColumn_alignLeft_colWidthGreaterThanStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.LEFT;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length() + 2, true);

    assertThat(result).isEqualTo(value);
  }

  // -----------------------------

  @Test
  public void buildColumn_alignCenter_colWidthEqualsStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length(), false);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void buildColumn_alignCenter_colWidthEqualsStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length(), true);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void buildColumn_alignCenter_colWidthLessThanStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    String result = column.buildColumn(6, false);

    assertThat(result).isEqualTo(value.substring(0, 4) + "..");
  }

  @Test
  public void buildColumn_alignCenter_colWidthLessThanStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    String result = column.buildColumn(6, true);

    assertThat(result).isEqualTo(value.substring(0, 4) + "..");
  }

  @Test
  public void buildColumn_alignCenter_colWidthGreaterThanStringValueLength_trimIsFalse() {
    String value = "thisisastring";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length() + 2, false);

    assertThat(result).isEqualTo(" " + value + " ");
  }

  @Test
  public void buildColumn_alignCenter_colWidthGreaterThanStringValueLength_trimIsTrue() {
    String value = "thisisastring";
    Align align = Align.CENTER;
    Column column = new Column(value, align);

    String result = column.buildColumn(value.length() + 2, true);

    assertThat(result).isEqualTo(value);
  }
}
