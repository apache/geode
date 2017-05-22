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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

@Category(UnitTest.class)
public class ExportedLogsSizeInfoTest {

  @Test
  public void testExportedLogsSizeInfoConstructor() {
    ExportedLogsSizeInfo sizeDetail = new ExportedLogsSizeInfo(1L, 11L, 111L);
    assertThat(sizeDetail).isNotNull();
    assertThat(sizeDetail.getLogsSize()).isEqualTo(1L);
    assertThat(sizeDetail.getDiskAvailable()).isEqualTo(11L);
    assertThat(sizeDetail.getDiskSize()).isEqualTo(111L);
  }

  @Test
  public void testExportedLogsSizeInfoZeroArgConstructor() {
    ExportedLogsSizeInfo sizeDetail = new ExportedLogsSizeInfo();
    assertThat(sizeDetail).isNotNull();
    assertThat(sizeDetail.getLogsSize()).isEqualTo(0L);
    assertThat(sizeDetail.getDiskAvailable()).isEqualTo(0L);
    assertThat(sizeDetail.getDiskSize()).isEqualTo(0L);
  }

  @Test
  public void equals_returnsTrueForTwoInstancesWithTheSameFieldValues() throws Exception {
    ExportedLogsSizeInfo sizeDetail1 = new ExportedLogsSizeInfo(2L, 22L, 222L);
    ExportedLogsSizeInfo sizeDetail2 = new ExportedLogsSizeInfo(2L, 22L, 222L);
    assertThat(sizeDetail1.equals(sizeDetail1)).isTrue();
    assertThat(sizeDetail1.equals(sizeDetail2)).isTrue();
    assertThat(sizeDetail2.equals(sizeDetail1)).isTrue();
  }

  @Test
  public void equals_returnsFalseWhenLogsSizeDiffers() throws Exception {
    ExportedLogsSizeInfo sizeDetail1 = new ExportedLogsSizeInfo(3L, 33L, 333L);
    ExportedLogsSizeInfo sizeDetail2 = new ExportedLogsSizeInfo(33L, 33L, 333L);
    assertThat(sizeDetail1.equals(sizeDetail2)).isFalse();
    assertThat(sizeDetail2.equals(sizeDetail1)).isFalse();
  }

  @Test
  public void equals_returnsFalseWhenAvailableDiskDiffers() throws Exception {
    ExportedLogsSizeInfo sizeDetail1 = new ExportedLogsSizeInfo(4L, 44L, 444L);
    ExportedLogsSizeInfo sizeDetail2 = new ExportedLogsSizeInfo(4L, 4L, 444L);
    assertThat(sizeDetail1.equals(sizeDetail2)).isFalse();
    assertThat(sizeDetail2.equals(sizeDetail1)).isFalse();
  }

  @Test
  public void equals_returnsFalseWheneDiskSizeDiffers() throws Exception {
    ExportedLogsSizeInfo sizeDetail1 = new ExportedLogsSizeInfo(5L, 55L, 555L);
    ExportedLogsSizeInfo sizeDetail2 = new ExportedLogsSizeInfo(5L, 55L, 55L);
    assertThat(sizeDetail1.equals(sizeDetail2)).isFalse();
    assertThat(sizeDetail2.equals(sizeDetail1)).isFalse();
  }

  @Test
  public void equals_returnsFalseForComparisonWithNullObject() throws Exception {
    ExportedLogsSizeInfo sizeDetail1 = new ExportedLogsSizeInfo(6L, 66L, 666L);
    ExportedLogsSizeInfo sizeDetail2 = null;
    assertThat(sizeDetail1.equals(sizeDetail2)).isFalse();
  }

  @Test
  public void testClassInequality() {
    ExportedLogsSizeInfo sizeDeatai1 = new ExportedLogsSizeInfo(7L, 77L, 777L);
    String sizeDetail2 = sizeDeatai1.toString();
    assertThat(sizeDeatai1.equals(sizeDetail2)).isFalse();
    assertThat(sizeDetail2.equals(sizeDeatai1)).isFalse();
  }

  @Test
  public void testHashCode() throws Exception {
    ExportedLogsSizeInfo sizeDetail1 = new ExportedLogsSizeInfo();
    ExportedLogsSizeInfo sizeDetail2 = new ExportedLogsSizeInfo(8L, 88L, 888L);
    ExportedLogsSizeInfo sizeDetail3 = new ExportedLogsSizeInfo(88L, 8L, 888L);

    assertThat(sizeDetail1.hashCode()).isNotEqualTo(sizeDetail2.hashCode());
    assertThat(sizeDetail1.hashCode()).isNotEqualTo(sizeDetail3.hashCode());
    assertThat(sizeDetail2.hashCode()).isNotEqualTo(sizeDetail3.hashCode());

    assertThat(sizeDetail1.hashCode()).isEqualTo(29791);
    assertThat(sizeDetail2.hashCode()).isEqualTo(41095);
    assertThat(sizeDetail3.hashCode()).isEqualTo(115495);
  }

  @Test
  public void deserialization_setsFieldsToOriginalUnserializedValues() throws Exception {
    ExportedLogsSizeInfo sizeDetail = new ExportedLogsSizeInfo(9L, 99L, 999L);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    sizeDetail.toData(out);
    ExportedLogsSizeInfo sizeDetailIn = new ExportedLogsSizeInfo();
    sizeDetailIn.fromData(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

    assertThat(sizeDetailIn).isEqualTo(sizeDetail);
  }

  @Test
  public void testToString() throws Exception {
    ExportedLogsSizeInfo sizeDetail = new ExportedLogsSizeInfo(10L, 100L, 1000L);
    assertThat(sizeDetail.toString())
        .isEqualTo("[logsSize: 10, diskAvailable: 100, diskSize: 1000]");

  }

}
