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
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;


public class JMXDataUpdaterGetDoubleAttributeTest {

  private Cluster cluster;
  private JMXDataUpdater jmxDataUpdater;
  private Float floatStat;
  private Double doubleStat;

  @Before
  public void setUp() {
    this.cluster = mock(Cluster.class);
    this.jmxDataUpdater = new JMXDataUpdater("server", "cluster", this.cluster, null, null);
    this.floatStat = 1.2345f;
    this.doubleStat = 1.2345d;
  }

  @Test
  public void shouldNotReturnZeroForFloat() {
    double value = jmxDataUpdater.getDoubleAttribute(floatStat, "someStatistic");
    assertThat(value).isNotEqualTo(0);
  }

  @Test
  public void shouldNotReturnZeroForPrimitiveFloat() {
    double value = jmxDataUpdater.getDoubleAttribute(floatStat.floatValue(), "someStatistic");
    assertThat(value).isNotEqualTo(0);
  }

  @Test
  public void returnsDoubleCloseToFloat() {
    double value = jmxDataUpdater.getDoubleAttribute(floatStat, "someStatistic");
    assertThat(value).isEqualTo(floatStat, Offset.offset(0.001d));
  }

  @Test
  public void returnsDoubleCloseToPrimitiveFloat() {
    double value = jmxDataUpdater.getDoubleAttribute(floatStat.floatValue(), "someStatistic");
    assertThat(value).isEqualTo(floatStat, Offset.offset(0.001d));
  }

  @Test
  public void returnsDoubleCloseToNegativeFloat() {
    this.floatStat = -floatStat;
    double value = jmxDataUpdater.getDoubleAttribute(floatStat, "someStatistic");
    assertThat(value).isEqualTo(floatStat, Offset.offset(0.001d));
  }

  @Test
  public void returnsDoubleForDouble() {
    double value = jmxDataUpdater.getDoubleAttribute(doubleStat, "someStatistic");
    assertThat(value).isEqualTo(floatStat, Offset.offset(0.001d));
  }

  @Test
  public void returnsDoubleForDoublePrimitive() {
    double value = jmxDataUpdater.getDoubleAttribute(doubleStat.doubleValue(), "someStatistic");
    assertThat(value).isEqualTo(floatStat, Offset.offset(0.001d));
  }

  @Test
  public void returnsZeroForNull() {
    double value = jmxDataUpdater.getDoubleAttribute(null, "someStatistic");
    assertThat(value).isEqualTo(0d, Offset.offset(0.001d));
  }

  @Test
  public void returnsZeroForString() {
    double value = jmxDataUpdater.getDoubleAttribute("abc", "someStatistic");
    assertThat(value).isEqualTo(0d, Offset.offset(0.001d));
  }

}
