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
package org.apache.geode.alerting.internal;

import static org.apache.geode.alerting.spi.AlertLevel.ERROR;
import static org.apache.geode.alerting.spi.AlertLevel.NONE;
import static org.apache.geode.alerting.spi.AlertLevel.SEVERE;
import static org.apache.geode.alerting.spi.AlertLevel.WARNING;
import static org.apache.geode.alerting.spi.AlertLevel.find;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.alerting.spi.AlertLevel;
import org.apache.geode.logging.spi.LogWriterLevel;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link AlertLevel}.
 */
@Category(AlertingTest.class)
public class AlertLevelTest {

  @Test
  public void isSerializable() {
    assertThat(NONE).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() {
    AlertLevel logLevel = (AlertLevel) SerializationUtils.clone(NONE);

    assertThat(logLevel).isEqualTo(NONE).isSameAs(NONE);
  }

  @Test
  public void findWARNING() {
    assertThat(find(LogWriterLevel.WARNING.intLevel())).isEqualTo(WARNING);
  }

  @Test
  public void findERROR() {
    assertThat(find(LogWriterLevel.ERROR.intLevel())).isEqualTo(ERROR);
  }

  @Test
  public void findSEVERE() {
    assertThat(find(LogWriterLevel.SEVERE.intLevel())).isEqualTo(SEVERE);
  }

  @Test
  public void findNONE() {
    assertThat(find(LogWriterLevel.NONE.intLevel())).isEqualTo(NONE);
  }

  @Test
  public void findINFO() {
    assertThatThrownBy(() -> find(LogWriterLevel.INFO.intLevel()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No AlertLevel found for intLevel " + LogWriterLevel.INFO.intLevel());
  }

  @Test
  public void findCONFIG() {
    assertThatThrownBy(() -> find(LogWriterLevel.CONFIG.intLevel()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No AlertLevel found for intLevel " + LogWriterLevel.CONFIG.intLevel());
  }

  @Test
  public void findFINE() {
    assertThatThrownBy(() -> find(LogWriterLevel.FINE.intLevel()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No AlertLevel found for intLevel " + LogWriterLevel.FINE.intLevel());
  }

  @Test
  public void findFINER() {
    assertThatThrownBy(() -> find(LogWriterLevel.FINER.intLevel()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No AlertLevel found for intLevel " + LogWriterLevel.FINER.intLevel());
  }

  @Test
  public void findFINEST() {
    assertThatThrownBy(() -> find(LogWriterLevel.FINEST.intLevel()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No AlertLevel found for intLevel " + LogWriterLevel.FINEST.intLevel());
  }

  @Test
  public void findALL() {
    assertThatThrownBy(() -> find(LogWriterLevel.ALL.intLevel()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No AlertLevel found for intLevel " + LogWriterLevel.ALL.intLevel());
  }
}
