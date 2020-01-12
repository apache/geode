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
package org.apache.geode.internal.logging;

import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ERROR;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINER;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.NONE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.SEVERE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.find;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.spi.LogWriterLevel;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LogWriterLevel}.
 */
@Category(LoggingTest.class)
public class LogWriterLevelTest {

  @Test
  public void isSerializable() {
    assertThat(ALL).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() {
    LogWriterLevel logLevel = SerializationUtils.clone(ALL);

    assertThat(logLevel).isEqualTo(ALL).isSameAs(ALL);
  }

  @Test
  public void findNONE() {
    assertThat(find(NONE.intLevel())).isEqualTo(NONE);
  }

  @Test
  public void findSEVERE() {
    assertThat(find(SEVERE.intLevel())).isEqualTo(SEVERE);
  }

  @Test
  public void findERROR() {
    assertThat(find(ERROR.intLevel())).isEqualTo(ERROR);
  }

  @Test
  public void findWARNING() {
    assertThat(find(WARNING.intLevel())).isEqualTo(WARNING);
  }

  @Test
  public void findINFO() {
    assertThat(find(INFO.intLevel())).isEqualTo(INFO);
  }

  @Test
  public void findCONFIG() {
    assertThat(find(CONFIG.intLevel())).isEqualTo(CONFIG);
  }

  @Test
  public void findFINE() {
    assertThat(find(FINE.intLevel())).isEqualTo(FINE);
  }

  @Test
  public void findFINER() {
    assertThat(find(FINER.intLevel())).isEqualTo(FINER);
  }

  @Test
  public void findFINEST() {
    assertThat(find(FINEST.intLevel())).isEqualTo(FINEST);
  }

  @Test
  public void findALL() {
    assertThat(find(ALL.intLevel())).isEqualTo(ALL);
  }
}
