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

package org.apache.geode.management.internal.configuration.converters;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.management.configuration.Pdx;

public class PdxConverterTest {
  private PdxConverter converter = new PdxConverter();

  @Test
  public void fromConfig() {
    Pdx pdx = new Pdx();
    pdx.setReadSerialized(true);
    pdx.setPersistent(true);
    PdxType pdxType = converter.fromConfigObject(pdx);
    assertThat(pdxType.isPersistent()).isTrue();
    assertThat(pdxType.isReadSerialized()).isTrue();
  }

  @Test
  public void fromXmlObject() {
    PdxType pdxType = new PdxType();
    pdxType.setDiskStoreName("test");
    pdxType.setIgnoreUnreadFields(false);
    Pdx pdxConfig = converter.fromXmlObject(pdxType);
    assertThat(pdxConfig.getDiskStoreName()).isEqualTo("test");
    assertThat(pdxConfig.isIgnoreUnreadFields()).isEqualTo(false);
    assertThat(pdxConfig.isPersistent()).isNull();
  }
}
