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

package org.apache.geode.management.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class PdxTest {

  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Test
  public void getUri() {
    Pdx config = new Pdx();
    assertThat(config.getLinks().getList())
        .isEqualTo("/configurations/pdx");
    assertThat(config.getLinks().getSelf())
        .isEqualTo("/configurations/pdx");
  }

  @Test
  public void defaultAutoSerializerIsNull() {
    assertThat(new Pdx().getAutoSerializer()).isNull();
  }

  @Test
  public void remembersAutoSerializer() {
    Pdx pdx = new Pdx();
    AutoSerializer autoSerializer = new AutoSerializer(true, "pat");

    pdx.setAutoSerializer(autoSerializer);

    assertThat(pdx.getAutoSerializer()).isSameAs(autoSerializer);
  }

  @Test
  public void setAutoSerializerThrowsGivenNonNullPdxSerializer() {
    Pdx pdx = new Pdx();
    AutoSerializer autoSerializer = new AutoSerializer(true, "pat");
    pdx.setPdxSerializer(new ClassName("name"));

    assertThatThrownBy(() -> pdx.setAutoSerializer(autoSerializer))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The autoSerializer can not be set if a pdxSerializer is already set.");
  }

  @Test
  public void setPdxSerializerThrowsGivenNonNullAutoSerializer() {
    Pdx pdx = new Pdx();
    ClassName pdxSerializer = new ClassName("name");
    pdx.setAutoSerializer(new AutoSerializer(true, "pat"));

    assertThatThrownBy(() -> pdx.setPdxSerializer(pdxSerializer))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The pdxSerializer can not be set if an autoSerializer is already set.");
  }

  @Test
  public void serializationOfNonDefaultsWithAutoSerializer() throws Exception {
    Pdx originalPdx = new Pdx();
    originalPdx.setDiskStoreName("diskStore");
    originalPdx.setIgnoreUnreadFields(true);
    originalPdx.setReadSerialized(true);
    originalPdx.setAutoSerializer(new AutoSerializer(true, "pat1"));

    String json = mapper.writeValueAsString(originalPdx);
    Pdx deserializedPdx = mapper.readValue(json, Pdx.class);

    assertThat(deserializedPdx.getDiskStoreName()).as("diskStoreName").isEqualTo("diskStore");
    assertThat(deserializedPdx.isIgnoreUnreadFields()).as("IgnoreUnreadFields").isTrue();
    assertThat(deserializedPdx.isReadSerialized()).as("ReadSerialized").isTrue();
    assertThat(deserializedPdx.getAutoSerializer().isPortable()).as("AutoSerializer portable")
        .isTrue();
    assertThat(deserializedPdx.getAutoSerializer().getPatterns()).as("AutoSerializer patterns")
        .containsExactly("pat1");
    assertThat(deserializedPdx.getPdxSerializer()).as("PdxSerializer").isNull();
  }

  @Test
  public void serializationOfNonDefaultsWithPdxSerializer() throws Exception {
    Pdx originalPdx = new Pdx();
    originalPdx.setDiskStoreName("diskStore");
    originalPdx.setIgnoreUnreadFields(true);
    originalPdx.setReadSerialized(true);
    originalPdx.setPdxSerializer(new ClassName("name"));

    String json = mapper.writeValueAsString(originalPdx);
    Pdx deserializedPdx = mapper.readValue(json, Pdx.class);

    assertThat(deserializedPdx.getDiskStoreName()).as("diskStoreName").isEqualTo("diskStore");
    assertThat(deserializedPdx.isIgnoreUnreadFields()).as("IgnoreUnreadFields").isTrue();
    assertThat(deserializedPdx.isReadSerialized()).as("ReadSerialized").isTrue();
    assertThat(deserializedPdx.getAutoSerializer()).as("AutoSerializer").isNull();
    assertThat(deserializedPdx.getPdxSerializer().getClassName()).as("PdxSerializer className")
        .isEqualTo("name");
    assertThat(deserializedPdx.getPdxSerializer().getInitProperties())
        .as("PdxSerializer initProperties").isEmpty();
  }

  @Test
  public void serializationOfDefaults() throws Exception {
    Pdx originalPdx = new Pdx();

    String json = mapper.writeValueAsString(originalPdx);
    Pdx deserializedPdx = mapper.readValue(json, Pdx.class);

    assertThat(deserializedPdx.getDiskStoreName()).as("diskStoreName").isNull();
    assertThat(deserializedPdx.isIgnoreUnreadFields()).as("IgnoreUnreadFields").isNull();
    assertThat(deserializedPdx.isReadSerialized()).as("ReadSerialized").isNull();
    assertThat(deserializedPdx.getAutoSerializer()).as("AutoSerializer").isNull();
    assertThat(deserializedPdx.getPdxSerializer()).as("PdxSerializer").isNull();
  }
}
