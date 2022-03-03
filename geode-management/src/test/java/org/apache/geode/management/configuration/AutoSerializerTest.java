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

package org.apache.geode.management.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class AutoSerializerTest {
  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Test
  public void constructorWithNoPatterns() {
    AutoSerializer autoSerializer = new AutoSerializer(true);
    assertThat(autoSerializer.getPatterns()).isNull();
  }

  @Test
  public void constructorWithNullPatterns() {
    AutoSerializer autoSerializer = new AutoSerializer(true, (String[]) null);
    assertThat(autoSerializer.getPatterns()).isNull();
  }

  @Test
  public void serialization() throws JsonProcessingException {
    AutoSerializer originalAutoSerializer = new AutoSerializer(true, "pat1", "pat2");
    String json = mapper.writeValueAsString(originalAutoSerializer);
    AutoSerializer deserializedAutoSerializer = mapper.readValue(json, AutoSerializer.class);

    assertThat(deserializedAutoSerializer.isPortable()).as("portable").isTrue();
    assertThat(deserializedAutoSerializer.getPatterns()).as("patterns").containsExactly("pat1",
        "pat2");
  }

  @Test
  public void serializationOfNull() throws JsonProcessingException {
    AutoSerializer originalAutoSerializer = new AutoSerializer(null, null);
    String json = mapper.writeValueAsString(originalAutoSerializer);
    AutoSerializer deserializedAutoSerializer = mapper.readValue(json, AutoSerializer.class);

    assertThat(deserializedAutoSerializer.isPortable()).as("portable").isNull();
    assertThat(deserializedAutoSerializer.getPatterns()).as("patterns").isNull();
  }
}
