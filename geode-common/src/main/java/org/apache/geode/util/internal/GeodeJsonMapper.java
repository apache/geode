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

package org.apache.geode.util.internal;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * helper class for creating various json mappers used by Geode Project
 */
public class GeodeJsonMapper {

  /**
   * @return a jackson json mapper that allows single quotes and is able to deserialize json
   *         string without @JsonTypeInfo if base class is a concrete implementation.
   */
  public static ObjectMapper getMapper() {
    ObjectMapper mapper = JsonMapper.builder()
        .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
        .enable(MapperFeature.USE_BASE_TYPE_AS_DEFAULT_IMPL)
        .build();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    return mapper;
  }

  public static ObjectMapper getMapperIgnoringUnknownProperties() {
    ObjectMapper mapper = getMapper();
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }
}
