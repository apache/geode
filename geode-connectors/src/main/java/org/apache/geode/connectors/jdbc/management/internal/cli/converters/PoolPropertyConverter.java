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
package org.apache.geode.connectors.jdbc.management.internal.cli.converters;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.geode.management.internal.cli.domain.PoolProperty;
import org.apache.geode.util.internal.GeodeJsonMapper;

/***
 * Converter for CreateDataSourceCommand's --pool-properties option.
 * Spring Shell 3.x automatically handles String to custom type conversion
 * through standard Spring converters registered as beans.
 */
public class PoolPropertyConverter {

  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  public static PoolProperty convert(String value) {
    try {
      return mapper.readValue(value, PoolProperty.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid json: \"" + value + "\" details: " + e);
    }
  }
}
