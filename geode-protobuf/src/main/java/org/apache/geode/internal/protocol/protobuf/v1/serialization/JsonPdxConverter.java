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
package org.apache.geode.internal.protocol.protobuf.v1.serialization;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.JSONFormatterException;
import org.apache.geode.pdx.PdxInstance;

@Experimental
public class JsonPdxConverter implements TypeConverter<String, PdxInstance> {
  @Override
  public PdxInstance decode(String incoming) throws DecodingException {
    try {
      return JSONFormatter.fromJSON(incoming);
    } catch (JSONFormatterException ex) {
      throw new DecodingException("Could not decode JSON-encoded object ", ex);
    }
  }

  @Override
  public String encode(PdxInstance incoming) throws EncodingException {
    try {
      return JSONFormatter.toJSON(incoming);
    } catch (JSONFormatterException ex) {
      throw new EncodingException("Could not encode PDX object as JSON", ex);
    }
  }

  @Override
  public SerializationType getSerializationType() {
    return SerializationType.JSON;
  }
}
