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

package org.apache.geode.rest.internal.web.controllers.support;

import java.io.IOException;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import org.apache.geode.rest.internal.web.util.JsonWriter;

/**
 * The RegionData class is a container for data fetched from a GemFire Cache Region.
 * <p/>
 *
 * @see com.fasterxml.jackson.databind.JsonSerializable
 * @see java.lang.Iterable
 * @since GemFire 8.0
 */

@SuppressWarnings("unused")
@XmlRootElement(name = "region")
@XmlType(name = "org.gopivotal.app.web.controllers.support.RegionData")
public class RegionEntryData<T> extends RegionData<T> {

  public RegionEntryData() {
    super();
  }

  public RegionEntryData(final String regionNamePath) {
    super(regionNamePath);
  }

  @Override
  public void serialize(final JsonGenerator jsonGenerator,
      final SerializerProvider serializerProvider) throws IOException {
    /*
     * if(this!=null && this.size() > 1) { jsonGenerator.writeStartObject();
     * jsonGenerator.writeArrayFieldStart(getRegionNamePath()); }
     */
    for (T element : this) {
      JsonWriter.writeValueAsJson(jsonGenerator, element, null);
    }

    /*
     * if(this!=null && this.size() > 1) { jsonGenerator.writeEndArray();
     * jsonGenerator.writeEndObject(); }
     */
  }

  @Override
  public void serializeWithType(final JsonGenerator jsonGenerator,
      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
      throws IOException {
    // NOTE serializing "type" meta-data is not necessary in this case; just call serialize.
    serialize(jsonGenerator, serializerProvider);
  }

}
