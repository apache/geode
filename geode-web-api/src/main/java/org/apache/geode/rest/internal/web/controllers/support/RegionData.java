/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.rest.internal.web.controllers.support;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.rest.internal.web.util.JSONUtils;
import org.apache.geode.rest.internal.web.util.JsonWriter;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The RegionData class is a container for data fetched from a GemFire Cache Region.
 * <p/>
 * @see com.fasterxml.jackson.databind.JsonSerializable
 * @see java.lang.Iterable
 * @since GemFire 8.0
 */

@SuppressWarnings("unused")
@XmlRootElement(name = "region")
@XmlType(name = "org.gopivotal.app.web.controllers.support.RegionData")
public class RegionData<T> implements Iterable<T>, JsonSerializable {

  private final List<T> data = new ArrayList<T>();

  private String regionNamePath;

  public RegionData() {
  }

  public RegionData(final String regionNamePath) {
    setRegionNamePath(regionNamePath);
  }

  public String getRegionNamePath() {
    Assert.state(StringUtils.hasText(this.regionNamePath), "The Region name/path was not properly initialized!");
    return regionNamePath;
  }

 public final void setRegionNamePath(final String regionNamePath) {
   Assert.hasText(regionNamePath, "The name or path of the Region must be specified!");
   this.regionNamePath = regionNamePath;
 }

  public RegionData<T> add(final T data) {
    //We are adding null data into the response
    //Assert.notNull(data, String.format("The data to add to Region (%1$s) cannot be null!", getRegionNamePath()));
    //if(data != null) {
      this.data.add(data);
    //}
    return this;
  }

  public RegionData<T> add(final T... data) {
    for (final T element : data) {
      if (element != null) {
        add(element);
      }
    }

    return this;
  }

  public RegionData<T> add(final Iterable<T> data) {
    for (final T element : data) {
      //Adding null data into the response
      /*
      if (element != null) {
        add(element);
      }
      */
      add(element);
    }

    return this;
  }

  public T get(final int index) {
    return list().get(index);
  }

  public boolean isEmpty() {
    return this.data.isEmpty();
  }

  public Iterator<T> iterator() {
    return list().iterator();
  }

  public List<T> list() {
    return Collections.unmodifiableList(this.data);
  }

  public int size() {
    return this.data.size();
  }

  protected String convertToJson(final PdxInstance pdxObj) {
    return (pdxObj != null ? JSONFormatter.toJSON(pdxObj) : null);
  }
  
  public void serialize(final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
    throws IOException
  {
 
    //if(this!=null && this.size() > 1) {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeArrayFieldStart(getRegionNamePath());
    //}
    
    for (T element : this) {
      JsonWriter.writeValueAsJson(jsonGenerator, element, null);
    }
    
    //if(this!=null && this.size() > 1) {
    jsonGenerator.writeEndArray();
    jsonGenerator.writeEndObject();
    //}   
  }

  public void serializeWithType(final JsonGenerator jsonGenerator,
                               final SerializerProvider serializerProvider,
                               final TypeSerializer typeSerializer)
   throws IOException
  {
    // NOTE serializing "type" meta-data is not necessary in this case; just call serialize.
    serialize(jsonGenerator, serializerProvider);
  }
  
}
