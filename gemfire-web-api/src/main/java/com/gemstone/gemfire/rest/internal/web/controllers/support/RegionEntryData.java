/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.controllers.support;

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
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.rest.internal.web.util.JSONUtils;
import com.gemstone.gemfire.rest.internal.web.util.JsonWriter;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The RegionData class is a container for data fetched from a GemFire Cache Region.
 * <p/>
 * @author NIlkanth Patel, John Blum
 * @see com.fasterxml.jackson.databind.JsonSerializable
 * @see java.lang.Iterable
 * @since 8.0
 */

@SuppressWarnings("unused")
@XmlRootElement(name = "region")
@XmlType(name = "org.gopivotal.app.web.controllers.support.RegionData")
public class RegionEntryData<T> extends  RegionData<T> {

  public RegionEntryData() {
    super();
  }

  public RegionEntryData(final String regionNamePath) {
    super(regionNamePath);
  }
  
  @Override
  public void serialize(final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
    throws IOException
  {
    /*
    if(this!=null && this.size() > 1) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeArrayFieldStart(getRegionNamePath());
    }
    */
    for (T element : this) {
      JsonWriter.writeValueAsJson(jsonGenerator, element, null);
    }
    
    /*
    if(this!=null && this.size() > 1) {
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    } 
    */  
  }

  @Override
  public void serializeWithType(final JsonGenerator jsonGenerator,
                               final SerializerProvider serializerProvider,
                               final TypeSerializer typeSerializer)
   throws IOException
  {
    // NOTE serializing "type" meta-data is not necessary in this case; just call serialize.
    serialize(jsonGenerator, serializerProvider);
  }
  
}

