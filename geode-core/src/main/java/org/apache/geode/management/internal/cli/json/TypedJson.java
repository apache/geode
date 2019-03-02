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
package org.apache.geode.management.internal.cli.json;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.pdx.PdxInstance;

/**
 * A JSON serializer that has special handling for collections to limit the number of elements
 * written to the document. It also has special handling for PdxInstance and query Structs.
 */
public class TypedJson {

  ObjectMapper mapper;
  SimpleModule mapperModule;

  Map<Object, List<Object>> forbidden = new java.util.IdentityHashMap<Object, List<Object>>();

  boolean addCommaBeforeNextElement;

  private Map<String, List<Object>> map;

  private int maxCollectionElements;


  {
    mapper = new ObjectMapper();
    mapperModule = new SimpleModule();
    mapperModule.addSerializer(Collection.class, new TypedJsonCollectionSerializer());
    mapperModule.addSerializer(Map.class, new TypedJsonMapSerializer());
    mapperModule.addSerializer(PdxInstance.class, new TypedJsonPdxInstanceSerializer());
    mapperModule.addSerializer(StructImpl.class, new TypedJsonStructSerializer());
    mapperModule.addSerializer(Region.Entry.class, new TypedJsonRegionEntrySerializer());
    // todo: arrays need to be handled & queryCollectionsDepth applied
    mapper.registerModule(mapperModule);
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    // todo: we need type information for enums but Jackson doesn't seem to do that
    mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
  }


  public TypedJson(int maxCollectionElements) {
    this.map = new LinkedHashMap<>();
    this.maxCollectionElements = maxCollectionElements;
  }

  /**
   * Constructor for tests. This is equivalent to
   * new TypedJson(maxCollectionElements).add(key, value)
   */
  public TypedJson(String key, Object value, int maxCollectionElements) {
    List<Object> list = new ArrayList<>(1);
    this.map = new LinkedHashMap<>();
    if (value != null) {
      list.add(value);
    }
    this.map.put(key, list);
    this.maxCollectionElements = maxCollectionElements;
  }

  /**
   * User can build on this object by adding Objects against a key.
   *
   * TypedJson result = new TypedJson(); result.add(KEY,object); If users add more objects against
   * the same key the newly added object will be appended to the existing key forming an array of
   * objects.
   *
   * If the KEY is a new one then it will be a key map value.
   *
   * @param key Key against which an object will be added
   * @param value Object to be added
   * @return TypedJson object
   */
  public TypedJson add(String key, Object value) {
    List<Object> list = this.map.get(key);
    if (list != null) {
      list.add(value);
    } else {
      list = new ArrayList<>();
      list.add(value);
      this.map.put(key, list);
    }
    return this;
  }

  @Override
  public String toString() {
    StringWriter w = new StringWriter();
    synchronized (w.getBuffer()) {
      try {
        return this.write(w).toString();
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }
  }


  private Writer write(Writer writer) throws GfJsonException {
    try {
      boolean addComma = false;

      writer.write('{');
      for (Map.Entry<String, List<Object>> entry : this.map.entrySet()) {
        if (addComma) {
          writer.write(',');
        }
        mapper.writerFor(entry.getKey().getClass()).writeValue(writer, entry.getKey());
        writer.write(':');
        writeList(writer, entry.getValue());
        addCommaBeforeNextElement = false;
        addComma = true;
      }
      writer.write('}');

      return writer;
    } catch (IOException exception) {
      throw new GfJsonException(exception);
    }
  }

  private Writer writeList(Writer writer, List<Object> values) throws GfJsonException {
    try {
      boolean addComma = false;
      int length = values.size();

      writer.write('[');

      if (length == 0) {
        mapper.writeValueAsString(null);
      } else {
        for (int i = 0; i < length; i += 1) {
          if (addComma) {
            writer.write(',');
          }
          mapper.writeValue(writer, values.get(i));
          addCommaBeforeNextElement = false;
          addComma = true;
        }
      }
      writer.write(']');

      return writer;
    } catch (IOException e) {
      throw new GfJsonException(e);
    }
  }

  private String quote(String string) {
    StringWriter sw = new StringWriter();
    synchronized (sw.getBuffer()) {
      try {
        return quote(string, sw).toString();
      } catch (IOException ignored) {
        // will never happen - we are writing to a string writer
        return "";
      }
    }
  }


  /**
   * Handle some special GemFire classes. We don't want to expose some of the internal classes.
   * Hence corresponding interface or external classes should be shown.
   */
  private String internalToExternal(Class clazz, Object value) {
    if (value != null && value instanceof Region.Entry) {
      return Region.Entry.class.getCanonicalName();
    }
    if (value != null && value instanceof PdxInstance) {
      return PdxInstance.class.getCanonicalName();
    }
    return clazz.getCanonicalName();
  }

  private Writer quote(String string, Writer w) throws IOException {
    if (string == null || string.length() == 0) {
      w.write("\"\"");
      return w;
    }

    char b;
    char c = 0;
    String hhhh;
    int i;
    int len = string.length();

    w.write('"');
    for (i = 0; i < len; i += 1) {
      b = c;
      c = string.charAt(i);
      switch (c) {
        case '\\':
        case '"':
          w.write('\\');
          w.write(c);
          break;
        case '/':
          if (b == '<') {
            w.write('\\');
          }
          w.write(c);
          break;
        case '\b':
          w.write("\\b");
          break;
        case '\t':
          w.write("\\t");
          break;
        case '\n':
          w.write("\\n");
          break;
        case '\f':
          w.write("\\f");
          break;
        case '\r':
          w.write("\\r");
          break;
        default:
          if (c < ' ' || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
            hhhh = "000" + Integer.toHexString(c);
            w.write("\\u" + hhhh.substring(hhhh.length() - 4));
          } else {
            w.write(c);
          }
      }
    }
    w.write('"');
    return w;
  }


  class TypedJsonCollectionSerializer extends JsonSerializer<Collection> {
    @Override
    public void serializeWithType(Collection value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      typeSer.writeTypePrefix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
      _serialize(value, gen);
      typeSer.writeTypeSuffix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
    }

    @Override
    public void serialize(Collection value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      _serialize(value, gen);
      gen.writeEndObject();
    }

    public void _serialize(Collection value, JsonGenerator gen) throws IOException {
      Iterator<Object> objects = value.iterator();
      for (int i = 0; i < maxCollectionElements && objects.hasNext(); i++) {
        gen.writeObjectField("" + i, objects.next());
      }
    }

    @Override
    public Class<Collection> handledType() {
      return Collection.class;
    }
  }

  class TypedJsonMapSerializer extends JsonSerializer<Map> {
    @Override
    public void serializeWithType(Map value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      typeSer.writeTypePrefix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
      _serialize(value, gen);
      typeSer.writeTypeSuffix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
    }

    @Override
    public void serialize(Map value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      _serialize(value, gen);
      gen.writeEndObject();
    }

    public void _serialize(Map value, JsonGenerator gen) throws IOException {
      Iterator<Map.Entry> entries = value.entrySet().iterator();
      for (int i = 0; i < maxCollectionElements && entries.hasNext(); i++) {
        Map.Entry entry = entries.next();
        gen.writeObjectField(entry.getKey().toString(), entry.getValue());
      }
    }

    @Override
    public Class<Map> handledType() {
      return Map.class;
    }
  }

  static class TypedJsonPdxInstanceSerializer extends JsonSerializer<PdxInstance> {
    @Override
    public void serializeWithType(PdxInstance value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      typeSer.writeTypePrefix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
      _serialize(value, gen);
      typeSer.writeTypeSuffix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
    }

    @Override
    public void serialize(PdxInstance value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      _serialize(value, gen);
      gen.writeEndObject();
    }

    public void _serialize(PdxInstance value, JsonGenerator gen) throws IOException {
      for (String field : value.getFieldNames()) {
        gen.writeObjectField(field, value.getField(field));
      }
    }

    @Override
    public Class<PdxInstance> handledType() {
      return PdxInstance.class;
    }
  }

  static class TypedJsonStructSerializer extends JsonSerializer<StructImpl> {
    @Override
    public void serializeWithType(StructImpl value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      typeSer.writeTypePrefix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
      serialize(value, gen, serializers);
      typeSer.writeTypeSuffix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
    }

    @Override
    public void serialize(StructImpl value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      _serialize(value, gen);
      gen.writeEndObject();
    }

    public void _serialize(StructImpl value, JsonGenerator gen) throws IOException {
      String fields[] = value.getFieldNames();
      Object[] values = value.getFieldValues();
      for (int i = 0; i < fields.length; i++) {
        gen.writeObjectField(fields[i], values[i]);
      }
    }

    @Override
    public Class<StructImpl> handledType() {
      return StructImpl.class;
    }
  }

  static class TypedJsonRegionEntrySerializer extends JsonSerializer<Region.Entry> {
    @Override
    public void serializeWithType(Region.Entry value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      typeSer.writeTypePrefix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
      gen.writeObjectField(value.getKey().toString(), value.getValue());
      typeSer.writeTypeSuffix(gen, typeSer.typeId(value, JsonToken.START_OBJECT));
    }

    @Override
    public void serialize(Region.Entry value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeObjectField(value.getKey().toString(), value.getValue());
      gen.writeEndObject();
    }

    @Override
    public Class<Region.Entry> handledType() {
      return Region.Entry.class;
    }
  }
}
