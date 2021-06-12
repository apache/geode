/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.json;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.ResolvableSerializer;
import com.fasterxml.jackson.databind.type.ArrayType;

import org.apache.geode.internal.logging.DateFormatter;

public class QueryResultFormatter extends AbstractJSONFormatter {
  /**
   * map contains the named objects to be serialized
   */
  private final Map<String, List<Object>> map;

  /**
   * Create a formatter that will limit collection sizes to maxCollectionElements and will limit
   * object traversal to being the same but in depth.
   *
   * @param maxCollectionElements limit on collection elements and depth-first object traversal
   */
  public QueryResultFormatter(int maxCollectionElements) {
    this(maxCollectionElements, maxCollectionElements);
  }

  /**
   * Create a formatter that will limit collection sizes to maxCollectionElements
   *
   * @param maxCollectionElements limit on collection elements
   * @param serializationDepth when traversing objects, how deep should we go?
   */
  private QueryResultFormatter(int maxCollectionElements, int serializationDepth) {
    super(maxCollectionElements, serializationDepth, true);
    this.map = new LinkedHashMap<>();
  }

  @Override
  void postCreateMapper() {
    // Backward compatibility, always serialize type information. See GEODE-6808.
    if (generateTypeInformation) {
      TypeSerializationEnforcerModule typeModule =
          new TypeSerializationEnforcerModule(nonOverridableSerializers);

      // Consistency: use the same date format java.sql.Date as well as java.util.Date.
      mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
      SimpleDateFormat sdf = DateFormatter.createLocalizedDateFormat();
      mapper.setDateFormat(sdf);
      typeModule.addSerializer(java.sql.Date.class, new SqlDateSerializer(mapper.getDateFormat()));

      // Register module
      mapper.registerModule(typeModule);

      // Add type information whenever possible (Jackson has no way to force it for all values)
      mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    }
  }

  /**
   * After instantiating a formatter add the objects you want to be formatted using this method.
   * Typically this will be add("result", queryResult)
   */
  public synchronized QueryResultFormatter add(String key, Object value) {
    List<Object> list = this.map.get(key);
    if (list != null) {
      list.add(value);
    } else {
      list = new ArrayList<>();
      if (value != null) {
        list.add(value);
      }
      this.map.put(key, list);
    }
    return this;
  }

  /* non-javadoc use Jackson to serialize added objects into JSON format */
  @Override
  public synchronized String toString() {
    Writer writer = new StringWriter();
    try {
      boolean addComma = false;
      writer.write('{');
      for (Map.Entry<String, List<Object>> entry : this.map.entrySet()) {
        if (addComma) {
          writer.write(',');
        }

        // Keys are always of type String, in order to keep backward compatibility we need to
        // manually serialize them without type.
        writer.write("\"");
        writer.write(entry.getKey());
        writer.write("\"");
        writer.write(':');
        writeList(writer, entry.getValue());
        addComma = true;
      }
      writer.write('}');

      return writer.toString();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  private void writeList(Writer writer, List<Object> values) {
    // for each object we clear out the serializedObjects recursion map so that
    // we don't immediately see "duplicate" entries
    serializedObjects.clear();

    try {
      writer.write('[');
      boolean addComma = false;
      int length = values.size();

      if (length == 0) {
        mapper.writeValue(writer, null);
      } else {
        for (Object value : values) {
          if (addComma) {
            writer.write(',');
          }

          mapper.writerFor(value.getClass()).writeValue(writer, value);
          addComma = true;
        }
      }

      writer.write(']');
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Internal custom serializer for java.sql.Date.
   */
  private static class SqlDateSerializer extends JsonSerializer<java.sql.Date> {
    private final DateFormat dateFormat;

    SqlDateSerializer(DateFormat dateFormat) {
      this.dateFormat = dateFormat;
    }

    private void serializeInternal(java.sql.Date value, JsonGenerator gen) throws IOException {
      gen.writeString(dateFormat.format(value));
    }

    @Override
    public void serialize(java.sql.Date value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      serializeInternal(value, gen);
    }

    @Override
    public void serializeWithType(java.sql.Date value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
      serializeInternal(value, gen);
    }
  }

  /**
   * Internal custom serializer for all beans and primitive types.
   * Serializes the bean type information as a JSON array ("[]") into the buffer if and only if the
   * bean is not part of a primitive array. Afterwards it just delegates to the default serializer.
   */
  private static class ContextAwareBeanSerializer<T> extends JsonSerializer<T> {
    private final JsonSerializer defaultSerializer;

    ContextAwareBeanSerializer(JsonSerializer defaultSerializer) {
      this.defaultSerializer = defaultSerializer;
    }

    @SuppressWarnings("unchecked")
    private void serializeInternal(T value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (gen.getOutputContext().inArray()) {
        defaultSerializer.serialize(value, gen, serializers);
      } else {
        gen.writeStartArray();
        gen.writeString(value.getClass().getCanonicalName());
        if (defaultSerializer instanceof ResolvableSerializer) {
          ((ResolvableSerializer) defaultSerializer).resolve(serializers);
        }
        defaultSerializer.serialize(value, gen, serializers);
        gen.writeEndArray();
      }
    }

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      serializeInternal(value, gen, serializers);
    }

    @Override
    public void serializeWithType(T value, JsonGenerator gen, SerializerProvider serializers,
        TypeSerializer typeSer) throws IOException {
      serializeInternal(value, gen, serializers);
    }
  }

  /**
   * Custom array serializer.
   * Serializes the array type information as a JSON array ("[]") into the buffer and delegates to
   * the default serializer afterwards. Eventually the serialization mechanism will end up invoking
   * ContextAwareBeanSerializer, which won't serialize the type information for every single
   * element within the array.
   */
  private static class CustomArraySerializer<T> extends JsonSerializer<T> {
    private final JsonSerializer defaultSerializer;

    CustomArraySerializer(JsonSerializer defaultSerializer) {
      this.defaultSerializer = defaultSerializer;
    }

    @SuppressWarnings("unchecked")
    private void serializeInternal(T value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartArray();
      gen.writeString(value.getClass().getCanonicalName());
      defaultSerializer.serialize(value, gen, serializers);
      gen.writeEndArray();
    }

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      serializeInternal(value, gen, serializers);
    }

    @Override
    public void serializeWithType(T value, JsonGenerator gen, SerializerProvider serializers,
        TypeSerializer typeSer) throws IOException {
      serializeInternal(value, gen, serializers);
    }
  }

  /**
   * A Jackson module that installs serializer-modifiers to serialize type information for
   * all types based on the current serialization context.
   */
  private static class TypeSerializationEnforcerModule extends SimpleModule {
    private final Set<Class> nonOverridableSerializers;

    TypeSerializationEnforcerModule(Set<Class> nonOverridableSerializers) {
      super();
      this.nonOverridableSerializers = nonOverridableSerializers;
    }

    @Override
    public void setupModule(SetupContext context) {
      super.setupModule(context);

      context.addBeanSerializerModifier(new BeanSerializerModifier() {
        @Override
        public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription desc,
            JsonSerializer<?> serializer) {
          if (!nonOverridableSerializers.contains(desc.getBeanClass())) {
            return new ContextAwareBeanSerializer<>(serializer);
          }

          return serializer;
        }

        @Override
        public JsonSerializer<?> modifyArraySerializer(SerializationConfig config,
            ArrayType valueType, BeanDescription beanDesc, JsonSerializer<?> serializer) {
          return new CustomArraySerializer<>(serializer);
        }
      });
    }
  }
}
