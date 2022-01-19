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
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.management.internal.util.JsonUtil;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceImpl;

public abstract class AbstractJSONFormatter {
  final ObjectMapper mapper;
  final boolean generateTypeInformation;
  final Set<Class> nonOverridableSerializers;

  /*
   * serializedObjects is used to prevent recursive serialization in cases where
   * there are cyclical references
   */
  final Map<Object, Object> serializedObjects;

  /**
   * Create a formatter that will limit collection sizes to maxCollectionElements
   *
   * @param maxCollectionElements limit on collection elements
   * @param serializationDepth when traversing objects, how deep should we go?
   * @param generateTypeInformation whether generated JSON should have java type information
   */
  public AbstractJSONFormatter(int maxCollectionElements, int serializationDepth,
      boolean generateTypeInformation) {

    int serDepth = serializationDepth < 0 ? Integer.MAX_VALUE : serializationDepth;
    int maxElements = maxCollectionElements < 0 ? Integer.MAX_VALUE : maxCollectionElements;

    mapper = new ObjectMapper();
    serializedObjects = new IdentityHashMap<>();
    this.generateTypeInformation = generateTypeInformation;
    nonOverridableSerializers = new HashSet<>();

    SimpleModule mapperModule = new PreventReserializationModule(serializedObjects, serDepth);

    // insert a Struct serializer that knows about its format
    mapperModule.addSerializer(StructImpl.class, new StructSerializer());
    nonOverridableSerializers.add(StructImpl.class);

    // insert a PdxInstance serializer that knows about PDX fields/values
    mapperModule.addSerializer(PdxInstance.class, new PdxInstanceSerializer());
    nonOverridableSerializers.add(PdxInstanceImpl.class);

    // insert a RegionEntry serializer because they're too messy looking
    mapperModule.addSerializer(Region.Entry.class, new RegionEntrySerializer());
    nonOverridableSerializers.add(Region.Entry.class);

    // insert a collection serializer that limits the number of elements generated
    mapperModule.addSerializer(Collection.class, new CollectionSerializer(maxElements));

    // register the custom module
    mapper.registerModule(mapperModule);
    // to support jdk8 java.time if jackson-datatype-jsr310 is included in the classpath
    mapper.findAndRegisterModules();

    mapper.disable(MapperFeature.USE_ANNOTATIONS);
    // allow objects with no content
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    // use toString on Enums
    mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    // sort fields alphabetically
    mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);

    postCreateMapper();
  }

  void postCreateMapper() {
    // let subclasses modify the mapper
  }

  // TODO: Re-Implement this class.
  // Right now we avoid serializing objects that we've already serialized and, instead, just add
  // some custom markers ('duplicate' / 'reference@') to the JSON document. These markers, however,
  // can't be effectively used by clients of this class because the originally serialized objects
  // don't have any kind of 'id' in the resulting document (the 'reference@' marker might be
  // used BUT the document needs to be de-serialized within the same JVM on which it was
  // serialized AND IT MUST be Java - System.identityHashCode(object).
  private static class PreventReserializationSerializer extends JsonSerializer {
    int depth;
    Map serializedObjects;
    private final JsonSerializer defaultSerializer;
    private final int serializationDepth;

    PreventReserializationSerializer(JsonSerializer serializer, Map serializedObjects,
        int serializationDepth) {
      defaultSerializer = serializer;
      this.serializedObjects = serializedObjects;
      this.serializationDepth = serializationDepth;
    }

    private boolean isPrimitiveOrWrapper(Class<?> klass) {
      return JsonUtil.isPrimitiveOrWrapper(klass);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serializeWithType(Object value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      if (value == null || isPrimitiveOrWrapper(value.getClass()) || value.getClass().isEnum()) {
        defaultSerializer.serializeWithType(value, gen, serializers, typeSer);
        return;
      }
      depth += 1;
      try {
        if (depth > serializationDepth) {
          gen.writeString("{}");
        } else if (serializedObjects.containsKey(value)) {
          gen.writeString("duplicate " + value.getClass().getName());
        } else {
          serializedObjects.put(value, value);
          defaultSerializer.serializeWithType(value, gen, serializers, typeSer);
        }
      } finally {
        depth--;
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value == null || isPrimitiveOrWrapper(value.getClass()) || value.getClass().isEnum()) {
        defaultSerializer.serialize(value, gen, serializers);
        return;
      }
      if (serializedObjects.containsKey(value)) {
        gen.writeStartObject(value);
        gen.writeFieldName("duplicate");
        gen.writeString("reference@" + Integer.toHexString(System.identityHashCode(value)));
        gen.writeEndObject();
      } else {
        serializedObjects.put(value, value);
        defaultSerializer.serialize(value, gen, serializers);
      }
    }
  }


  /**
   * A custom JSON serializer for Struct.
   */
  private static class StructSerializer extends JsonSerializer<StructImpl> {
    @Override
    public void serializeWithType(StructImpl value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      // We don't want to expose the internal classes, show interface instead.
      WritableTypeId typeId = typeSer.typeId(value, Struct.class, JsonToken.START_OBJECT);
      typeSer.writeTypePrefix(gen, typeId);
      serializeElements(value, gen);
      typeSer.writeTypeSuffix(gen, typeId);
    }

    @Override
    public void serialize(StructImpl value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      serializeElements(value, gen);
      gen.writeEndObject();
    }

    void serializeElements(StructImpl value, JsonGenerator gen) throws IOException {
      String[] fields = value.getFieldNames();
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

  /**
   * A custom JSON serializer for PdxInstances.
   */
  private static class PdxInstanceSerializer extends JsonSerializer<PdxInstance> {
    @Override
    public void serializeWithType(PdxInstance value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      // We don't want to expose the internal classes, show interface instead.
      WritableTypeId typeId = typeSer.typeId(value, PdxInstance.class, JsonToken.START_OBJECT);
      typeSer.writeTypePrefix(gen, typeId);
      serializeFields(value, gen);
      typeSer.writeTypeSuffix(gen, typeId);
    }

    @Override
    public void serialize(PdxInstance value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      serializeFields(value, gen);
      gen.writeEndObject();
    }

    void serializeFields(PdxInstance value, JsonGenerator gen) throws IOException {
      for (String field : value.getFieldNames()) {
        gen.writeObjectField(field, value.getField(field));
      }
    }

    @Override
    public Class<PdxInstance> handledType() {
      return PdxInstance.class;
    }
  }

  /**
   * A custom JSON serializer for region entries.
   */
  private static class RegionEntrySerializer extends JsonSerializer<Region.Entry> {
    @Override
    public void serializeWithType(Region.Entry value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      typeSer.writeTypePrefix(gen,
          typeSer.typeId(value, Region.Entry.class, JsonToken.START_OBJECT));
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

  /**
   * A JSON serializer for collections to limit the number of elements written to the document.
   */
  private static class CollectionSerializer extends JsonSerializer<Collection> {
    private final int maxCollectionElements;

    CollectionSerializer(int maxCollectionElements) {
      this.maxCollectionElements = maxCollectionElements;
    }

    @Override
    public void serializeWithType(Collection value, JsonGenerator gen,
        SerializerProvider serializers, TypeSerializer typeSer)
        throws IOException {
      gen.setCurrentValue(value);
      WritableTypeId typeIdDef = typeSer.writeTypePrefix(gen,
          typeSer.typeId(value, JsonToken.START_OBJECT));
      serializeElements(value, gen);
      typeSer.writeTypeSuffix(gen, typeIdDef);
    }

    @Override
    public void serialize(Collection value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      serializeElements(value, gen);
      gen.writeEndObject();
    }

    void serializeElements(Collection value, JsonGenerator gen) throws IOException {
      Iterator objects = value.iterator();
      for (int i = 0; i < maxCollectionElements && objects.hasNext(); i++) {
        Object nextObject = objects.next();
        gen.writeObjectField("" + i, nextObject);
      }
    }

    @Override
    public Class<Collection> handledType() {
      return Collection.class;
    }
  }

  /**
   * A Jackson module that installs a serializer-modifier to detect and prevent
   * re-serialization of objects that have already been serialized. W/o this
   * Jackson would throw infinite-recursion exceptions.
   */
  private static class PreventReserializationModule extends SimpleModule {
    private final Map<Object, Object> serializedObjects;
    private final int serializationDepth;

    PreventReserializationModule(Map<Object, Object> serializedObjects, int serializationDepth) {
      this.serializedObjects = serializedObjects;
      this.serializationDepth = serializationDepth;
    }

    @Override
    public void setupModule(SetupContext context) {
      // install a modifier that prevents recursive serialization in cases where
      // there are cyclical references
      super.setupModule(context);
      context.addBeanSerializerModifier(new BeanSerializerModifier() {
        @Override
        public JsonSerializer<?> modifySerializer(
            SerializationConfig config, BeanDescription desc, JsonSerializer<?> serializer) {
          return new PreventReserializationSerializer(serializer, serializedObjects,
              serializationDepth);
        }
      });
    }
  }
}
