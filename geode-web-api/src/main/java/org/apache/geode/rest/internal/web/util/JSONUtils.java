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

package org.apache.geode.rest.internal.web.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.Assert;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.Version;

/**
 * The JSONUtils class is a utility class for getting JSON equivalent from Java types.
 *
 * @since GemFire 8.0
 */
public abstract class JSONUtils {

  private static AtomicReference<ObjectMapper> objectMapper = new AtomicReference<>(null);

  public static void setObjectMapper(final ObjectMapper objectMapper) {
    JSONUtils.objectMapper.compareAndSet(null, objectMapper);
  }

  private static ObjectMapper getObjectMapper() {
    ObjectMapper localObjectMapper = objectMapper.get();
    Assert.state(localObjectMapper != null, "The ObjectMapper reference must not be null!");
    return localObjectMapper;
  }

  public static JsonGenerator enableDisableJSONGeneratorFeature(JsonGenerator generator) {
    generator.enable(JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature());
    generator.disable(Feature.AUTO_CLOSE_TARGET);
    generator.setPrettyPrinter(new DefaultPrettyPrinter());
    return generator;
  }

  public static String formulateJsonForListFunctionsCall(Set<String> functionIds) {
    try (HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT)) {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
          .createGenerator((OutputStream) outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName("functions");
      JsonWriter.writeCollectionAsJson(generator, functionIds);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String formulateJsonForListKeys(Object[] keys, String fieldName) {

    try (HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT)) {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
          .createGenerator((OutputStream) outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      JsonWriter.writeObjectArrayAsJson(generator, keys, null);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String formulateJsonForListRegions(Set<Region<?, ?>> regions, String fieldName) {

    try (HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT)) {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
          .createGenerator((OutputStream) outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      JsonWriter.writeRegionSetAsJson(generator, regions);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }


  public static String formulateJsonForListQueriesCall(Region<String, String> queryRegion) {
    try (HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT)) {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
          .createGenerator((OutputStream) outputStream, JsonEncoding.UTF8));
      JsonWriter.writeQueryListAsJson(generator, "queries", queryRegion);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String formulateJsonForExistingQuery(String queryId, String oql) {

    try (HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT)) {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
          .createGenerator((OutputStream) outputStream, JsonEncoding.UTF8));
      JsonWriter.writeQueryAsJson(generator, queryId, oql);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String convertCollectionToJson(Collection<Object> collection) {

    try (HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT)) {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
          .createGenerator((OutputStream) outputStream, JsonEncoding.UTF8));
      JsonWriter.writeCollectionAsJson(generator, collection);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
