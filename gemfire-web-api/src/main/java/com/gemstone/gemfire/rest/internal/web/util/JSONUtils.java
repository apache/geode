/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import org.json.JSONException;

import org.springframework.hateoas.Link;
import org.springframework.util.Assert;

/**
 * The JSONUtils class is a utility class for getting JSON equivalent from Java types.
 *
 * @author Nilkanth Patel.
 * @author John Blum
 * @since 8.0
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
    generator.enable(Feature.ESCAPE_NON_ASCII);
    generator.disable(Feature.AUTO_CLOSE_TARGET);
    generator.setPrettyPrinter(new DefaultPrettyPrinter());
    return generator;
  }

  public static String formulateJsonForListFunctionsCall(Set<String> functionIds) {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);
    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName("functions");
      JsonWriter.writeCollectionAsJson(generator, functionIds);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForListCall(Map<String, Link> LinksByName, String keyName, String fieldName) {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeListAsJson(generator, LinksByName, keyName, fieldName);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForListKeys(Object[] keys, String fieldName) {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      JsonWriter.writeObjectArrayAsJson(generator, keys, null);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForListRegions(Set<Region<?, ?>> regions, String fieldName) {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      JsonWriter.writeRegionSetAsJson(generator, regions);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForGetOnKey(Object value) throws JSONException {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeValueAsJson(generator, value, "GET_ON_KEY_RESPONSE");
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForGetOnMultipleKey(Collection<Object> collection, String regionName) throws JSONException {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      generator.writeStartObject();
      generator.writeFieldName(regionName);
      JsonWriter.writeCollectionAsJson(generator, collection);
      generator.writeEndObject();
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForListQueriesCall(Region<String, String> queryRegion) {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);
    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeQueryListAsJson(generator, "queries", queryRegion);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String formulateJsonForExistingQuery(String queryId, String oql) {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeQueryAsJson(generator, queryId, oql);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String convertCollectionToJson(Collection<Object> collection) throws JSONException {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeCollectionAsJson(generator, collection);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String convertMapToJson(Map<?, ?> map) throws JSONException {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeMapAsJson(generator, map, null);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

  public static String convertStructToJson(StructImpl structSet) throws JSONException {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(com.gemstone.gemfire.internal.Version.CURRENT);

    try {
      JsonGenerator generator = enableDisableJSONGeneratorFeature(getObjectMapper().getFactory()
        .createGenerator(outputStream, JsonEncoding.UTF8));
      JsonWriter.writeStructAsJson(generator, structSet);
      generator.close();
      return new String(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      outputStream.close();
    }
  }

}
