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
package org.apache.geode.internal.protocol.protobuf.v1.serialization.codec;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.JsonPdxConverter;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class JsonPdxConverterJUnitTest {

  private String complexJSONString = "{\n" + "    \"_id\": \"599c7d885df276ac3e0bf10a\",\n"
      + "    \"index\": 0,\n" + "    \"guid\": \"395902d8-36ed-4178-ad70-2f720c557c55\",\n"
      + "    \"isActive\": true,\n" + "    \"balance\": \"$3,152.82\",\n"
      + "    \"picture\": \"http://placehold.it/32x32\",\n" + "    \"age\": 27,\n"
      + "    \"eyeColor\": \"blue\",\n" + "    \"name\": \"Kristina Norman\",\n"
      + "    \"gender\": \"female\",\n" + "    \"company\": \"ORBALIX\",\n"
      + "    \"email\": \"kristinanorman@orbalix.com\",\n"
      + "    \"phone\": \"+1 (983) 526-3433\",\n"
      + "    \"address\": \"400 Vermont Court, Denio, Wyoming, 7142\",\n"
      + "    \"about\": \"Mollit nostrud irure excepteur veniam aliqua. Non id tempor magna nisi ipsum minim. Culpa velit tempor culpa mollit cillum deserunt nisi culpa irure ut nostrud enim consectetur voluptate. Elit veniam velit enim minim. Sunt nostrud ea duis enim sit cillum.\",\n"
      + "    \"registered\": \"2015-03-11T02:22:45 +07:00\",\n" + "    \"latitude\": -0.853065,\n"
      + "    \"longitude\": -29.749358,\n" + "    \"tags\": [\n" + "      \"laboris\",\n"
      + "      \"velit\",\n" + "      \"non\",\n" + "      \"est\",\n" + "      \"anim\",\n"
      + "      \"amet\",\n" + "      \"cupidatat\"\n" + "    ],\n" + "    \"friends\": [\n"
      + "      {\n" + "        \"id\": 0,\n" + "        \"name\": \"Roseann Roy\"\n" + "      },\n"
      + "      {\n" + "        \"id\": 1,\n" + "        \"name\": \"Adriana Perry\"\n"
      + "      },\n" + "      {\n" + "        \"id\": 2,\n"
      + "        \"name\": \"Tyler Mccarthy\"\n" + "      }\n" + "    ],\n"
      + "    \"greeting\": \"Hello, Kristina Norman! You have 8 unread messages.\",\n"
      + "    \"favoriteFruit\": \"apple\"\n" + "  }";
  private Cache cache;

  @Before
  public void setUp() {
    CacheFactory cacheFactory = new CacheFactory();
    cacheFactory.set(ConfigurationProperties.MCAST_PORT, "0");
    cacheFactory.set(ConfigurationProperties.USE_CLUSTER_CONFIGURATION, "false");
    cacheFactory.set(ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, "false");
    cache = cacheFactory.create();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void testSimpleJSONEncode() {
    PdxInstanceFactory pdxInstanceFactory =
        ((GemFireCacheImpl) cache).createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);

    pdxInstanceFactory.writeString("string", "someString");
    pdxInstanceFactory.writeBoolean("boolean", true);
    PdxInstance pdxInstance = pdxInstanceFactory.create();

    String encodedJSON = new JsonPdxConverter().encode(pdxInstance);

    String expectedJsonString = "{\"string\":\"someString\",\"boolean\":true}";
    assertEquals(expectedJsonString, encodedJSON);
  }

  @Test
  public void testComplexJSONEncode() {
    PdxInstance pdxInstanceForComplexJSONString = createPDXInstanceForComplexJSONString();
    PdxInstance decodedJSONPdxInstance = new JsonPdxConverter().decode(complexJSONString);

    pdxInstanceEquals(pdxInstanceForComplexJSONString, decodedJSONPdxInstance);
  }

  private void pdxInstanceEquals(PdxInstance expectedPdxInstance,
      PdxInstance decodedJSONPdxInstance) {
    List<String> expectedFieldNames = expectedPdxInstance.getFieldNames();

    assertEquals(expectedFieldNames, decodedJSONPdxInstance.getFieldNames());

    expectedFieldNames.forEach(fieldName -> {
      assertEquals(expectedPdxInstance.getField(fieldName).getClass(),
          decodedJSONPdxInstance.getField(fieldName).getClass());
      assertEquals(pdxFieldValues(expectedPdxInstance, fieldName),
          pdxFieldValues(decodedJSONPdxInstance, fieldName));
    });
  }

  /**
   * This method is very specific to this test. It will take a pdxInstance object and return you the
   * value for the fieldName. In most cases it will return the value directly, but in the case of
   * collections LinkedList&lt;String&gt; it will return an ArrayList&lt;String&gt; or in the case
   * of a LinkedList&lt;PdxInstance&gt; it will return an ArrayList&lt;ArrayList&lt;String&gt;&gt;.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object pdxFieldValues(PdxInstance pdxInstance, String fieldName) {
    Object fieldValue = pdxInstance.getField(fieldName);
    // Check if the value is of type PDXInstance. If so, then iterate over its fields and return an
    // ArrayList of all the values
    if (fieldValue instanceof PdxInstance) {
      ArrayList<Object> objects = new ArrayList<>();
      ((PdxInstance) fieldValue).getFieldNames().forEach(
          innerFieldName -> objects.add(pdxFieldValues((PdxInstance) fieldValue, innerFieldName)));
      return objects;
    }
    // Else check if the value is of type LinkedList. Then it is a collection of either type String
    // or type PDXInstance. If of type String, then return an ArrayList<String> otherwise the
    // collection
    // contains a collection of PdxInstance.
    else if (fieldValue instanceof LinkedList) {
      @SuppressWarnings("unchecked")
      LinkedList<Object> value = (LinkedList) fieldValue;
      // if the first value of the LinkedList is not a PDXInstance return the LinkedList
      if (!value.isEmpty() && !(value.getFirst() instanceof PdxInstance)) {
        return value;
      } else {
        // Here the LinkedList contains PDXInstances. Here we will iterate the linkedList and
        // process
        // each pdxInstance into and ArrayList of the pdx's values.
        ArrayList<Object> objects = new ArrayList<>();
        value.forEach(internalPdxInstance -> {
          ArrayList<Object> innerObject = new ArrayList<>();
          ((PdxInstance) internalPdxInstance).getFieldNames()
              .forEach(internalFieldName -> innerObject
                  .add(pdxFieldValues((PdxInstance) internalPdxInstance, internalFieldName)));
          objects.add(innerObject);
          objects.sort((Comparator) (o1, o2) -> (byte) ((ArrayList) o1).get(0));
        });
        return objects;
      }
    }
    // Otherwise if the field is not a PdxInstance or LinkedList, then return the value.
    else {
      return fieldValue;
    }
  }

  /**
   * Create a PDXInstance object that is equivalent to @link{complexJSONString}
   */
  private PdxInstance createPDXInstanceForComplexJSONString() {
    PdxInstanceFactory friendPdxFactory =
        ((InternalCache) cache).createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);

    friendPdxFactory.writeByte("id", (byte) 0);
    PdxInstance friendPdx1 = friendPdxFactory.writeString("name", "Roseann Roy").create();

    WritablePdxInstance friendPdx2 = friendPdx1.createWriter();
    friendPdx2.setField("id", (byte) 1);
    friendPdx2.setField("name", "Adriana Perry");

    WritablePdxInstance friendPdx3 = friendPdx1.createWriter();
    friendPdx3.setField("id", (byte) 2);
    friendPdx3.setField("name", "Tyler Mccarthy");

    PdxInstanceFactory pdxInstanceFactory =
        cache.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME);
    pdxInstanceFactory.writeString("_id", "599c7d885df276ac3e0bf10a");
    pdxInstanceFactory.writeByte("index", (byte) 0);
    pdxInstanceFactory.writeString("guid", "395902d8-36ed-4178-ad70-2f720c557c55");
    pdxInstanceFactory.writeBoolean("isActive", true);
    pdxInstanceFactory.writeString("balance", "$3,152.82");
    pdxInstanceFactory.writeString("picture", "http://placehold.it/32x32");
    pdxInstanceFactory.writeByte("age", (byte) 27);
    pdxInstanceFactory.writeString("eyeColor", "blue");
    pdxInstanceFactory.writeString("name", "Kristina Norman");
    pdxInstanceFactory.writeString("gender", "female");
    pdxInstanceFactory.writeString("company", "ORBALIX");
    pdxInstanceFactory.writeString("email", "kristinanorman@orbalix.com");
    pdxInstanceFactory.writeString("phone", "+1 (983) 526-3433");
    pdxInstanceFactory.writeString("address", "400 Vermont Court, Denio, Wyoming, 7142");
    pdxInstanceFactory.writeString("about",
        "Mollit nostrud irure excepteur veniam aliqua. Non id tempor magna nisi ipsum minim. Culpa velit tempor culpa mollit cillum deserunt nisi culpa irure ut nostrud enim consectetur voluptate. Elit veniam velit enim minim. Sunt nostrud ea duis enim sit cillum.");
    pdxInstanceFactory.writeString("registered", "2015-03-11T02:22:45 +07:00");
    pdxInstanceFactory.writeDouble("latitude", -0.853065);
    pdxInstanceFactory.writeDouble("longitude", -29.749358);
    pdxInstanceFactory.writeObject("tags",
        new LinkedList<>(asList("laboris", "velit", "non", "est", "anim", "amet", "cupidatat")));
    pdxInstanceFactory.writeObject("friends",
        new LinkedList<>(asList(friendPdx1, friendPdx2, friendPdx3)));
    pdxInstanceFactory.writeString("greeting",
        "Hello, Kristina Norman! You have 8 unread messages.");
    pdxInstanceFactory.writeString("favoriteFruit", "apple");
    return pdxInstanceFactory.create();
  }

  @Test
  public void testJSONDecode() {
    PdxInstance pdxInstance = new JsonPdxConverter().decode(complexJSONString);

    assertNotNull(pdxInstance);
    List<String> fieldNames = asList("_id", "index", "guid", "isActive", "balance", "picture",
        "age", "eyeColor", "name", "gender", "company", "email", "phone", "address", "about",
        "registered", "latitude", "longitude", "tags", "friends", "greeting", "favoriteFruit");
    assertEquals(fieldNames, pdxInstance.getFieldNames());
  }

}
