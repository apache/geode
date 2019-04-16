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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.TestExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufRequestUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * This integration test uses a Cache to hold PdxInstances in serialized form.
 * <p>
 * For "get" operations we put a PdxSerializable object into the cache in another JVM so that it
 * will be in serialized form in the unit test controller JVM. Then we pull it out using the
 * protobuf API and ensure that the result is in JSON document form.
 * <p>
 * For "put" operations we use the protobuf API to store a JSON document in the cache and then check
 * to make sure that a PdxInstance with a JSON signature is in the cache.
 * <p>
 * This addresses JIRA tickets GEODE-4116 and GEODE-4168.
 */
@Category({ClientServerTest.class})
public class GetAndPutJsonDocumentsDUnitTest {

  /** this JSON document is used by the "put" the tests */
  private static final String jsonDocument =
      "{\"name\":\"Charlemagne\",\"age\":1275,\"nationality\":\"french\",\"emailAddress\":\"none\"}";

  private static final PdxDocument pdxDocument =
      new PdxDocument("Charlemagne", 1275, "french", "none");

  /** this key is used to store the JSON document in the cache */
  private static final String key = "aPdxInstance";

  private static final String regionName = "TestRegion";

  private static ProtobufSerializationService serializationService;

  private MemberVM storingVM;

  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(1);

  public ServerStarterRule serverStarterRule = new ServerStarterRule();

  @Rule
  public RuleChain chain = RuleChain.outerRule(clusterStartupRule)
      .around(serverStarterRule);

  private InternalCache cache;
  private Region<String, Object> testRegion;

  @Before
  public void setUp() {
    serializationService = new ProtobufSerializationService();

    final MemberVM locatorVM = clusterStartupRule.startLocatorVM(0);
    final int locatorPort = locatorVM.invoke(() -> ClusterStartupRule.getLocator().getPort());

    serverStarterRule.startServer(new Properties(), locatorPort);
    cache = serverStarterRule.getCache();

    // create a distributed region in two VMs so that we can store an object
    // in "storingVM" and so ensure that it is in serialized form in the other
    // VM
    testRegion =
        cache.<String, Object>createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    storingVM = clusterStartupRule.startServerVM(1, locatorPort);

    storingVM.invoke("create region", () -> {
      ClusterStartupRule.getCache().<String, Object>createRegionFactory(RegionShortcut.REPLICATE)
          .create(regionName);
    });
  }

  @Test
  public void testThatGetReturnsJSONDocumentForPdxInstance() throws Exception {
    storingVM.invoke(GetAndPutJsonDocumentsDUnitTest::storeTestDocument);

    RegionAPI.GetRequest getRequest = generateGetRequest(key);
    GetRequestOperationHandler operationHandler = new GetRequestOperationHandler();
    cache.setReadSerializedForCurrentThread(true);
    try {
      Result result = operationHandler.process(serializationService, getRequest,
          TestExecutionContext.getNoAuthCacheExecutionContext(cache));

      Assert.assertTrue(result instanceof Success);
      RegionAPI.GetResponse response = (RegionAPI.GetResponse) result.getMessage();
      assertEquals(BasicTypes.EncodedValue.ValueCase.JSONOBJECTRESULT,
          response.getResult().getValueCase());
      String actualValue = response.getResult().getJsonObjectResult();
      assertEquals(jsonDocument, actualValue);
    } finally {
      cache.setReadSerializedForCurrentThread(false);
    }
  }

  @Test
  public void testThatGetAllReturnsJSONDocumentForPdxInstance() throws Exception {
    storingVM.invoke(GetAndPutJsonDocumentsDUnitTest::storeTestDocument);

    cache.setReadSerializedForCurrentThread(true);
    try {
      RegionAPI.GetAllRequest getRequest = generateGetAllRequest(key);
      GetAllRequestOperationHandler operationHandler = new GetAllRequestOperationHandler();
      Result result = operationHandler.process(serializationService, getRequest,
          TestExecutionContext.getNoAuthCacheExecutionContext(cache));

      Assert.assertTrue(result instanceof Success);
      RegionAPI.GetAllResponse response = (RegionAPI.GetAllResponse) result.getMessage();
      BasicTypes.Entry entry = response.getEntriesList().get(0);
      BasicTypes.EncodedValue entryValue = entry.getValue();
      assertEquals(BasicTypes.EncodedValue.ValueCase.JSONOBJECTRESULT, entryValue.getValueCase());
      String actualValue = entryValue.getJsonObjectResult();
      assertEquals(jsonDocument, actualValue);
    } finally {
      cache.setReadSerializedForCurrentThread(false);
    }
  }

  @Test
  public void testThatPutCreatesPdxInstanceFromJsonDocument() throws Exception {
    RegionAPI.PutRequest putRequest = generatePutRequest(key, jsonDocument);
    PutRequestOperationHandler operationHandler = new PutRequestOperationHandler();
    Result result = operationHandler.process(serializationService, putRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cache));

    Assert.assertTrue(result instanceof Success);
    PdxInstance pdxInstance = (PdxInstance) testRegion.get(key);
    assertEquals("__GEMFIRE_JSON", pdxInstance.getClassName());
  }

  @Test
  public void testThatPutAllCreatesPdxInstanceFromJsonDocument() throws Exception {
    RegionAPI.PutAllRequest putRequest = generatePutAllRequest(key, jsonDocument);
    PutAllRequestOperationHandler operationHandler = new PutAllRequestOperationHandler();
    Result result = operationHandler.process(serializationService, putRequest,
        TestExecutionContext.getNoAuthCacheExecutionContext(cache));

    Assert.assertTrue(result instanceof Success);
    PdxInstance pdxInstance = (PdxInstance) testRegion.get(key);
    assertEquals("__GEMFIRE_JSON", pdxInstance.getClassName());
  }


  ///////////////////////////////// methods for encoding messages //////////////////////////////

  // separate static method to avoid serializing the test class.
  // We only have to put on a different VM because we don't properly handle PdxSerializable (but not
  // actually serialized as a PdxInstance) objects.
  private static void storeTestDocument() {
    ClusterStartupRule.getCache().getRegion(regionName).put(key, pdxDocument);
  }

  private RegionAPI.GetRequest generateGetRequest(String key) throws EncodingException {
    BasicTypes.EncodedValue testKey = serializationService.encode(key);
    return ProtobufRequestUtilities.createGetRequest(regionName, testKey).getGetRequest();
  }

  private RegionAPI.GetAllRequest generateGetAllRequest(String key) throws EncodingException {
    HashSet<BasicTypes.EncodedValue> testKeys = new HashSet<>();
    BasicTypes.EncodedValue testKey = serializationService.encode(key);
    testKeys.add(testKey);
    return ProtobufRequestUtilities.createGetAllRequest(regionName, testKeys);
  }

  private RegionAPI.PutRequest generatePutRequest(String key, String jsonDocument)
      throws EncodingException {
    BasicTypes.Entry testEntry = createKVEntry(key, jsonDocument);
    return ProtobufRequestUtilities.createPutRequest(regionName, testEntry).getPutRequest();
  }

  private RegionAPI.PutAllRequest generatePutAllRequest(String key, String jsonDocument)
      throws EncodingException {
    Set<BasicTypes.Entry> entries = new HashSet<>();
    entries.add(createKVEntry(key, jsonDocument));
    return ProtobufRequestUtilities.createPutAllRequest(regionName, entries).getPutAllRequest();
  }

  private BasicTypes.Entry createKVEntry(String key, String jsonDocument) throws EncodingException {
    BasicTypes.EncodedValue testKey = serializationService.encode(key);
    BasicTypes.EncodedValue testValue = encodeJSONDocument(jsonDocument);
    return ProtobufUtilities.createEntry(testKey, testValue);
  }

  private BasicTypes.EncodedValue encodeJSONDocument(String jsonDocument) {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    return builder.setJsonObjectResultBytes(ByteString.copyFromUtf8(jsonDocument)).build();
  }


  public static class PdxDocument implements PdxSerializable {
    private String name;
    private int age;
    private String nationality;
    private String emailAddress;

    public PdxDocument() {}


    public PdxDocument(String name, int age, String nationality, String emailAddress) {
      this.name = name;
      this.age = age;
      this.nationality = nationality;
      this.emailAddress = emailAddress;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("name", name).writeInt("age", age).writeString("nationality", nationality)
          .writeString("emailAddress", emailAddress);
    }

    @Override
    public void fromData(PdxReader reader) {
      name = reader.readString("name");
      age = reader.readInt("age");
      nationality = reader.readString("nationality");
      emailAddress = reader.readString("emailAddress");
    }
  }
}
