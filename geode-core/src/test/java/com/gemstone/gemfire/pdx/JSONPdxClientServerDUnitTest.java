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
package com.gemstone.gemfire.pdx;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.internal.json.PdxToJSON;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

import org.json.JSONException;
import org.json.JSONObject;
/**
 * @author hiteshk
 *
 */
public class JSONPdxClientServerDUnitTest extends CacheTestCase {

  public JSONPdxClientServerDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void preTearDownCacheTestCase() {
    // this test creates client caches in some VMs and so
    // breaks the contract of CacheTestCase to hold caches in
    // that class's "cache" instance variable
    disconnectAllFromDS();
  }

  public void testSimplePut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    createServerRegion(vm0);
    int port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        JSONAllStringTest();
        return null;
      }
    });
   
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        JSONAllByteArrayTest();
        return null; 
      }
    });    
  }
  
  //this is for unquote fielnames in json string
  public void testSimplePut2() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    createServerRegion(vm0);
    int port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        JSONUnQuoteFields();
        return null;
      }
    });
     
  }
  
  public void testPdxInstanceAndJSONConversion() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    createServerRegion(vm0, true);
    int port = createServerRegion(vm3, true);
    createClientRegion(vm1, port, false, true);
    createClientRegion(vm2, port, false, true);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        VerifyPdxInstanceAndJsonConversion();
        return null;
      }
    }); 
  }

  public  void VerifyPdxInstanceAndJsonConversion() {
    Region region = getRootRegion("testSimplePdx");
    
    //Create Object and initialize its members.
    TestObjectForPdxFormatter testObject = new TestObjectForPdxFormatter();
    testObject.defaultInitialization();

    // put the object into cache.
    region.put("101", testObject);
    
    //Get the object as PdxInstance
    Object result = (Object)region.get("101");
    if (result instanceof PdxInstance) { 
      PdxInstance pi = (PdxInstance)result;
      String json = JSONFormatter.toJSON(pi);
     
      try {  
        JSONObject jsonObject = new JSONObject(json);
        
        //Testcase-1: Validate json string against the pdxInstance.
        //validation for primitive types
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Int type values are not matched",
            testObject.getP_int(), jsonObject.getInt(testObject.getP_intFN()));
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: long type values are not matched",
            testObject.getP_long(), jsonObject.getLong(testObject.getP_longFN()));
        
        //validation for wrapper types
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Boolean type values are not matched",
            testObject.getW_bool().booleanValue(), jsonObject.getBoolean(testObject.getW_boolFN()));
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Float type values are not matched",
            testObject.getW_double().doubleValue(), jsonObject.getDouble(testObject.getW_doubleFN()));
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: bigDec type values are not matched",
            testObject.getW_bigDec().longValue(), jsonObject.getLong(testObject.getW_bigDecFN()));
        
        //vlidation for array types
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Byte[] type values are not matched",
            (int)testObject.getW_byteArray()[1], jsonObject.getJSONArray(testObject.getW_byteArrayFN()).getInt(1));
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Double[] type values are not matched",
            testObject.getW_doubleArray()[0], jsonObject.getJSONArray(testObject.getW_doubleArrayFN()).getDouble(0));
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: String[] type values are not matched",
            testObject.getW_strArray()[2], jsonObject.getJSONArray(testObject.getW_strArrayFN()).getString(2));
        
        //validation for collection types
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: list type values are not matched", 
            testObject.getC_list().get(0), 
            jsonObject.getJSONArray(testObject.getC_listFN()).getString(0));
        
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: stack type values are not matched", 
            testObject.getC_stack().get(2), 
            jsonObject.getJSONArray(testObject.getC_stackFN()).getString(2));
        
        //validation for Map
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Map type values are not matched", 
            testObject.getM_empByCity().get("Ahmedabad").get(0).getFname(), 
            jsonObject.getJSONObject(testObject.getM_empByCityFN()).getJSONArray("Ahmedabad").getJSONObject(0).getString("fname"));
        
        //validation Enum
        junit.framework.Assert.assertEquals("VerifyPdxInstanceToJson: Enum type values are not matched", 
            testObject.getDay().toString(), 
            jsonObject.getString(testObject.getDayFN()));       
      } catch (JSONException e) {
        fail("Error in VerifyPdxInstanceToJson, Malformed json, can not create JSONArray from it");
      }
    }else {
      fail("Error in VerifyPdxInstanceToJson, result must be of type PdxInstance");
    }
    
    //TestCase-2 : Validate Java-->JSON-->PdxInstance --> Java Mapping
    TestObjectForPdxFormatter actualTestObject = new TestObjectForPdxFormatter();
    actualTestObject.defaultInitialization();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setDateFormat(new SimpleDateFormat("MM/dd/yyyy"));
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    
    try {
      //1. get the json from the object using Jackosn Object Mapper
      String json = objectMapper.writeValueAsString(actualTestObject);
      String jsonWithClassType = actualTestObject.addClassTypeToJson(json);
      
      //2. convert json into the PdxInstance and put it into the region
      PdxInstance pi = JSONFormatter.fromJSON(jsonWithClassType);
      region.put("201", pi);
      
      //3. get the value on key "201" and validate PdxInstance.getObject() API.
      Object receivedObject = region.get("201");
      if (receivedObject instanceof PdxInstance){
        PdxInstance receivedPdxInstance = (PdxInstance)receivedObject;
        
        //4. get the actualType testObject from the pdxInstance and compare it with actualTestObject
        Object getObj = receivedPdxInstance.getObject();
        if(getObj instanceof TestObjectForPdxFormatter){
          TestObjectForPdxFormatter receivedTestObject = (TestObjectForPdxFormatter)getObj;
          
          boolean isEqual = actualTestObject.equals(receivedTestObject);
          Assert.assertTrue(isEqual, "actualTestObject and receivedTestObject should be equal");
        }else {
          fail("getObj is expected to be an instance of TestObjectForPdxFormatter");
        }
      }else {
        fail("receivedObject is expected to be of type PdxInstance");
      }
      
    } catch (JsonProcessingException e) {
      fail("JsonProcessingException:  error encountered while converting JSON from Java object: " + e.getMessage());
      
    } catch (JSONException e) {
      fail("JSONException:  error encountered while adding @type classType into Json: " + e.getMessage());
    }
  }  
  
  String getJSONDir(String file) {
    String path = TestUtil.getResourcePath(getClass(), file);
    return new File(path).getParent();
  }
  
  public void JSONUnQuoteFields() {
    System.setProperty("pdxToJson.unqouteFieldNames", "true");
    PdxToJSON.PDXTOJJSON_UNQUOTEFIELDNAMES = true;
    String jsonStringsDir = getJSONDir("/com/gemstone/gemfire/pdx/jsonStrings/unquoteJsonStrings/json1.txt");
    JSONAllStringTest(jsonStringsDir);
    PdxToJSON.PDXTOJJSON_UNQUOTEFIELDNAMES = false;
  }  
  
  public void JSONAllStringTest() {
    String jsonStringsDir = getJSONDir("jsonStrings/json1.txt");
    JSONAllStringTest(jsonStringsDir);
  }
  
  public void JSONAllStringTest(String dirname) {
    
      
    JSONData[] allJsons = loadAllJSON(dirname);
    int i = 0;
    for(JSONData jsonData : allJsons)
    {
      if(jsonData != null) {
        i++;
        VerifyJSONString(jsonData);
      }
    }
    Assert.assertTrue(i >= 1, "Number of files should be more than 10 : " + i);
  }
  
  public void JSONAllByteArrayTest() {
    String jsonStringsDir = getJSONDir("jsonStrings/json1.txt");
      
    JSONData[] allJsons = loadAllJSON(jsonStringsDir);
    int i = 0;
    for(JSONData jsonData : allJsons)
    {
      if(jsonData != null) {
        i++;
        VerifyJSONByteArray(jsonData);
      }
    }
    Assert.assertTrue(i > 10, "Number of files should be more than 10");
  }
  
  static class JSONData {
    String jsonFileName;
    byte[] jsonByteArray;
    
    public JSONData(String fn, byte[] js) {
      jsonFileName = fn;
      jsonByteArray = js;
    }
    public String getFileName () {
      return jsonFileName;
    }
    public String getJsonString() {
      return new String(jsonByteArray);
    }
    public byte[] getJsonByteArray(){
      return jsonByteArray;
    }
  }
  
  
  public  void VerifyJSONString(JSONData jd) {
    Region r = getRootRegion("testSimplePdx");
    
    PdxInstance pdx = JSONFormatter.fromJSON(jd.getJsonString());
    
    r.put(1, pdx);
    
    pdx = (PdxInstance)r.get(1);
    
    String getJsonString = JSONFormatter.toJSON(pdx);
    
    String o1 = jsonParse(jd.getJsonString());
    String o2 = jsonParse(getJsonString);
    junit.framework.Assert.assertEquals("Json Strings are not equal " + jd.getFileName() + " " +  Boolean.getBoolean("pdxToJson.unqouteFieldNames"), o1, o2);
  
    PdxInstance pdx2 = JSONFormatter.fromJSON(getJsonString);
    
    junit.framework.Assert.assertEquals("Pdx are not equal; json filename " + jd.getFileName(), pdx, pdx2);    
  }  
  
  protected final static int INT_TAB = '\t';
  protected final static int INT_LF = '\n';
  protected final static int INT_CR = '\r';
  protected final static int INT_SPACE = 0x0020;
  
  public String jsonParse(String jsonSting) {
        
   byte[] ba = jsonSting.getBytes();
   byte [] withoutspace = new byte[ba.length];
  
      int i = 0;
      int j = 0;
      for(i =0; i< ba.length; i++) {
        int cbyte = ba[i];
        
        if(cbyte == INT_TAB || cbyte == INT_LF || cbyte == INT_CR || cbyte == INT_SPACE)
          continue;
        withoutspace[j++] = ba[i];
      }
      
      return new String(withoutspace, 0, j);
    
  }
  public  void VerifyJSONByteArray(JSONData jd) {
    Region r = getRootRegion("testSimplePdx");
    
    PdxInstance pdx = JSONFormatter.fromJSON(jd.getJsonByteArray());
    
    r.put(1, pdx);
    
    pdx = (PdxInstance)r.get(1);
    
    byte[] jsonByteArray = JSONFormatter.toJSONByteArray(pdx);
    
    byte[] o1 = jsonParse(jd.getJsonByteArray());
    byte[] o2 = jsonParse(jsonByteArray);
    
   // junit.framework.Assert.assertEquals("Pdx byte aray are not equal after fetching from cache " + jd.getFileName(), o1, o2); 
   compareByteArray(o1, o2);
    
    PdxInstance pdx2 = JSONFormatter.fromJSON(jsonByteArray);
    boolean pdxequals = pdx.equals(pdx2);
    
    junit.framework.Assert.assertEquals("Pdx are not equal for byte array ; json filename " + jd.getFileName(), pdx, pdx2 );    
  }  
  public void compareByteArray(byte[] b1, byte[] b2) {
    if(b1.length != b2.length) 
      throw new IllegalStateException("Json byte array length are not equal " + b1.length + " ; " + b2.length);
    
    for(int i =0; i< b1.length; i++) {
      if(b1[i] != b2[i])
        throw new IllegalStateException("Json byte arrays are not equal ");
    }
  } 
  public byte[] jsonParse(byte[] jsonBA) {
    
    byte[] ba = jsonBA;
    byte [] withoutspace = new byte[ba.length];
  
    int i = 0;
    int j = 0;
    for(i =0; i< ba.length; i++) {
      int cbyte = ba[i];
      
      if(cbyte == INT_TAB || cbyte == INT_LF || cbyte == INT_CR || cbyte == INT_SPACE)
        continue;
      withoutspace[j++] = ba[i];
    }
      
     byte[] retBA = new byte[j];
     
     for(i =0 ; i< j ; i++) {
       retBA[i] = withoutspace[i];
     }
      
      return retBA;
    
  }
  public static JSONData[] loadAllJSON(String jsondir) {
    File dir = new File(jsondir);
    
    JSONData [] JSONDatas = new JSONData[dir.list().length];
    int i = 0;
    for(String jsonFileName : dir.list()) {
      
      if(!jsonFileName.contains(".txt"))
        continue;
      try {
        byte[] ba = getBytesFromFile(dir.getAbsolutePath() +  File.separator + jsonFileName);
        JSONDatas[i++] = new JSONData(jsonFileName, ba);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }       
    return JSONDatas;
  }
  
  public static byte[] getBytesFromFile(String fileName) throws IOException {
    File file = new File(fileName);
    
    java.io.InputStream is = new FileInputStream(file);

     // Get the size of the file
     long length = file.length();

     // Create the byte array to hold the data
     byte[] bytes = new byte[(int)length];

     // Read in the bytes
     int offset = 0;
     int numRead = 0;
     while (offset < bytes.length
            && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
         offset += numRead;
     }

     // Ensure all the bytes have been read in
     if (offset < bytes.length) {
         throw new IOException("Could not completely read file "+file.getName());
     }
    
     is.close();
     return bytes;
  }
  
  private void closeCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });
  }
  
  
  private int createServerRegion(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        //af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PARTITION);
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createServerRegion(VM vm, final boolean isPdxReadSerialized) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        //af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PARTITION);
        createRootRegion("testSimplePdx", af.create());
        
        ((GemFireCacheImpl)getCache()).getCacheConfig().setPdxReadSerialized(isPdxReadSerialized);
        
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createServerRegionWithPersistence(VM vm,
      final boolean persistentPdxRegistry) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory();
        if(persistentPdxRegistry) {
          cf.setPdxPersistent(true)
          .setPdxDiskStore("store");
        }
//      
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs())
          .create("store");
        
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store");
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createServerAccessor(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        //af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.EMPTY);
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createLonerServerRegion(VM vm, final String regionName, final String dsId) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty("locators", "");
        props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, dsId);
        getSystem(props);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion(regionName, af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private void createClientRegion(final VM vm, final int port) {
    createClientRegion(vm, port, false);
  }

  private void createClientRegion(final VM vm, final int port, 
      final boolean threadLocalConnections) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        cf.setPoolThreadLocalConnections(threadLocalConnections);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
  
  private void createClientRegion(final VM vm, final int port, 
      final boolean threadLocalConnections, final boolean isPdxReadSerialized) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        cf.setPoolThreadLocalConnections(threadLocalConnections);
        cf.setPdxReadSerialized(isPdxReadSerialized);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
  
  
}


