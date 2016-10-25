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
package org.apache.geode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import com.gemstone.gemfire.OldClientSupportProvider;

@Category(DistributedTest.class)
@SuppressWarnings("deprecation")
public class OldClientSupportDUnitTest extends JUnit4CacheTestCase {

  static private final List<String> allGeodeThrowableClasses = Arrays.asList(new String[] {
    "org.apache.geode.cache.execute.EmptyRegionFunctionException",
  });

  private Cache myCache;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    myCache = getCache();
  }

  @Test
  public void cacheInstallsOldClientSupportServiceProvider() throws Exception {
    Assert.assertNotNull(((InternalCache)myCache).getService(OldClientSupportService.class));
  }

  /**
   * This test can be vastly simplified if clients have the ability to translate org.apache.geode package
   * prefixes to com.gemstone.gemfire.  In that case we will only need to test translation of
   * EmtpyRegionFunctionException.
   */
  @Test
  public void testConversionOfThrowablesForOldClients() throws Exception {
    List<Throwable> problems = new LinkedList<>();
    
    for (String geodeClassName: allGeodeThrowableClasses) {
      try {
        convertThrowableForOldClient(geodeClassName);
      } catch (Exception e) {
        System.out.println("-- failed");
        Exception failure = new Exception("Failed processing " + geodeClassName + ": " + e.toString());
        failure.initCause(e);
        problems.add(failure);
      }
    }
    
    if (!problems.isEmpty()) {
      Assert.fail(problems.toString());
    }
  }
  
  private void convertThrowableForOldClient(String geodeClassName) throws Exception {
    Version oldClientVersion = Version.GFE_82;
    final String comGemstoneGemFire = "com.gemstone.gemfire";
    final int comGemstoneGemFireLength = comGemstoneGemFire.length();

    OldClientSupportService oldClientSupport = OldClientSupportProvider.getService(myCache);

    System.out.println("checking " + geodeClassName);
    Class geodeClass = Class.forName(geodeClassName);
    Object geodeObject = instantiate(geodeClass);
    if (geodeObject instanceof Throwable) {
      Throwable geodeThrowable = (Throwable)instantiate(geodeClass);
      Throwable gemfireThrowable = oldClientSupport.getThrowable(geodeThrowable, oldClientVersion);
      Assert.assertEquals("Failed to convert " + geodeClassName + ". Throwable class is " + gemfireThrowable.getClass().getName()
        , comGemstoneGemFire, gemfireThrowable.getClass().getName().substring(0, comGemstoneGemFireLength));
    }
  }
  
  private Object instantiate(Class aClass) throws Exception {
    Constructor c = null;
    try {
      c = aClass.getConstructor();
      return c.newInstance();
    } catch (NoSuchMethodException e1) {
      try {
        c = aClass.getConstructor(String.class);
        return c.newInstance("test instance");
      } catch (NoSuchMethodException e2) {
        try {
          c = aClass.getConstructor(String.class, Throwable.class);
          return c.newInstance("test instance", null);
        } catch (NoSuchMethodException e3) {
          throw new RuntimeException("unable to find an instantiator for " + aClass.getName());
        }
      }
    }
  }

  /**
   * com.gemstone.gemfire objects received from a client should translate to
   * org.apache.geode objects.  Here we perform a simple unit test on a
   * com.gemstone.gemfire object to ensure that this is happening correctly
   * for Java-serialized objects
   */
  @Test
  public void oldClientObjectTranslatesToGeodeObject_javaSerialization() throws Exception {
    com.gemstone.gemfire.ClientSerializableObject gemfireObject = new com.gemstone.gemfire.ClientSerializableObject();
    com.gemstone.gemfire.ClientSerializableObject subObject = new com.gemstone.gemfire.ClientSerializableObject();
    gemfireObject.setSubObject(subObject);
    
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(500);
    DataOutputStream dataOut = new DataOutputStream(byteStream);
    DataSerializer.writeObject(gemfireObject, dataOut);
    dataOut.flush();
    byte[] serializedForm = byteStream.toByteArray();

    ByteArrayDataInput byteDataInput = new ByteArrayDataInput();
    byteDataInput.initialize(serializedForm, Version.GFE_82);
    ClientSerializableObject result = DataSerializer.readObject(byteDataInput);
    Assert.assertEquals("Expected an org.apache.geode exception but found " + result,
      result.getClass().getName().substring(0, "org.apache.geode".length()), "org.apache.geode");
    ClientSerializableObject newSubObject = result.getSubObject();
    Assert.assertNotNull(newSubObject);
  }


  /**
   * com.gemstone.gemfire objects received from a client should translate to
   * org.apache.geode objects.  Here we perform a simple unit test on a
   * com.gemstone.gemfire object to ensure that this is happening correctly
   * for data-serialized objects
   */
  @Test
  public void oldClientObjectTranslatesToGeodeObject_dataSerialization() throws Exception {
    com.gemstone.gemfire.ClientDataSerializableObject gemfireObject = new com.gemstone.gemfire.ClientDataSerializableObject();

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(500);
    DataOutputStream dataOut = new DataOutputStream(byteStream);
    // use an internal API to ensure that java serialization isn't used
    InternalDataSerializer.writeObject(gemfireObject, dataOut, false);
    dataOut.flush();
    byte[] serializedForm = byteStream.toByteArray();

    ByteArrayDataInput byteDataInput = new ByteArrayDataInput();
    byteDataInput.initialize(serializedForm, Version.GFE_82);
    Object result = DataSerializer.readObject(byteDataInput);
    Assert.assertEquals("Expected an org.apache.geode object but found " + result,
      result.getClass().getName().substring(0, "org.apache.geode".length()), "org.apache.geode");
  }

  /**
   * com.gemstone.gemfire objects received from a client should translate to
   * org.apache.geode objects.  Here we perform a simple unit test on a
   * com.gemstone.gemfire object to ensure that this is happening correctly
   * for PDX-serialized objects
   */
  @Test
  public void oldClientObjectTranslatesToGeodeObject_pdxSerialization() throws Exception {
    com.gemstone.gemfire.ClientPDXSerializableObject gemfireObject = new com.gemstone.gemfire.ClientPDXSerializableObject();

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(500);
    DataOutputStream dataOut = new DataOutputStream(byteStream);
    // use an internal API to ensure that java serialization isn't used
    InternalDataSerializer.writeObject(gemfireObject, dataOut, false);
    dataOut.flush();
    byte[] serializedForm = byteStream.toByteArray();

    ByteArrayDataInput byteDataInput = new ByteArrayDataInput();
    byteDataInput.initialize(serializedForm, Version.GFE_82);
    Object result = DataSerializer.readObject(byteDataInput);
    Assert.assertEquals("Expected an org.apache.geode object but found " + result,
      result.getClass().getName().substring(0, "org.apache.geode".length()), "org.apache.geode");
  }
}
