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
package com.gemstone.gemfire.cache.operations.internal;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.operations.PutOperationContextJUnitTest;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

@Category(IntegrationTest.class)
public class GetOperationContextImplJUnitTest {

  @Test
  public void testGetSerializedValue() throws IOException {
    {
      byte[] byteArrayValue = new byte[]{1,2,3,4};
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertNull("value is an actual byte array which is not a serialized blob", poc.getSerializedValue());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is null which is not a serialized blob", poc.getSerializedValue());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is a String which is not a serialized blob", poc.getSerializedValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, poc.getSerializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "")
          .set(MCAST_PORT, "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        Assert.assertArrayEquals(blob, poc.getSerializedValue());
      } finally {
        c.close();
      }
    }
  }

  @Test
  public void testGetDeserializedValue() throws IOException {
    {
      byte[] byteArrayValue = new byte[]{1,2,3,4};
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getDeserializedValue());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getDeserializedValue());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        PdxInstance pi = (PdxInstance) poc.getDeserializedValue();
        Assert.assertEquals("value", pi.getField("v"));
      } finally {
        c.close();
      }
    }
  }

  @Test
  public void testGetValue() throws IOException {
    {
      byte[] byteArrayValue = new byte[]{1,2,3,4};
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getValue());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getValue());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
      } finally {
        c.close();
      }
    }
  }

  @Test
  public void testGetObject() throws IOException {
    {
      byte[] byteArrayValue = new byte[]{1,2,3,4};
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getObject());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getObject());
    }

    {
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getObject());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is a serialized blob which is not an Object", poc.getObject());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        GetOperationContextImpl poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        Assert.assertNull("value is a serialized blob which is not an Object", poc.getObject());
      } finally {
        c.close();
      }
    }
  }
}
