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
package org.apache.geode.cache.operations;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class PutOperationContextJUnitTest {

  @Test
  public void testGetSerializedValue() throws IOException {
    {
      byte[] byteArrayValue = new byte[] {1, 2, 3, 4};
      PutOperationContext poc =
          new PutOperationContext("key", byteArrayValue, false, PutOperationContext.CREATE, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertNull("value is an actual byte array which is not a serialized blob",
          poc.getSerializedValue());
    }

    {
      PutOperationContext poc =
          new PutOperationContext("key", null, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is null which is not a serialized blob", poc.getSerializedValue());
    }

    {
      PutOperationContext poc =
          new PutOperationContext("key", "value", true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is a String which is not a serialized blob",
          poc.getSerializedValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      PutOperationContext poc =
          new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, poc.getSerializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        PutOperationContext poc =
            new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
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
      byte[] byteArrayValue = new byte[] {1, 2, 3, 4};
      PutOperationContext poc =
          new PutOperationContext("key", byteArrayValue, false, PutOperationContext.CREATE, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getDeserializedValue());
    }

    {
      PutOperationContext poc =
          new PutOperationContext("key", null, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getDeserializedValue());
    }

    {
      PutOperationContext poc =
          new PutOperationContext("key", "value", true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      PutOperationContext poc =
          new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        PutOperationContext poc =
            new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
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
      byte[] byteArrayValue = new byte[] {1, 2, 3, 4};
      PutOperationContext poc =
          new PutOperationContext("key", byteArrayValue, false, PutOperationContext.CREATE, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getValue());
    }

    {
      PutOperationContext poc =
          new PutOperationContext("key", null, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getValue());
    }

    {
      PutOperationContext poc =
          new PutOperationContext("key", "value", true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      PutOperationContext poc =
          new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        PutOperationContext poc =
            new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
        Assert.assertTrue(poc.isObject());
        Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
      } finally {
        c.close();
      }
    }
  }

  public static class PdxValue implements PdxSerializable {
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((v == null) ? 0 : v.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      PdxValue other = (PdxValue) obj;
      if (v == null) {
        return other.v == null;
      } else
        return v.equals(other.v);
    }

    private String v;

    public PdxValue() {}

    public PdxValue(String v) {
      this.v = v;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("v", v);
    }

    @Override
    public void fromData(PdxReader reader) {
      v = reader.readString("v");
    }

  }
}
