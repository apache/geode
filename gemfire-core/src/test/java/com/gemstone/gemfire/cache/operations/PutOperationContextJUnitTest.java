package com.gemstone.gemfire.cache.operations;

import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.matchers.JUnitMatchers;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PutOperationContextJUnitTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetSerializedValue() throws IOException {
    {
      byte[] byteArrayValue = new byte[]{1,2,3,4};
      PutOperationContext poc = new PutOperationContext("key", byteArrayValue, false, PutOperationContext.CREATE, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertNull("value is an actual byte array which is not a serialized blob", poc.getSerializedValue());
    }

    {
      PutOperationContext poc = new PutOperationContext("key", null, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is null which is not a serialized blob", poc.getSerializedValue());
    }

    {
      PutOperationContext  poc = new PutOperationContext("key", "value", true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is a String which is not a serialized blob", poc.getSerializedValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      PutOperationContext poc = new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, poc.getSerializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set("locators", "").set("mcast-port", "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        PutOperationContext poc = new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
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
      PutOperationContext poc = new PutOperationContext("key", byteArrayValue, false, PutOperationContext.CREATE, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getDeserializedValue());
    }

    {
      PutOperationContext poc = new PutOperationContext("key", null, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getDeserializedValue());
    }

    {
      PutOperationContext  poc = new PutOperationContext("key", "value", true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      PutOperationContext poc = new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set("locators", "").set("mcast-port", "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        PutOperationContext poc = new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
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
      PutOperationContext poc = new PutOperationContext("key", byteArrayValue, false, PutOperationContext.CREATE, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getValue());
    }

    {
      PutOperationContext poc = new PutOperationContext("key", null, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getValue());
    }

    {
      PutOperationContext  poc = new PutOperationContext("key", "value", true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getValue());
    }

    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      byte[] blob = baos.toByteArray();
      PutOperationContext poc = new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      Cache c = (new CacheFactory()).set("locators", "").set("mcast-port", "0").setPdxReadSerialized(true).create();
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PdxValue("value"), dos);
        dos.close();
        byte[] blob = baos.toByteArray();
        PutOperationContext poc = new PutOperationContext("key", blob, true, PutOperationContext.CREATE, false);
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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PdxValue other = (PdxValue) obj;
      if (v == null) {
        if (other.v != null)
          return false;
      } else if (!v.equals(other.v))
        return false;
      return true;
    }

    private String v;
    
    public PdxValue() {
    }
    
    public PdxValue(String v) {
      this.v = v;
    }
    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("v", this.v);
    }

    @Override
    public void fromData(PdxReader reader) {
      this.v = reader.readString("v");
    }
    
  }
}
