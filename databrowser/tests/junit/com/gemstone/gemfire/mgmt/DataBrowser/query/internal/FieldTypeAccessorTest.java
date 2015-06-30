package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import junit.framework.TestCase;

public class FieldTypeAccessorTest extends TestCase {
  
  private FieldTypeAccessor fieldTypeAccessor;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    
    if (PdxHelper.getInstance().isPdxSupported()) {
      Class<?> fieldTypeClass = null;
      try {
        fieldTypeClass = Class.forName("com.gemstone.gemfire.pdx.FieldType");
      } catch (ClassNotFoundException e) {
        fieldTypeClass = null;
      }
      
      if (fieldTypeClass == null) {
        try {
          fieldTypeClass = Class.forName("com.gemstone.gemfire.pdx.internal.FieldType");
        } catch (ClassNotFoundException e) {
          fieldTypeClass = null;
        }
      }
      
      assertNotNull("PDX is supported i.e. GemFire version > 6.6. But can't find class FieldType. It's :"+fieldTypeClass, fieldTypeClass);
      
      fieldTypeAccessor = new FieldTypeAccessor(fieldTypeClass);
      
      System.out.println(fieldTypeAccessor);
    }
  }
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    fieldTypeAccessor = null;
  }

  public void testGetBoolean() {
    assertNotNull(fieldTypeAccessor.getBoolean());
  }

  public void testGetByte() {
    assertNotNull(fieldTypeAccessor.getByte());
  }

  public void testGetChar() {
    assertNotNull(fieldTypeAccessor.getChar());
  }

  public void testGetShort() {
    assertNotNull(fieldTypeAccessor.getBoolean());
  }

  public void testGetInt() {
    assertNotNull(fieldTypeAccessor.getInt());
  }

  public void testGetLong() {
    assertNotNull(fieldTypeAccessor.getLong());
  }

  public void testGetFloat() {
    assertNotNull(fieldTypeAccessor.getFloat());
  }

  public void testGetDouble() {
    assertNotNull(fieldTypeAccessor.getDouble());
  }

  public void testGetDate() {
    assertNotNull(fieldTypeAccessor.getDate());
  }

  public void testGetString() {
    assertNotNull(fieldTypeAccessor.getString());
  }

  public void testGetObject() {
    assertNotNull(fieldTypeAccessor.getObject());
  }

  public void testGetBooleanArray() {
    assertNotNull(fieldTypeAccessor.getBooleanArray());
  }

  public void testGetByteArray() {
    assertNotNull(fieldTypeAccessor.getByteArray());
  }

  public void testGetCharArray() {
    assertNotNull(fieldTypeAccessor.getCharArray());
  }

  public void testGetShortArray() {
    assertNotNull(fieldTypeAccessor.getShortArray());
  }

  public void testGetIntArray() {
    assertNotNull(fieldTypeAccessor.getIntArray());
  }

  public void testGetLongArray() {
    assertNotNull(fieldTypeAccessor.getLongArray());
  }

  public void testGetFloatArray() {
    assertNotNull(fieldTypeAccessor.getFloatArray());
  }

  public void testGetDoubleArray() {
    assertNotNull(fieldTypeAccessor.getDoubleArray());
  }

  public void testGetStringArray() {
    assertNotNull(fieldTypeAccessor.getStringArray());
  }

  public void testGetObjectArray() {
    assertNotNull(fieldTypeAccessor.getObjectArray());
  }

  public void testGetArrayOfByteArrays() {
    assertNotNull(fieldTypeAccessor.getArrayOfByteArrays());
  }
}
