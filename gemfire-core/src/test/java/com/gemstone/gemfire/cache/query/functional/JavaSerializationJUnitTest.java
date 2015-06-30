/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.functional;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JavaSerializationJUnitTest extends TestCase {
  
  public JavaSerializationJUnitTest(String testName) {
    super(testName);
  }
  
  public void testStructImplSerialization() throws Exception {
    String[] fieldNames = {"col1", "col2"};
    ObjectType[] fieldTypes = {new ObjectTypeImpl(Integer.class), new ObjectTypeImpl(String.class)};
    StructTypeImpl type = new StructTypeImpl(fieldNames, fieldTypes);
    Object[] values = {new Integer(123), new String("456")};
    StructImpl si = new StructImpl(type, values);
    verifyJavaSerialization(si);   
  }
  
  public void testUndefinedSerialization() throws Exception {
    verifyJavaSerialization(QueryService.UNDEFINED);   
  }
  
  private void verifyJavaSerialization(Object obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(obj);
    out.close();
    baos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bais);
    Object obj2 = in.readObject();
    in.close();
    bais.close();
    assert (obj.equals(obj2));
  }
}
