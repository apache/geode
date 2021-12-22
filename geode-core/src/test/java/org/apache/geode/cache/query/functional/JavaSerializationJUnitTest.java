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
package org.apache.geode.cache.query.functional;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;

public class JavaSerializationJUnitTest {

  @Test
  public void testStructImplSerialization() throws Exception {
    String[] fieldNames = {"col1", "col2"};
    ObjectType[] fieldTypes = {new ObjectTypeImpl(Integer.class), new ObjectTypeImpl(String.class)};
    StructTypeImpl type = new StructTypeImpl(fieldNames, fieldTypes);
    Object[] values = {123, "456"};
    StructImpl si = new StructImpl(type, values);
    verifyJavaSerialization(si);
  }

  @Test
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
