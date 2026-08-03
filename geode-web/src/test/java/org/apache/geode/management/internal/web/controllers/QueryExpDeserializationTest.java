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
package org.apache.geode.management.internal.web.controllers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import javax.management.Query;
import javax.management.QueryExp;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.junit.Test;

public class QueryExpDeserializationTest {
  @Test
  public void testQueryExp() throws Exception {
    QueryExp query = Query.eq(Query.attr("Name"), Query.value("mock"));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(query);
    oos.close();

    byte[] decoded = baos.toByteArray();
    ValidatingObjectInputStream ois =
        new ValidatingObjectInputStream(new ByteArrayInputStream(decoded));
    ois.accept("javax.management.*", "java.lang.*", "java.util.*");

    QueryExp q = (QueryExp) ois.readObject();
    System.out.println(q);
  }
}
