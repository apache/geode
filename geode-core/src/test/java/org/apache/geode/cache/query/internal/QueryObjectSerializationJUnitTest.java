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
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.types.ObjectType;

/**
 * Tests the Serialization of the Query related class.
 *
 * @since GemFire 3.0
 */
public class QueryObjectSerializationJUnitTest implements Serializable {

  /** A <code>ByteArrayOutputStream</code> that data is serialized to */
  private transient ByteArrayOutputStream baos;

  /**
   * Creates a new <code>ByteArrayOutputStream</code> for this test to work with.
   */
  @Before
  public void setUp() {
    baos = new ByteArrayOutputStream();
  }

  @After
  public void tearDown() {
    baos = null;
  }

  /**
   * Returns a <code>DataOutput</code> to write to
   */
  private DataOutputStream getDataOutput() {
    return new DataOutputStream(baos);
  }

  /**
   * Returns a <code>DataInput</code> to read from
   */
  private DataInputStream getDataInput() {
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    return new DataInputStream(bais);
  }

  /**
   * Data serializes and then data de-serializes the given object and asserts that the two objects
   * satisfy o1.equals(o2)
   */
  private void checkRoundTrip(Object o1) throws IOException, ClassNotFoundException {
    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o1, out);
    out.flush();
    DataInput in = getDataInput();
    assertEquals(o1, DataSerializer.readObject(in));
    baos = new ByteArrayOutputStream();
  }

  /**
   * Tests the serialization of many, but not all of the possible ResultSets
   */
  @Test
  public void testSerializationOfQueryResults() throws IOException, ClassNotFoundException {
    Collection data = new java.util.ArrayList();
    data.add(null);
    data.add(null);
    data.add("some string");
    data.add(Long.MAX_VALUE);
    data.add(45);
    data.add(QueryService.UNDEFINED);

    ObjectType elementType = new SimpleObjectType();
    // Undefined
    checkRoundTrip(QueryService.UNDEFINED);
    // ResultsBag
    ResultsBag rbWithoutData = new ResultsBag();
    rbWithoutData.setElementType(elementType); // avoid NPE in equals
    checkRoundTrip(rbWithoutData);
    ResultsBag rbWithData = new ResultsBag(data, null);
    rbWithData.setElementType(elementType); // avoid NPE in equals
    checkRoundTrip(rbWithData);
    /*
     * Set rbWithoutDataAsSet = new ResultsBag().asSet(); ResultsCollectionWrapper rcw = new
     * ResultsCollectionWrapper(elementType, rbWithoutDataAsSet, -1); checkRoundTrip(rcw); Set
     * rbWithDataAsSet = new ResultsBag(data, (CachePerfStats)null).asSet();
     * ResultsCollectionWrapper rcwWithData = new ResultsCollectionWrapper(elementType,
     * rbWithDataAsSet, -1); checkRoundTrip(rcwWithData);
     */
    // SortedResultSet
    SortedResultSet srsWithoutData = new SortedResultSet();
    srsWithoutData.setElementType(elementType); // avoid NPE in equals
    checkRoundTrip(srsWithoutData);
    SortedResultSet srsWithData = new SortedResultSet();
    srsWithData.setElementType(elementType); // avoid NPE in equals
    checkRoundTrip(srsWithData);

    // SortedStructSet
    // SortedStructSet sssWithoutData = new SortedStructSet();
    // checkRoundTrip(sssWithoutData);
  }

  private static class SimpleObjectType implements ObjectType {
    public SimpleObjectType() {}

    @Override
    public boolean isCollectionType() {
      return false;
    }

    @Override
    public boolean isMapType() {
      return false;
    }

    @Override
    public boolean isStructType() {
      return false;
    }

    @Override
    public String getSimpleClassName() {
      return "java.lang.Object";
    }

    @Override
    public Class resolveClass() {
      return Object.class;
    }

    @Override
    public void toData(DataOutput out) {}

    @Override
    public void fromData(DataInput in) {}

    public boolean equals(Object o) {
      return o instanceof SimpleObjectType;
    }
  }
}
