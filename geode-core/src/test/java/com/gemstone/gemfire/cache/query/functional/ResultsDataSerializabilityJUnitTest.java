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
/*
 * ResultsDataSerializabilityJUnitTest.java
 * JUnit based test
 *
 * Created on March 8, 2007
 */

package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.ResultsCollectionWrapper;
import com.gemstone.gemfire.cache.query.internal.ResultsSet;
import com.gemstone.gemfire.cache.query.internal.SortedResultSet;
import com.gemstone.gemfire.cache.query.internal.SortedStructSet;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.internal.StructSet;
import com.gemstone.gemfire.cache.query.internal.Undefined;
import com.gemstone.gemfire.cache.query.internal.types.CollectionTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.MapTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test whether query results are DataSerializable
 *
 */
@Category(IntegrationTest.class)
public class ResultsDataSerializabilityJUnitTest {
  
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }
  
  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  /* In the absence of some kind of hook into the DataSerializer,
   * just test to see if the known implementation classes that are part of query
   * results implement the  DataSerializable interface
   */
  @Test
  public void testImplementsDataSerializable() throws Exception {
    Class[] classes = new Class[] {
      SortedResultSet.class,
      ResultsCollectionWrapper.class,
      ResultsSet.class,
      SortedStructSet.class,
      StructImpl.class,
      StructSet.class,
      Undefined.class,
//      QRegion.class, // QRegions remain unserializable
      CollectionTypeImpl.class,
      MapTypeImpl.class,
      ObjectTypeImpl.class,
      StructTypeImpl.class,
    };
    
    List list = new ArrayList();
    for (int i = 0; i < classes.length; i++) {
      Class nextClass = classes[i];
      if (!DataSerializable.class.isAssignableFrom(nextClass)) {
        if (!DataSerializableFixedID.class.isAssignableFrom(nextClass)) {
          list.add(nextClass.getName());
        }
      }
    }
    
    assertTrue(list + " are not DataSerializable",
               list.isEmpty());
  }
  
  // For locally executed queries, ResultsCollectionPdxDeserializerWrapper is
  // returned which is not required to be DSFID as it is not sent over the
  // network. Hence a dunit test is required for testing this functionality.
  
  /* test DataSerializability of a simple query result */
  @Ignore
  @Test
  public void testDataSerializability() throws Exception {
              
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for(int i = 0; i < 10000; i++) {
      region.put(i+"",new Portfolio(i));
    }
    
    String queryStr = "SELECT DISTINCT * FROM /Portfolios";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    
    SelectResults res1 = (SelectResults)q.execute();
        
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    DataSerializer.writeObject(res1, out, false); // false prevents Java serialization
    out.close();
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));    
    SelectResults res2 = (SelectResults)DataSerializer.readObject(in);
    in.close();
    
    assertEquals(res2.size(), res1.size());
  }
  
}
