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
package com.gemstone.gemfire.cache;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.membership.InternalRole;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests the subclasses of RoleException to make sure they are Serializable
 */
@Category(UnitTest.class)
public class RoleExceptionJUnitTest {
  
  /**
   * Assert that RegionAccessException is serializable.
   */
  @Test
  public void testRegionAccessExceptionIsSerializable() throws Exception {
    RegionAccessException out = createRegionAccessException();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(out);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    RegionAccessException in = (RegionAccessException) ois.readObject();
    assertEquals(createSetOfRoles(), in.getMissingRoles());
    
    assertEquals(out.getMessage(), in.getMessage());
    assertEquals(out.getRegionFullPath(), in.getRegionFullPath());
  }

  /**
   * Assert that RegionDistributionException is serializable.
   */
  @Test
  public void testRegionDistributionExceptionIsSerializable() throws Exception {
    RegionDistributionException out = createRegionDistributionException();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(out);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    RegionDistributionException in = (RegionDistributionException) ois.readObject();
    assertEquals(createSetOfRoles(), in.getFailedRoles());

    assertEquals(out.getMessage(), in.getMessage());
    assertEquals(out.getRegionFullPath(), in.getRegionFullPath());
  }

  /**
   * Assert that CommitDistributionException is serializable.
   */
  @Test
  public void testCommitDistributionExceptionIsSerializable() throws Exception {
    String s = "MyString";
    Set outExceptions = new HashSet();
    outExceptions.add(createRegionDistributionException());
    
    CommitDistributionException out = new CommitDistributionException(
        s, outExceptions);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(out);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    CommitDistributionException in = (CommitDistributionException) ois.readObject();
    
    Set inExceptions = in.getRegionDistributionExceptions();
    assertNotNull(inExceptions);
    Iterator iter = inExceptions.iterator();
    assertTrue(iter.hasNext());
    RegionDistributionException e = (RegionDistributionException) iter.next();
    assertEquals(createSetOfRoles(), e.getFailedRoles());

    assertEquals(out.getMessage(), in.getMessage());
  }

  private Set createSetOfRoles() {
    Set set = new HashSet();
    set.add(InternalRole.getRole("RoleA"));
    set.add(InternalRole.getRole("RoleB"));
    set.add(InternalRole.getRole("RoleC"));
    set.add(InternalRole.getRole("RoleD"));
    return set;
  }
  
  private RegionAccessException createRegionAccessException() {
    String s = "MyString";
    String regionFullPath = "MyPath"; 
    Set missingRoles = createSetOfRoles();
    return new RegionAccessException(s, regionFullPath, missingRoles);
  }
  
  private RegionDistributionException createRegionDistributionException() {
    String s = "MyString";
    String regionFullPath = "MyPath"; 
    Set missingRoles = createSetOfRoles();
    return new RegionDistributionException(s, regionFullPath, missingRoles);
  }
  
}

