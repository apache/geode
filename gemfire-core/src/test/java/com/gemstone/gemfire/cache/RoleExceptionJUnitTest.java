/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import java.io.*;
import java.util.*;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.distributed.internal.membership.InternalRole;
import com.gemstone.junit.UnitTest;

/** Tests the subclasses of RoleException to make sure they are Serializable */
@Category(UnitTest.class)
public class RoleExceptionJUnitTest extends TestCase {
  
  public RoleExceptionJUnitTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Assert that RegionAccessException is serializable.
   */
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

