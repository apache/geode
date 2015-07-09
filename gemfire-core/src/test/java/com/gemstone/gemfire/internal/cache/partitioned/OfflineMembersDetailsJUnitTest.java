/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class OfflineMembersDetailsJUnitTest extends TestCase {
  
  public void testSerialization() throws Exception {
    Set<PersistentMemberID>[] offlineMembers = new Set[5];
    for(int i =0; i < offlineMembers.length; i++) {
      offlineMembers[i] = new HashSet<PersistentMemberID>();
      offlineMembers[i].add(new PersistentMemberID(DiskStoreID.random(), InetAddress.getLocalHost(), "a", System.currentTimeMillis(),(short)0));
    }
    OfflineMemberDetailsImpl details = new OfflineMemberDetailsImpl(offlineMembers );
    
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(boas);
    details.toData(out);
    
    OfflineMemberDetailsImpl details2 = new OfflineMemberDetailsImpl();
    details2.fromData(new DataInputStream(new ByteArrayInputStream(boas.toByteArray())));
  }

}
