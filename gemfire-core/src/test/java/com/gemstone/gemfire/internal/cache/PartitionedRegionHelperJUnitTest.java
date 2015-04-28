/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class PartitionedRegionHelperJUnitTest extends TestCase {
  
  public void testEscapeUnescape() {
    {
      String bucketName = PartitionedRegionHelper.getBucketName("/root/region", 5);
      assertEquals("Name = " + bucketName, -1, bucketName.indexOf('/'));
      assertEquals("/root/region" , PartitionedRegionHelper.getPRPath(bucketName));
    }

    {
      String bucketName = PartitionedRegionHelper.getBucketName("/root/region_one", 5);
      assertEquals("Name = " + bucketName, -1, bucketName.indexOf('/'));
      assertEquals("/root/region_one" , PartitionedRegionHelper.getPRPath(bucketName));
    }
  }
  
  

}
