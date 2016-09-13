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
package org.apache.geode.cache;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OperationJUnitTest {

  /**
   * Check CREATE Operation.
   */
  @Test
  public void testCREATE() {
    Operation op = Operation.CREATE;
    assertTrue(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check PUTALL_CREATE Operation.
   */
  @Test
  public void testPUTALL_CREATE() {
    Operation op = Operation.PUTALL_CREATE;
    assertTrue(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertTrue(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check SEARCH_CREATE Operation.
   */
  @Test
  public void testSEARCH_CREATE() {
    Operation op = Operation.SEARCH_CREATE;
    assertTrue(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertTrue(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check LOCAL_LOAD_CREATE Operation.
   */
  @Test
  public void testLOCAL_LOAD_CREATE() {
    Operation op = Operation.LOCAL_LOAD_CREATE;
    assertTrue(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertTrue(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertTrue(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check NET_LOAD_CREATE Operation.
   */
  @Test
  public void testNET_LOAD_CREATE() {
    Operation op = Operation.NET_LOAD_CREATE;
    assertTrue(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertTrue(op.isNetLoad());
    assertTrue(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check UPDATE Operation.
   */
  @Test
  public void testUPDATE() {
    Operation op = Operation.UPDATE;
    assertFalse(op.isCreate());
    assertTrue(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check PUTALL_UPDATE Operation.
   */
  @Test
  public void testPUTALL_UPDATE() {
    Operation op = Operation.PUTALL_UPDATE;
    assertFalse(op.isCreate());
    assertTrue(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertTrue(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check SEARCH_UPDATE Operation.
   */
  @Test
  public void testSEARCH_UPDATE() {
    Operation op = Operation.SEARCH_UPDATE;
    assertFalse(op.isCreate());
    assertTrue(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertTrue(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check LOCAL_LOAD_UPDATE Operation.
   */
  @Test
  public void testLOCAL_LOAD_UPDATE() {
    Operation op = Operation.LOCAL_LOAD_UPDATE;
    assertFalse(op.isCreate());
    assertTrue(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertTrue(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertTrue(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check NET_LOAD_UPDATE Operation.
   */
  @Test
  public void testNET_LOAD_UPDATE() {
    Operation op = Operation.NET_LOAD_UPDATE;
    assertFalse(op.isCreate());
    assertTrue(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertTrue(op.isNetLoad());
    assertTrue(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check INVALIDATE Operation.
   */
  @Test
  public void testINVALIDATE() {
    Operation op = Operation.INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertTrue(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check LOCAL_INVALIDATE Operation.
   */
  @Test
  public void testLOCAL_INVALIDATE() {
    Operation op = Operation.LOCAL_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertTrue(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check DESTROY Operation.
   */
  @Test
  public void testDESTROY() {
    Operation op = Operation.DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertTrue(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REMOVEALL Operation.
   */
  @Test
  public void testREMOVEALL() {
    Operation op = Operation.REMOVEALL_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertTrue(op.isDestroy());
    assertTrue(op.isRemoveAll());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check LOCAL_DESTROY Operation.
   */
  @Test
  public void testLOCAL_DESTROY() {
    Operation op = Operation.LOCAL_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertTrue(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check EVICT_DESTROY Operation.
   */
  @Test
  public void testEVICT_DESTROY() {
    Operation op = Operation.EVICT_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertTrue(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_LOAD_SNAPSHOT Operation.
   */
  @Test
  public void testREGION_LOAD_SNAPSHOT() {
    Operation op = Operation.REGION_LOAD_SNAPSHOT;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_LOCAL_DESTROY Operation.
   */
  @Test
  public void testREGION_LOCAL_DESTROY() {
    Operation op = Operation.REGION_LOCAL_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_CREATE Operation.
   */
  @Test
  public void testREGION_CREATE() {
    Operation op = Operation.REGION_CREATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_CLOSE Operation.
   */
  @Test
  public void testREGION_CLOSE() {
    Operation op = Operation.REGION_CLOSE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertTrue(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_DESTROY Operation.
   */
  @Test
  public void testREGION_DESTROY() {
    Operation op = Operation.REGION_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check EXPIRE_DESTROY Operation.
   */
  @Test
  public void testEXPIRE_DESTROY() {
    Operation op = Operation.EXPIRE_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertTrue(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check EXPIRE_LOCAL_DESTROY Operation.
   */
  @Test
  public void testEXPIRE_LOCAL_DESTROY() {
    Operation op = Operation.EXPIRE_LOCAL_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertTrue(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertTrue(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check EXPIRE_INVALIDATE Operation.
   */
  @Test
  public void testEXPIRE_INVALIDATE() {
    Operation op = Operation.EXPIRE_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertTrue(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check EXPIRE_LOCAL_INVALIDATE Operation.
   */
  @Test
  public void testEXPIRE_LOCAL_INVALIDATE() {
    Operation op = Operation.EXPIRE_LOCAL_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertTrue(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertTrue(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_EXPIRE_DESTROY Operation.
   */
  @Test
  public void testREGION_EXPIRE_DESTROY() {
    Operation op = Operation.REGION_EXPIRE_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertFalse(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_EXPIRE_LOCAL_DESTROY Operation.
   */
  @Test
  public void testREGION_EXPIRE_LOCAL_DESTROY() {
    Operation op = Operation.REGION_EXPIRE_LOCAL_DESTROY;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_EXPIRE_INVALIDATE Operation.
   */
  @Test
  public void testREGION_EXPIRE_INVALIDATE() {
    Operation op = Operation.REGION_EXPIRE_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertTrue(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertFalse(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_EXPIRE_LOCAL_INVALIDATE Operation.
   */
  @Test
  public void testREGION_EXPIRE_LOCAL_INVALIDATE() {
    Operation op = Operation.REGION_EXPIRE_LOCAL_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertTrue(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertTrue(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_LOCAL_INVALIDATE Operation.
   */
  @Test
  public void testREGION_LOCAL_INVALIDATE() {
    Operation op = Operation.REGION_LOCAL_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertTrue(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_INVALIDATE Operation.
   */
  @Test
  public void testREGION_INVALIDATE() {
    Operation op = Operation.REGION_INVALIDATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertTrue(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isClose());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_CLEAR Operation.
   */
  @Test
  public void testREGION_CLEAR() {
    Operation op = Operation.REGION_CLEAR;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertTrue(op.isClear());
  }

  /**
   * Check REGION_LOCAL_CLEAR Operation.
   */
  @Test
  public void testREGION_LOCAL_CLEAR() {
    Operation op = Operation.REGION_LOCAL_CLEAR;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertTrue(op.isClear());
  }

  /**
   * Check CACHE_CREATE Operation
   */
  @Test
  public void testCACHE_CREATE() {
    Operation op = Operation.CACHE_CREATE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check CACHE_CLOSE Operation.
   */
  @Test
  public void testCACHE_CLOSE() {
    Operation op = Operation.CACHE_CLOSE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertTrue(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check REGION_REINITIALIZE Operation.
   */
  @Test
  public void testREGION_REINITIALIZE() {
    Operation op = Operation.REGION_REINITIALIZE;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertTrue(op.isRegionDestroy());
    assertTrue(op.isRegion());
    assertTrue(op.isLocal());
    assertFalse(op.isDistributed());
    assertFalse(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }

  /**
   * Check UPDATE_VERSION Operation.
   */
  @Test
  public void testUPDATE_VERSION() {
    Operation op = Operation.UPDATE_VERSION_STAMP;
    assertFalse(op.isCreate());
    assertFalse(op.isUpdate());
    assertFalse(op.isInvalidate());
    assertFalse(op.isDestroy());
    assertFalse(op.isPutAll());
    assertFalse(op.isRegionInvalidate());
    assertFalse(op.isRegionDestroy());
    assertFalse(op.isRegion());
    assertFalse(op.isLocal());
    assertTrue(op.isDistributed());
    assertTrue(op.isEntry());
    assertFalse(op.isExpiration());
    assertFalse(op.isLocalLoad());
    assertFalse(op.isNetLoad());
    assertFalse(op.isLoad());
    assertFalse(op.isNetSearch());
    assertFalse(op.isClose());
    assertFalse(op.isClear());
  }
}
