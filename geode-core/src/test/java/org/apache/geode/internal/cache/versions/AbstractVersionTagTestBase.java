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
package com.gemstone.gemfire.internal.cache.versions;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public abstract class AbstractVersionTagTestBase {
  @SuppressWarnings("rawtypes")
  protected abstract VersionTag createVersionTag();
  
  @SuppressWarnings("rawtypes")
  private VersionTag vt;
  
  @Before
  public void setup() {
    this.vt = createVersionTag();
  }
  @Test
  public void testFromOtherMemberBit() {
    assertEquals(false, vt.isFromOtherMember());
    vt.setIsRemoteForTesting();
    assertEquals(true, vt.isFromOtherMember());
  }
  
  @Test
  public void testTimeStampUpdatedBit() {
    assertEquals(false, vt.isTimeStampUpdated());
    vt.setTimeStampApplied(true);
    assertEquals(true, vt.isTimeStampUpdated());
    vt.setTimeStampApplied(false);
    assertEquals(false, vt.isTimeStampUpdated());
  }
  
  @Test
  public void testGatewayTagBit() {
    assertEquals(false, vt.isGatewayTag());
    vt.setIsGatewayTag(true);
    assertEquals(true, vt.isGatewayTag());
    vt.setIsGatewayTag(false);
    assertEquals(false, vt.isGatewayTag());
  }
  
  @Test
  public void testRecordedBit() {
    assertEquals(false, vt.isRecorded());
    vt.setRecorded();
    assertEquals(true, vt.isRecorded());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testPreviousMemberIDBit() {
    assertEquals(false, vt.hasPreviousMemberID());
    vt.setPreviousMemberID(null);
    assertEquals(true, vt.hasPreviousMemberID());
  }
  
  @Test
  public void testPosDupBit() {
    assertEquals(false, vt.isPosDup());
    vt.setPosDup(true);
    assertEquals(true, vt.isPosDup());
    vt.setPosDup(false);
    assertEquals(false, vt.isPosDup());
  }
  
  @Test
  public void testAllowedByResolverBit() {
    assertEquals(false, vt.isAllowedByResolver());
    vt.setAllowedByResolver(true);
    assertEquals(true, vt.isAllowedByResolver());
    vt.setAllowedByResolver(false);
    assertEquals(false, vt.isAllowedByResolver());
  }
}
