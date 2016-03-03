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

package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.*;

/*
 * This test simply verifies the static class delegates properly to the impl
 * 
 * PowerMock used in this test to inject mocked impl into static class
 * The PowerMockRule bootstraps PowerMock without the need for 
 * the @RunWith(PowerMockRunner.class) annotation, which was interfering with jacoco
 * 
 */
@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "*.UnitTest" })
@PrepareForTest({ ReferenceCountHelper.class })
public class ReferenceCountHelperJUnitTest { 

  private ReferenceCountHelperImpl prepareInstance() {
    ReferenceCountHelperImpl rchi = mock(ReferenceCountHelperImpl.class);
    PowerMockito.mockStatic(ReferenceCountHelper.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(ReferenceCountHelper.getInstance()).thenReturn(rchi);
    return rchi;
  }
  
  @Test  
  public void trackReferenceCountsTrueTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    when(rchi.trackReferenceCounts()).thenReturn(true);
    boolean b = ReferenceCountHelper.trackReferenceCounts();
    assertTrue(b);
    verify(rchi).trackReferenceCounts();
  }

  @Test  
  public void trackReferenceCountsFalseTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    when(rchi.trackReferenceCounts()).thenReturn(false);
    boolean b = ReferenceCountHelper.trackReferenceCounts();
    assertFalse(b);
    verify(rchi).trackReferenceCounts();
  }

  @Test  
  public void trackFreedReferenceCountsTrueTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    when(rchi.trackFreedReferenceCounts()).thenReturn(true);
    assertTrue(ReferenceCountHelper.trackFreedReferenceCounts());
    verify(rchi).trackFreedReferenceCounts();
  }

  @Test  
  public void trackFreedReferenceCountsFalseTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    when(rchi.trackFreedReferenceCounts()).thenReturn(false);
    assertFalse(ReferenceCountHelper.trackFreedReferenceCounts());
    verify(rchi).trackFreedReferenceCounts();
  }

  @Test  
  public void setReferenceCountOwnerTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    Object theOwner = new Object();
    ReferenceCountHelper.setReferenceCountOwner(theOwner);
    verify(rchi).setReferenceCountOwner(theOwner);
  }

  @Test  
  public void createReferenceCountOwnerTest() throws Exception { 
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    Object expectedResult = new String("createReferenceCountOwner result");
    when(rchi.createReferenceCountOwner()).thenReturn(expectedResult);
    Object s = ReferenceCountHelper.createReferenceCountOwner();
    assertEquals(s, expectedResult);
    verify(rchi).createReferenceCountOwner();
  }

  @Test  
  public void skipRefCountTrackingTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    ReferenceCountHelper.skipRefCountTracking();
    verify(rchi).skipRefCountTracking();
  }

  @Test  
  public void isRefCountTrackingTrueTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    when(rchi.isRefCountTracking()).thenReturn(true);
    boolean b = ReferenceCountHelper.isRefCountTracking();
    assertTrue(b);
    verify(rchi).isRefCountTracking();
  }

  @Test  
  public void isRefCountTrackingFalseTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    when(rchi.isRefCountTracking()).thenReturn(false);
    boolean b = ReferenceCountHelper.isRefCountTracking();
    assertFalse(b);
    verify(rchi).isRefCountTracking();
  }

  @Test  
  public void unskipRefCountTrackingTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    ReferenceCountHelper.unskipRefCountTracking();
    verify(rchi).unskipRefCountTracking();
  }

  @Test  
  public void getRefCountInfoTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    List<RefCountChangeInfo> expectedResult = Collections.emptyList();
    when(rchi.getRefCountInfo((long)1000)).thenReturn(expectedResult);
    List<RefCountChangeInfo> l = ReferenceCountHelper.getRefCountInfo(1000);
    assertEquals(l, expectedResult);
    verify(rchi).getRefCountInfo(1000);    
  }

  @Test  
  public void refCountChangedTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    ReferenceCountHelper.refCountChanged((long)1000, true, 4);
    verify(rchi).refCountChanged((long)1000, true, 4);
  }

  @Test  
  public void freeRefCountInfoTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    ReferenceCountHelper.freeRefCountInfo((long)1000);
    verify(rchi).freeRefCountInfo((long)1000);
  }

  @Test  
  public void getReferenceCountOwnerTest() throws Exception {
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    Object expectedResult = new String("getReferenceCountOwner result");
    when(rchi.getReferenceCountOwner()).thenReturn(expectedResult);
    Object o = ReferenceCountHelper.getReferenceCountOwner();
    assertEquals(o, expectedResult);
    verify(rchi).getReferenceCountOwner();
  }

  @Test  
  public void getReenterCountTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    AtomicInteger expectedResult = new AtomicInteger(8);
    when(rchi.getReenterCount()).thenReturn(expectedResult);
    AtomicInteger ai = ReferenceCountHelper.getReenterCount();
    assertEquals(ai, expectedResult);
    verify(rchi).getReenterCount();
  }

  @Test  
  public void getFreeRefCountInfoTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    List<RefCountChangeInfo> expectedResult = Collections.emptyList();
    when(rchi.getFreeRefCountInfo((long) 1000)).thenReturn(expectedResult);
    List<RefCountChangeInfo> l = ReferenceCountHelper.getFreeRefCountInfo(1000);
    assertEquals(l, expectedResult);
    verify(rchi).getFreeRefCountInfo(1000);
  }

  @Test  
  public void peekRefCountInfoTest() throws Exception {    
    ReferenceCountHelperImpl rchi = prepareInstance(); 
    List<RefCountChangeInfo> expectedResult = Collections.emptyList();
    when(rchi.peekRefCountInfo((long) 1000)).thenReturn(expectedResult);
    List<RefCountChangeInfo> l = ReferenceCountHelper.peekRefCountInfo(1000);
    assertEquals(l, expectedResult);
    verify(rchi).peekRefCountInfo(1000);
  }
}