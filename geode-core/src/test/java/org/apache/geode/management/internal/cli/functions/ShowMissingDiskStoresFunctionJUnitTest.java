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

package org.apache.geode.management.internal.cli.functions;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ShowMissingDiskStoresFunctionJUnitTest {
  private GemFireCacheImpl cache;
  private GemFireCacheImpl oldCacheInstance;
  private InternalDistributedSystem system;
  private PartitionedRegion pr1;
  private PartitionedRegion pr2;
  private DistributedRegion prRoot;
  private PartitionAttributes pa;
  private PartitionRegionConfig prc;
  private Logger logger;
  private Appender mockAppender;
  private ArgumentCaptor<LogEvent> loggingEventCaptor;
  private FunctionContext context;
  private TestResultSender resultSender;
  private PersistentMemberManager memberManager;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    system = (InternalDistributedSystem) cache.getDistributedSystem();
    pr1 = mock(PartitionedRegion.class);
    pr2 = mock(PartitionedRegion.class);
    prRoot = mock(DistributedRegion.class);
    pa = mock(PartitionAttributes.class);
    prc = mock(PartitionRegionConfig.class);
    cache = Fakes.cache();
    oldCacheInstance = GemFireCacheImpl.setInstanceForTests(cache);
    resultSender = new TestResultSender();
    context = new FunctionContextImpl("testFunction", null, resultSender);
    memberManager = mock(PersistentMemberManager.class);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    GemFireCacheImpl.setInstanceForTests(oldCacheInstance);
  }

  private class TestSPRFFunc1 extends ShowMissingDiskStoresFunction {
    @Override
    protected Cache getCache() {
      return null;
    }
  }

  private class TestSPRFFunc2 extends ShowMissingDiskStoresFunction {
    @Override
    protected Cache getCache() {
      return cache;
    }
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#getCache()}.
   */
  @Test
  public final void testGetCache() {
    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    assertTrue(rff.getCache() instanceof Cache);
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   */
  @Test
  public final void testExecute() {
    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    List<?> results = null;

    when(cache.getPersistentMemberManager()).thenReturn(memberManager);

    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertNotNull(results);
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   */
  @Test
  public final void testExecuteWithNullContextThrowsRuntimeException() {
    expectedException.expect(RuntimeException.class);

    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    rff.execute(null);
    fail("Missing expected RuntimeException");
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   */
  @Test
  public final void testExecuteWithNullCacheInstanceHasEmptyResults() {
    TestSPRFFunc1 rff = new TestSPRFFunc1();
    List<?> results = null;

    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertNotNull(results);
    assertEquals(1,results.size());
    assertNull(results.get(0));
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   */
  @Test
  public final void testExecuteWithNullGFCIResultValueIsNull() {
    TestSPRFFunc2 rff = new TestSPRFFunc2();
    List<?> results = null;

    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    GemFireCacheImpl.setInstanceForTests(null);

    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertNotNull(results);
    assertEquals(1,results.size());
    assertNull(results.get(0));
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   */
  @Test
  public final void testExecuteWhenGFCIClosedResultValueIsNull() {
    TestSPRFFunc2 rff = new TestSPRFFunc2();
    List<?> results = null;

    when(cache.getPersistentMemberManager()).thenReturn(memberManager);
    when(((GemFireCacheImpl)cache).isClosed()).thenReturn(true);
    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertNotNull(results);
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   * @throws UnknownHostException 
   */
  @Test
  public final void testExecuteReturnsMissingDiskStores() throws UnknownHostException {
    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    List<?> results = null;

    when(cache.getPersistentMemberManager()).thenReturn(memberManager);

    // Fake missing disk-stores
    Set<PersistentMemberID> regions1 = new HashSet<PersistentMemberID>();
    regions1.add(new PersistentMemberID(new DiskStoreID(), InetAddress.getLocalHost(), "/diskStore1", 1L, (short)1));
    regions1.add(new PersistentMemberID(new DiskStoreID(), InetAddress.getLocalHost(), "/diskStore2", 2L, (short)2));
    Map<String,Set<PersistentMemberID>> mapMember1 = new HashMap<String,Set<PersistentMemberID>>();;
    mapMember1.put("member1", regions1);
    when(memberManager.getWaitingRegions()).thenReturn(mapMember1);

    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertNotNull(results);
    assertEquals(1,results.size());
    Set<?> detailSet = (Set<?>)results.get(0);
    assertEquals(2, detailSet.toArray().length);
    assertTrue(detailSet.toArray()[0] instanceof PersistentMemberPattern);
    assertTrue(detailSet.toArray()[1] instanceof PersistentMemberPattern);
    // Results are not sorted so verify results in either order
    if (((PersistentMemberPattern)detailSet.toArray()[0]).getDirectory().equals("/diskStore1")) {
      assertEquals("/diskStore2", ((PersistentMemberPattern)detailSet.toArray()[1]).getDirectory());
    } else if (((PersistentMemberPattern)detailSet.toArray()[0]).getDirectory().equals("/diskStore2")) {
      assertEquals("/diskStore1", ((PersistentMemberPattern)detailSet.toArray()[1]).getDirectory());
    } else {
      fail("Incorrect missing colocated region results");
    }
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   */
  @Test
  public final void testExecuteReturnsMissingColocatedRegions() {
    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    List<?> results = null;

    when(cache.getPersistentMemberManager()).thenReturn(memberManager);

    // Fake missing colocated regions
    Set<PartitionedRegion> prs = new HashSet<PartitionedRegion>();
    prs.add(pr1);
    prs.add(pr2);
    List<String> missing1 = new ArrayList<String>(Arrays.asList("child1", "child2"));
    when(cache.getPartitionedRegions()).thenReturn(prs);
    when(pr1.getMissingColocatedChildren()).thenReturn(missing1);
    when(pr1.getFullPath()).thenReturn("/pr1");

    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertEquals(1,results.size());
    Set<?> detailSet = (Set<?>)results.get(0);
    assertEquals(2, detailSet.toArray().length);
    assertTrue(detailSet.toArray()[0] instanceof ColocatedRegionDetails);
    assertTrue(detailSet.toArray()[1] instanceof ColocatedRegionDetails);
    assertEquals("/pr1", ((ColocatedRegionDetails)detailSet.toArray()[0]).getParent());
    assertEquals("/pr1", ((ColocatedRegionDetails)detailSet.toArray()[1]).getParent());
    // Results are not sorted so verify results in either order
    if (((ColocatedRegionDetails)detailSet.toArray()[0]).getChild().equals("child1")) {
      assertEquals("child2", ((ColocatedRegionDetails)detailSet.toArray()[1]).getChild());
    } else if (((ColocatedRegionDetails)detailSet.toArray()[0]).getChild().equals("child2")) {
      assertEquals("child1", ((ColocatedRegionDetails)detailSet.toArray()[1]).getChild());
    } else {
      fail("Incorrect missing colocated region results");
    }
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   * @throws UnknownHostException 
   */
  @Test
  public final void testExecuteReturnsMissingStoresAndRegions() throws UnknownHostException {
    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    List<?> results = null;

    when(cache.getPersistentMemberManager()).thenReturn(memberManager);

    // Fake missing disk-stores
    Set<PersistentMemberID> regions1 = new HashSet<PersistentMemberID>();
    regions1.add(new PersistentMemberID(new DiskStoreID(), InetAddress.getLocalHost(), "/diskStore1", 1L, (short)1));
    regions1.add(new PersistentMemberID(new DiskStoreID(), InetAddress.getLocalHost(), "/diskStore2", 2L, (short)2));
    Map<String,Set<PersistentMemberID>> mapMember1 = new HashMap<String,Set<PersistentMemberID>>();;
    mapMember1.put("member1", regions1);
    when(memberManager.getWaitingRegions()).thenReturn(mapMember1);

    // Fake missing colocated regions
    Set<PartitionedRegion> prs = new HashSet<PartitionedRegion>();
    prs.add(pr1);
    prs.add(pr2);
    List<String> missing1 = new ArrayList<String>(Arrays.asList("child1", "child2"));
    when(cache.getPartitionedRegions()).thenReturn(prs);
    when(pr1.getMissingColocatedChildren()).thenReturn(missing1);
    when(pr1.getFullPath()).thenReturn("/pr1");

    rff.execute(context);
    try {
      results = resultSender.getResults();
    } catch (Throwable e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
    assertEquals(2,results.size());
    for (Object result:results) {
      Set<?> detailSet = (Set<?>)result;
      if (detailSet.toArray()[0] instanceof PersistentMemberPattern) {
        assertEquals(2, detailSet.toArray().length);
        assertTrue(detailSet.toArray()[1] instanceof PersistentMemberPattern);
        // Results are not sorted so verify results in either order
        if (((PersistentMemberPattern)detailSet.toArray()[0]).getDirectory().equals("/diskStore1")) {
          assertEquals("/diskStore2", ((PersistentMemberPattern)detailSet.toArray()[1]).getDirectory());
        } else if (((PersistentMemberPattern)detailSet.toArray()[0]).getDirectory().equals("/diskStore2")) {
          assertEquals("/diskStore1", ((PersistentMemberPattern)detailSet.toArray()[1]).getDirectory());
        } else {
          fail("Incorrect missing colocated region results");
        }
      } else if (detailSet.toArray()[0] instanceof ColocatedRegionDetails) {
        assertEquals(2, detailSet.toArray().length);
        assertTrue(detailSet.toArray()[1] instanceof ColocatedRegionDetails);
        assertEquals("/pr1", ((ColocatedRegionDetails)detailSet.toArray()[0]).getParent());
        assertEquals("/pr1", ((ColocatedRegionDetails)detailSet.toArray()[1]).getParent());
        // Results are not sorted so verify results in either order
        if (((ColocatedRegionDetails)detailSet.toArray()[0]).getChild().equals("child1")) {
          assertEquals("child2", ((ColocatedRegionDetails)detailSet.toArray()[1]).getChild());
        } else if (((ColocatedRegionDetails)detailSet.toArray()[0]).getChild().equals("child2")) {
          assertEquals("child1", ((ColocatedRegionDetails)detailSet.toArray()[1]).getChild());
        } else {
          fail("Incorrect missing colocated region results");
        }
      } else {
        fail("Unexpected result type: " + detailSet.toArray()[0].getClass());
      }
    }
  }

  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#execute(org.apache.geode.cache.execute.FunctionContext)}.
   * @throws Throwable 
   */
  @Test
  public final void testExecuteCatchesExceptions() throws Throwable {
    expectedException.expect(RuntimeException.class);

    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();

    when(cache.getPersistentMemberManager()).thenThrow(new RuntimeException());

    rff.execute(context);
    List<?> results = resultSender.getResults();
    fail("Failed to catch expected RuntimeException");
  }


  /**
   * Test method for {@link org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction#getId()}.
   */
  @Test
  public final void testGetId() {
    ShowMissingDiskStoresFunction rff = new ShowMissingDiskStoresFunction();
    assertEquals(ShowMissingDiskStoresFunction.class.getName(), rff.getId());
  }

  private static class TestResultSender implements ResultSender {

    private final List<Object> results = new LinkedList<Object>();

    private Throwable t;

    protected List<Object> getResults() throws Throwable {
      if (t != null) {
        throw t;
      }
      return Collections.unmodifiableList(results);
    }

    @Override
    public void lastResult(final Object lastResult) {
      results.add(lastResult);
    }

    @Override
    public void sendResult(final Object oneResult) {
      results.add(oneResult);
    }

    @Override
    public void sendException(final Throwable t) {
      this.t = t;
    }
  }
}
