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
package com.gemstone.gemfire.internal.cache.lru;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.PlaceHolderDiskRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

/**  This class tests the LRUCapacityController's core clock algorithm.  */
@Category(IntegrationTest.class)
public class LRUClockJUnitTest extends junit.framework.TestCase {
  
  private String myTestName;

  //static int unusedPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);

  static Properties sysProps = new Properties();
  static {
    //sysProps.setProperty(DistributionConfig.DistributedSystemConfigProperties.MCAST_PORT, String.valueOf(unusedPort));
    // a loner is all this test needs
    sysProps.setProperty(MCAST_PORT, "0");
    sysProps.setProperty(LOCATORS, "");
  }

  public LRUClockJUnitTest( String name ) {
    super( name );
    this.myTestName = name;
  }

  protected LRUClockJUnitTest( String prefix, String methodName ) {
    super( methodName );
    this.myTestName = prefix + methodName;
  }
  
  /**
   *  The JUnit setup method
   *
   * @exception  Exception  Description of the Exception
   */
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.out.println( "\n\n### beginning " + this.myTestName + "###" );
  }

  /**
   *  The teardown method for JUnit
   *
   * @exception  Exception  Description of the Exception
   */
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.out.println( "###  finished " + this.myTestName + "###" );
  }

  /**
   *  A unit test for JUnit
   *
   * @exception  Exception  Description of the Exception
   */
  @Test
  public void testAddToClockFace() throws Exception {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );
    
    // getLRUEntry( maxScan )
    
    LRUTestEntry[] nodes = new LRUTestEntry[10];
    int i = 0;
    for( i = 0; i < 10; i++ ) {
      nodes[i] = getANode( i ); 
      clock.appendEntry( nodes[i] );
    }
    
    // getLRUEntry until empty... verify order of results.
    
    for( i = 0; i < 10; i++ ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    
    assertTrue( "expected null", clock.getLRUEntry( ) == null );
  }

  @Test
  public void testFIFO() {
    try {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );

    for( int i = 0; i < 100; i++ ) {
      LRUClockNode entry = getANode( i );
      clock.appendEntry( entry );
    }
    
    for( int i = 100; i < 2000; i++ ) {
      LRUClockNode entry = getANode( i );
      clock.appendEntry( entry );
      Object obj = clock.getLRUEntry();
      if ( obj instanceof LRUTestEntry ) {
        LRUTestEntry le = (LRUTestEntry) obj;
        le.setEvicted();
      } else {
        assertTrue( "found wrong type: " + obj.getClass().getName(), false );
      }
      
    }

    int counter = 0;
    LRUTestEntry le = (LRUTestEntry) clock.getLRUEntry(); 
    while( le != null ) {
      counter++;
      le = (LRUTestEntry) clock.getLRUEntry();
    }
    assertTrue( "expected 100, found " + counter, counter == 100);
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch ( Throwable t ) {
      t.printStackTrace();
      assertTrue( "failed", false );
    }
  }
  
  @Test
  public void testEvicted() throws Exception {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );
    
    // getLRUEntry( maxScan )
    
    LRUTestEntry[] nodes = new LRUTestEntry[10];
    int i = 0;
    for( i = 0; i < 10; i++ ) {
      nodes[i] = getANode( i ); 
      clock.appendEntry( nodes[i] );
    }

    for( i = 0; i < 10; i += 2 ) {
      clock.unlinkEntry( nodes[i] );
    }
    
    // getLRUEntry until empty... verify order of results.
    
    for( i = 1; i < 10; i += 2 ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    
    assertTrue( "expected null", clock.getLRUEntry( ) == null );
  }

  @Test
  public void testRecentlyUsed() throws Exception {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );
    
    // getLRUEntry( maxScan )
    
    LRUTestEntry[] nodes = new LRUTestEntry[10];
    int i = 0;
    for( i = 0; i < 10; i++ ) {
      nodes[i] = getANode( i ); 
      clock.appendEntry( nodes[i] );
      if ( i % 2 == 0 ) {
        nodes[i].setRecentlyUsed(); 
      }
    }
    
    // getLRUEntry until empty... verify order of results.

    // should find 1, 3, etc... as 0, 2, 4 etc... were marked recently used..      
    for( i = 1; i < 10; i += 2 ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    
    // now 0, 2, 4 should go...
    for( i = 0; i < 10; i += 2 ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    
    assertTrue( "expected null", clock.getLRUEntry( ) == null );
  }
  
  @Test
  public void testRemoveHead() throws Exception {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );
    LRUTestEntry[] nodes = new LRUTestEntry[10];
    int i = 0;
    for( i = 0; i < 10; i++ ) {
      nodes[i] = getANode( i ); 
      clock.appendEntry( nodes[i] );
    }
    clock.unlinkEntry(nodes[0]);
    for( i = 1; i < 10; i ++ ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    assertEquals(null, clock.getLRUEntry( ));
  }
  
  @Test
  public void testRemoveMiddle() throws Exception {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );
    LRUTestEntry[] nodes = new LRUTestEntry[10];
    int i = 0;
    for( i = 0; i < 10; i++ ) {
      nodes[i] = getANode( i ); 
      clock.appendEntry( nodes[i] );
    }
    clock.unlinkEntry(nodes[5]);
    for( i = 0; i < 5; i ++ ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    for( i = 6; i < 10; i ++ ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    assertEquals(null, clock.getLRUEntry( ));
  }
  
  @Test
  public void testRemoveTail() throws Exception {
    NewLRUClockHand clock = getAClockHand( getARegion(), new TestEnableLRU() );
    LRUTestEntry[] nodes = new LRUTestEntry[10];
    int i = 0;
    for( i = 0; i < 10; i++ ) {
      nodes[i] = getANode( i ); 
      clock.appendEntry( nodes[i] );
    }
    clock.unlinkEntry(nodes[9]);
    
    for( i = 0; i < 9; i ++ ) {
      LRUTestEntry n = (LRUTestEntry) clock.getLRUEntry( );
      assertTrue( "expected nodes[" + nodes[i].id() + "], found nodes[" + n.id() + "]", n == nodes[i] );
    }
    assertEquals(null, clock.getLRUEntry( ));
  }
  
  /** manufacture a node so that a shared type can be used by SharedLRUClockTest. */
  protected LRUTestEntry getANode( int id ) {
    return new LocalLRUTestEntry( id );
  }
  
  public static interface LRUTestEntry extends LRUClockNode {
    public int id();
  }
  
  /** test implementation of an LRUClockNode */
  public static class LocalLRUTestEntry implements LRUTestEntry {
    
    int id;
    LRUClockNode next;
    LRUClockNode prev;
    int size;
    boolean recentlyUsed;
    boolean evicted;
    
    public LocalLRUTestEntry( int id ) {
      this.id = id;
      next = null;
      prev = null;
      size = 0;
      recentlyUsed = false;
      evicted = false;
    }
    
    public int id() {
      return id; 
    }
    
    public boolean isTombstone() {
      return false;
    }
    
    public void setNextLRUNode( LRUClockNode next ) {
      this.next = next; 
    }
    
    public LRUClockNode nextLRUNode() {
      return this.next; 
    }

    public void setPrevLRUNode( LRUClockNode prev ) {
      this.prev = prev; 
    }
    
    public LRUClockNode prevLRUNode() {
      return this.prev; 
    }
    
    
    public int updateEntrySize( EnableLRU cc ) {
      return this.size = 1; 
    }

    public int updateEntrySize(EnableLRU cc, Object value) {
      return this.size = 1; 
    }
    
    public int getEntrySize() {
      return this.size; 
    }
    
    /** this should only happen with the LRUClockHand sync'ed */
    public void setEvicted() {
      evicted = true;
    }

    public void unsetEvicted() {
      evicted = false;
    }
    
    public boolean testEvicted( ) {
      return evicted; 
    }
    
    public boolean testRecentlyUsed() {
      return recentlyUsed;  
    }
    
    public void setRecentlyUsed() {
      recentlyUsed = true;  
    }
    
    public void unsetRecentlyUsed() {
      recentlyUsed = false; 
    }
    public LRUClockNode absoluteSelf( ) { return this; }
    public LRUClockNode clearClones( ) { return this; }
    public int cloneCount( ) { return 0; }
  }


  public class TestEnableLRU implements EnableLRU {

    private final StatisticsType statType;

    {
    // create the stats type for MemLRU.
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    
    final String bytesAllowedDesc = 
      "Number of total bytes allowed in this region.";
    final String byteCountDesc = 
      "Number of bytes in region.";
    final String lruEvictionsDesc = 
      "Number of total entry evictions triggered by LRU.";
    final String lruEvaluationsDesc = 
      "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc =
      "Number of non-LRU entries evicted during LRU operations";
    final String lruDestroysDesc =
      "Number of entry destroys triggered by LRU.";
    final String lruDestroysLimitDesc =
      "Maximum number of entry destroys triggered by LRU before scan occurs.";
      
    statType = f.createType( "TestLRUStatistics",
      "Statistics about byte based Least Recently Used region entry disposal",
      new StatisticDescriptor[] {
        f.createLongGauge("bytesAllowed", bytesAllowedDesc, "bytes" ),
        f.createLongGauge("byteCount", byteCountDesc, "bytes" ),
        f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries" ),
        f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries" ),
        f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries"),
        f.createLongCounter("lruDestroys", lruDestroysDesc, "entries" ),
        f.createLongCounter("lruDestroysLimit", lruDestroysLimitDesc, "entries" ),
      }
    );
  }

  
    public int entrySize( Object key, Object value ) throws IllegalArgumentException {
      return 1;  
    }
    
    public long limit( ) {
      return 20; 
    }
    
    public boolean usesMem( ) {
      return false; 
    }
    
    public EvictionAlgorithm getEvictionAlgorithm() {
      return EvictionAlgorithm.LRU_ENTRY;
    }
    
    public LRUStatistics getStats() {
      return null;
    }

    public EvictionAction getEvictionAction() {
      return EvictionAction.DEFAULT_EVICTION_ACTION;
    }

      public StatisticsType getStatisticsType() {
        return statType;
      }

      public String getStatisticsName() {
        return "TestLRUStatistics";
      }

      public int getLimitStatId() {
        return statType.nameToId("bytesAllowed");

      }

      public int getCountStatId() {
        return statType.nameToId("byteCount");
      }

      public int getEvictionsStatId() {
        return statType.nameToId("lruEvictions");
      }

      public int getDestroysStatId() {
        return statType.nameToId("lruDestroys");
      }

      public int getDestroysLimitStatId() {
        return statType.nameToId("lruDestroysLimit");
      }

      public int getEvaluationsStatId() {
        return statType.nameToId("lruEvaluations");
      }
      
      public int getGreedyReturnsStatId() {
        return statType.nameToId("lruGreedyReturns");
      }

    public boolean mustEvict(LRUStatistics stats, Region region, int delta) {
      throw new UnsupportedOperationException("Not implemented");
    }

    public void afterEviction() {
      throw new UnsupportedOperationException("Not implemented");
    }

    public LRUStatistics initStats(Object region, StatisticsFactory sf)
    {
      String regionName;
      if (region instanceof Region) {
        regionName = ((Region)region).getName();
      } else if (region instanceof PlaceHolderDiskRegion) {
        regionName = ((PlaceHolderDiskRegion)region).getName();
        // @todo make it shorter (I think it is the fullPath
      } else {
        throw new IllegalStateException("expected Region or PlaceHolderDiskRegion");
      }
      final LRUStatistics stats = new LRUStatistics(sf, "TestLRUStatistics" + regionName, this);
      stats.setLimit( limit() );
      return stats;
    }
  }

  /** overridden in SharedLRUClockTest to test SharedLRUClockHand */
  protected NewLRUClockHand getAClockHand( Region reg, EnableLRU elru ) {
    return new NewLRUClockHand( reg, elru,new InternalRegionArguments());
  }
  
  private Region getARegion() throws Exception {
    DistributedSystem ds = DistributedSystem.connect( sysProps );
    Cache c = null;
    try {
      c = CacheFactory.create( ds );
    } catch ( CacheExistsException cee ) {
      c = CacheFactory.getInstance( ds ); 
    }
    AttributesFactory af = new AttributesFactory();
    Region root = c.getRegion("root");
    if ( root == null ) {
      root = c.createRegion("root", af.create() );
    }
    Region sub = root.createSubregion( myTestName, af.create() );
    return sub;
  }
  
}

