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

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.PlaceHolderDiskRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This class tests the LRUCapacityController's core clock algorithm.
 */
@Category(IntegrationTest.class)
public class LRUClockJUnitTest {
  
  private String myTestName;

  static Properties sysProps = new Properties();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    sysProps = new Properties();
    sysProps.setProperty(MCAST_PORT, "0");
    sysProps.setProperty(LOCATORS, "");
  }

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
  public void testFIFO() throws Exception {
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
  private LRUTestEntry getANode( int id ) {
    return new LocalLRUTestEntry( id );
  }

  private interface LRUTestEntry extends LRUClockNode {
    public int id();
  }
  
  /** test implementation of an LRUClockNode */
  private static class LocalLRUTestEntry implements LRUTestEntry {
    
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

    @Override
    public int id() {
      return id; 
    }
    
    public boolean isTombstone() {
      return false;
    }

    @Override
    public void setNextLRUNode( LRUClockNode next ) {
      this.next = next; 
    }

    @Override
    public LRUClockNode nextLRUNode() {
      return this.next; 
    }

    @Override
    public void setPrevLRUNode( LRUClockNode prev ) {
      this.prev = prev; 
    }

    @Override
    public LRUClockNode prevLRUNode() {
      return this.prev; 
    }

    @Override
    public int updateEntrySize( EnableLRU cc ) {
      return this.size = 1; 
    }

    @Override
    public int updateEntrySize(EnableLRU cc, Object value) {
      return this.size = 1; 
    }

    @Override
    public int getEntrySize() {
      return this.size; 
    }
    
    /** this should only happen with the LRUClockHand sync'ed */
    @Override
    public void setEvicted() {
      evicted = true;
    }

    @Override
    public void unsetEvicted() {
      evicted = false;
    }

    @Override
    public boolean testEvicted( ) {
      return evicted; 
    }

    @Override
    public boolean testRecentlyUsed() {
      return recentlyUsed;  
    }

    @Override
    public void setRecentlyUsed() {
      recentlyUsed = true;  
    }

    @Override
    public void unsetRecentlyUsed() {
      recentlyUsed = false; 
    }

    public LRUClockNode absoluteSelf( ) { return this; }
    public LRUClockNode clearClones( ) { return this; }
    public int cloneCount( ) { return 0; }
  }

  private class TestEnableLRU implements EnableLRU {

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

    @Override
    public int entrySize( Object key, Object value ) throws IllegalArgumentException {
      return 1;  
    }

    @Override
    public long limit( ) {
      return 20; 
    }
    
    public boolean usesMem( ) {
      return false; 
    }

    @Override
    public EvictionAlgorithm getEvictionAlgorithm() {
      return EvictionAlgorithm.LRU_ENTRY;
    }

    @Override
    public LRUStatistics getStats() {
      return null;
    }

    @Override
    public EvictionAction getEvictionAction() {
      return EvictionAction.DEFAULT_EVICTION_ACTION;
    }

    @Override
    public StatisticsType getStatisticsType() {
      return statType;
    }

    @Override
    public String getStatisticsName() {
      return "TestLRUStatistics";
    }

    @Override
    public int getLimitStatId() {
      return statType.nameToId("bytesAllowed");
    }

    @Override
    public int getCountStatId() {
      return statType.nameToId("byteCount");
    }

    @Override
    public int getEvictionsStatId() {
      return statType.nameToId("lruEvictions");
    }

    @Override
    public int getDestroysStatId() {
      return statType.nameToId("lruDestroys");
    }

    @Override
    public int getDestroysLimitStatId() {
      return statType.nameToId("lruDestroysLimit");
    }

    @Override
    public int getEvaluationsStatId() {
      return statType.nameToId("lruEvaluations");
    }

    @Override
    public int getGreedyReturnsStatId() {
      return statType.nameToId("lruGreedyReturns");
    }

    @Override
    public boolean mustEvict(LRUStatistics stats, Region region, int delta) {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void afterEviction() {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public LRUStatistics initStats(Object region, StatisticsFactory sf) {
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
  private NewLRUClockHand getAClockHand( Region reg, EnableLRU elru ) {
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
    Region sub = root.createSubregion( testName.getMethodName(), af.create() );
    return sub;
  }
  
}

