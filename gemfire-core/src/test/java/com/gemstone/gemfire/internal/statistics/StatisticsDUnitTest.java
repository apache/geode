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
package com.gemstone.gemfire.internal.statistics;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.RegionMembershipListenerAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.GemFireStatSampler;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.StatArchiveReader;
import com.gemstone.gemfire.internal.StatArchiveReader.StatSpec;
import com.gemstone.gemfire.internal.StatSamplerStats;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.StatArchiveReader.StatValue;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Integration tests for Statistics. VM0 performs puts and VM1 receives
 * updates. Both use custom statistics for start/end with increment to
 * add up puts and updates. Then validation tests values in stat resource
 * instances and uses StatArchiveReader. Both are tested against static
 * counters in both VMs.
 * <p/>
 * This test mimics hydratest/locators/cacheDS.conf in an attempt to reproduce
 * bug #45478. So far this test passes consistently.
 *
 * @author Kirk Lund
 * @since 7.0
 */
@SuppressWarnings("serial")
public class StatisticsDUnitTest extends CacheTestCase {

  private static final String dir = "StatisticsDUnitTest";

  private static final int MAX_PUTS = 1000;
  private static final int NUM_KEYS = 100;
  private static final int NUM_PUB_THREADS = 2;
  private static final int NUM_PUBS = 2;
  private static final boolean RANDOMIZE_PUTS = true;
  
  private static AtomicInteger updateEvents = new AtomicInteger();
  
  private static AtomicInteger puts = new AtomicInteger();
  
  private static AtomicReference<PubSubStats> subStatsRef = new AtomicReference<PubSubStats>();
  
  private static AtomicReferenceArray<PubSubStats> pubStatsRef = new AtomicReferenceArray<PubSubStats>(NUM_PUB_THREADS);
  
  /** Thread-safe static reference to RegionMembershipListener instance */
  private static AtomicReference<RegionMembershipListener> rmlRef = 
      new AtomicReference<RegionMembershipListener>();
  
  @SuppressWarnings("unused") /** invoked by reflection */
  private static void cleanup() {
    updateEvents.set(0);
    rmlRef.set(null);
  }
  
  @SuppressWarnings("unused") /** invoked by reflection */
  private static int getUpdateEvents() {
    return updateEvents.get();
  }
  
  @SuppressWarnings("unused") /** invoked by reflection */
  private static int getPuts() {
    return puts.get();
  }
  
  public StatisticsDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(getClass(), "cleanup");
    disconnectAllFromDS(); // because this test enabled stat sampling!
  }
  
  public void testPubAndSubCustomStats() throws Exception {
    final String testName = "testPubAndSubCustomStats";

    final String regionName = "region_" + testName;
    final VM[] pubs = new VM[NUM_PUBS];
    for (int pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      pubs[pubVM] = Host.getHost(0).getVM(pubVM);
    }
    final VM sub = Host.getHost(0).getVM(NUM_PUBS);

    final String subArchive = dir + File.separator + testName + "_sub" + ".gfs";
    final String[] pubArchives = new String[NUM_PUBS];
    for (int pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      pubArchives[pubVM] = dir + File.separator + testName + "_pub-" + pubVM + ".gfs";
    }
    
    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubVM = i;
      pubs[pubVM].invoke(new CacheSerializableRunnable("pub-connect-and-create-data-" + pubVM) {
        public void run2() throws CacheException {
          new File(dir).mkdir();
          final Properties props = new Properties();
          props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
          props.setProperty(DistributionConfig.STATISTIC_SAMPLE_RATE_NAME, "1000");
          props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, pubArchives[pubVM]);
          final InternalDistributedSystem system = getSystem(props);
  
          // assert that sampler is working as expected
          final GemFireStatSampler sampler = system.getStatSampler();
          assertTrue(sampler.isSamplingEnabled());
          assertTrue(sampler.isAlive());
          assertEquals(new File(pubArchives[pubVM]), sampler.getArchiveFileName());
          
          final WaitCriterion waitForSampleCollector = new WaitCriterion() {
            public boolean done() {
              return sampler.getSampleCollector() != null;
            }
            public String description() {
              return "sampler.getSampleCollector() is still null!";
            }
          };
          waitForCriterion(waitForSampleCollector, 4*1000, 10, true);
  
          final SampleCollector sampleCollector = sampler.getSampleCollector();
          assertNotNull(sampleCollector);
          
          final StatArchiveHandler archiveHandler = sampleCollector.getStatArchiveHandler();
          assertNotNull(archiveHandler);
          assertTrue(archiveHandler.isArchiving());
          
          // create cache and region
          final Cache cache = getCache();
          final RegionFactory<String, Number> factory = cache.createRegionFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          
          final RegionMembershipListener rml = new RegionMembershipListener();
          rmlRef.set(rml);
          factory.addCacheListener(rml);
          final Region<String, Number> region = factory.create(regionName);
          
          // create the keys
          if (region.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
            for (int key = 0; key < NUM_KEYS; key++) {
              region.create("KEY-"+key, null);
            }
          }
        }
      });
    }
    
    final DistributedMember subMember = (DistributedMember)
    sub.invoke(new SerializableCallable("sub-connect-and-create-keys") {
      @Override
      public Object call() throws Exception {
        new File(dir).mkdir();
        final Properties props = new Properties();
        props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
        props.setProperty(DistributionConfig.STATISTIC_SAMPLE_RATE_NAME, "1000");
        props.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, subArchive);
        final InternalDistributedSystem system = getSystem(props);
        
        final PubSubStats statistics = new PubSubStats(system, "sub-1", 1);
        subStatsRef.set(statistics);
        
        // assert that sampler is working as expected
        final GemFireStatSampler sampler = system.getStatSampler();
        assertTrue(sampler.isSamplingEnabled());
        assertTrue(sampler.isAlive());
        assertEquals(new File(subArchive), sampler.getArchiveFileName());
        
        final WaitCriterion waitForSampleCollector = new WaitCriterion() {
          public boolean done() {
            return sampler.getSampleCollector() != null;
          }
          public String description() {
            return "sampler.getSampleCollector() is still null!";
          }
        };
        waitForCriterion(waitForSampleCollector, 2*1000, 10, true);

        final SampleCollector sampleCollector = sampler.getSampleCollector();
        assertNotNull(sampleCollector);
        
        final StatArchiveHandler archiveHandler = sampleCollector.getStatArchiveHandler();
        assertNotNull(archiveHandler);
        assertTrue(archiveHandler.isArchiving());
        
        // create cache and region with UpdateListener
        final Cache cache = getCache();
        final RegionFactory<String, Number> factory = cache.createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        
        final CacheListener<String, Number> cl = new UpdateListener(statistics);          
        factory.addCacheListener(cl);
        final Region<String, Number> region = factory.create(regionName);
        
        // create the keys
        if (region.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
          for (int key = 0; key < NUM_KEYS; key++) {
            region.create("KEY-"+key, null);
          }
        }
        
        assertEquals(0, statistics.getUpdateEvents());
        return system.getDistributedMember();
      }
    });
    
    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubVM = i;
      AsyncInvocation[] publishers = new AsyncInvocation[NUM_PUB_THREADS];
      for (int j = 0; j < NUM_PUB_THREADS; j++) {
        final int pubThread = j;
        publishers[pubThread] = pubs[pubVM].invokeAsync(
            new CacheSerializableRunnable("pub-connect-and-put-data-" + pubVM + "-thread-" + pubThread) {
          public void run2() throws CacheException {
            final PubSubStats statistics = new PubSubStats(system, "pub-" + pubThread, pubVM);
            pubStatsRef.set(pubThread, statistics);
            
            final RegionMembershipListener rml = rmlRef.get();
            final Region<String, Number> region = getCache().getRegion(regionName);
    
            // assert that sub is in rml membership
            assertNotNull(rml);
            WaitCriterion wc = new WaitCriterion() {
              public boolean done() {
                return rml.contains(subMember) && rml.size() == NUM_PUBS;
              }
              public String description() {
                return rml.members + " should contain " + subMember;
              }
            };
            waitForCriterion(wc, 4*1000, 10, true);
            
            // publish lots of puts cycling through the NUM_KEYS
            assertEquals(0, statistics.getPuts());
            
            // cycle through the keys randomly
            if (RANDOMIZE_PUTS) {
              Random randomGenerator = new Random();
              int key = 0;
              for (int i = 0; i < MAX_PUTS; i++) {
                final long start = statistics.startPut();
                key = randomGenerator.nextInt(NUM_KEYS);
                region.put("KEY-"+key, i);
                statistics.endPut(start);
              }
              
            // cycle through he keys in order and wrapping back around
            } else {
              int key = 0;
              for (int i = 0; i < MAX_PUTS; i++) {
                final long start = statistics.startPut();
                region.put("KEY-"+key, i);
                key++; // cycle through the keys...
                if (key >= NUM_KEYS) {
                  key = 0;
                }
                statistics.endPut(start);
              }
            }
            assertEquals(MAX_PUTS, statistics.getPuts());
            
            // wait for 2 samples to ensure all stats have been archived
            final StatisticsType statSamplerType = getSystem().findType("StatSampler");
            final Statistics[] statsArray = getSystem().findStatisticsByType(statSamplerType);
            assertEquals(1, statsArray.length);
    
            final Statistics statSamplerStats = statsArray[0];
            final int initialSampleCount = statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT);
            
            wc = new WaitCriterion() {
              public boolean done() {
                return statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT) >= initialSampleCount + 2;
              }
              public String description() {
                return "Waiting for " + StatSamplerStats.SAMPLE_COUNT + " >= " + initialSampleCount + 2;
              }
            };
            waitForCriterion(wc, 4*1000, 10, true);
          }
        });
      }
      for (int pubThread = 0; pubThread < publishers.length; pubThread++) {
        publishers[pubThread].join();
        if (publishers[pubThread].exceptionOccurred()) {
          fail("Test failed", publishers[pubThread].getException());
        }
      }
    }
    
    sub.invoke(new CacheSerializableRunnable("sub-wait-for-samples") {
      public void run2() throws CacheException {
        // wait for 2 samples to ensure all stats have been archived
        final StatisticsType statSamplerType = getSystem().findType("StatSampler");
        final Statistics[] statsArray = getSystem().findStatisticsByType(statSamplerType);
        assertEquals(1, statsArray.length);

        final Statistics statSamplerStats = statsArray[0];
        final int initialSampleCount = statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT);
        
        final WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            return statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT) >= initialSampleCount + 2;
          }
          public String description() {
            return "Waiting for " + StatSamplerStats.SAMPLE_COUNT + " >= " + initialSampleCount + 2;
          }
        };
        waitForCriterion(wc, 4*1000, 10, true);
        
        // now post total updateEvents to static
        final PubSubStats statistics = subStatsRef.get();
        assertNotNull(statistics);
        updateEvents.set(statistics.getUpdateEvents());
      }
    });
    
    // validate pub values against sub values
    final int totalUpdateEvents = sub.invokeInt(getClass(), "getUpdateEvents");
    
    // validate pub values against pub statistics against pub archive
    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubIdx = i;
      pubs[pubIdx].invoke(new CacheSerializableRunnable("pub-validation") {
        @SuppressWarnings("unused")
        public void run2() throws CacheException {
          // add up all the puts
          assertEquals(NUM_PUB_THREADS, pubStatsRef.length());
          int totalPuts = 0;
          for (int pubThreadIdx = 0; pubThreadIdx < NUM_PUB_THREADS; pubThreadIdx++) {
            PubSubStats statistics = pubStatsRef.get(pubThreadIdx);
            assertNotNull(statistics);
            totalPuts += statistics.getPuts();
          }
          
          // assert that total puts adds up to max puts times num threads
          assertEquals(MAX_PUTS * NUM_PUB_THREADS, totalPuts);
          
          // assert that archive file contains same values as statistics
          File archive = new File(pubArchives[pubIdx]);
          assertTrue(archive.exists());
  
          StatArchiveReader reader = null;
          try {
            reader = new StatArchiveReader(new File[]{archive}, null, false);
          } catch (IOException e) {
            fail("Failed to read " + archive);
          }
  
          double combinedPuts = 0;
  
          @SuppressWarnings("rawtypes")
          List resources = reader.getResourceInstList();
          assertNotNull(resources);
          assertFalse(resources.isEmpty());
          
          for (@SuppressWarnings("rawtypes")
          Iterator iter = resources.iterator(); iter.hasNext();) {
            StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) iter.next();
            if (!ri.getType().getName().equals(PubSubStats.TYPE_NAME)) {
              continue;
            }
            
            StatValue[] statValues = ri.getStatValues();
            for (int i = 0; i < statValues.length; i++) {
              String statName = ri.getType().getStats()[i].getName();
              assertNotNull(statName);
              
              if (statName.equals(PubSubStats.PUTS)) {
                StatValue sv = statValues[i];
                sv.setFilter(StatValue.FILTER_NONE);
                
                double mostRecent = sv.getSnapshotsMostRecent();
                double min = sv.getSnapshotsMinimum();
                double max = sv.getSnapshotsMaximum();
                double maxMinusMin = sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum();
                double mean = sv.getSnapshotsAverage();
                double stdDev = sv.getSnapshotsStandardDeviation();
                
                assertEquals(mostRecent, max);
  
                double summation = 0;
                double[] rawSnapshots = sv.getRawSnapshots();
                for (int j = 0; j < rawSnapshots.length; j++) {
                  //log.convertToLogWriter().info("DEBUG " + ri.getName() + " " + statName + " rawSnapshots[" + j + "] = " + rawSnapshots[j]);
                  summation += rawSnapshots[j];
                }
                assertEquals(mean, summation / sv.getSnapshotsSize());
                
                combinedPuts += mostRecent;
              }
            }
          }
          
          // assert that sum of mostRecent values for all puts equals totalPuts
          assertEquals((double)totalPuts, combinedPuts);
          puts.getAndAdd(totalPuts);
        }
      });
    }
    
    // validate pub values against sub values
    int totalCombinedPuts = 0;
    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubIdx = i;
      final int totalPuts = pubs[pubIdx].invokeInt(getClass(), "getPuts");
      assertEquals(MAX_PUTS * NUM_PUB_THREADS, totalPuts);
      totalCombinedPuts += totalPuts;
    }
    assertEquals(totalCombinedPuts, totalUpdateEvents);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, totalCombinedPuts);
    
    // validate sub values against sub statistics against sub archive
    final int totalPuts = totalCombinedPuts;
    sub.invoke(new CacheSerializableRunnable("sub-validation") {
      @SuppressWarnings("unused")
      public void run2() throws CacheException {
        final PubSubStats statistics = subStatsRef.get();
        assertNotNull(statistics);
        final int updateEvents = statistics.getUpdateEvents();
        assertEquals(totalPuts, updateEvents);
        assertEquals(totalUpdateEvents, updateEvents);
        assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, updateEvents);
        
        // assert that archive file contains same values as statistics
        File archive = new File(subArchive);
        assertTrue(archive.exists());

        StatArchiveReader reader = null;
        try {
          reader = new StatArchiveReader(new File[]{archive}, null, false);
        } catch (IOException e) {
          fail("Failed to read " + archive);
        }

        double combinedUpdateEvents = 0;
        
        @SuppressWarnings("rawtypes")
        List resources = reader.getResourceInstList();
        for (@SuppressWarnings("rawtypes")
        Iterator iter = resources.iterator(); iter.hasNext();) {
          StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) iter.next();
          if (!ri.getType().getName().equals(PubSubStats.TYPE_NAME)) {
            continue;
          }
          
          StatValue[] statValues = ri.getStatValues();
          for (int i = 0; i < statValues.length; i++) {
            String statName = ri.getType().getStats()[i].getName();
            assertNotNull(statName);
            
            if (statName.equals(PubSubStats.UPDATE_EVENTS)) {
              StatValue sv = statValues[i];
              sv.setFilter(StatValue.FILTER_NONE);
              
              double mostRecent = sv.getSnapshotsMostRecent();
              double min = sv.getSnapshotsMinimum();
              double max = sv.getSnapshotsMaximum();
              double maxMinusMin = sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum();
              double mean = sv.getSnapshotsAverage();
              double stdDev = sv.getSnapshotsStandardDeviation();
              
              assertEquals(mostRecent, max);

              double summation = 0;
              double[] rawSnapshots = sv.getRawSnapshots();
              for (int j = 0; j < rawSnapshots.length; j++) {
                //log.convertToLogWriter().info("DEBUG " + ri.getName() + " " + statName + " rawSnapshots[" + j + "] = " + rawSnapshots[j]);
                summation += rawSnapshots[j];
              }
              assertEquals(mean, summation / sv.getSnapshotsSize());
              
              combinedUpdateEvents += mostRecent;
            }
          }
        }
        assertEquals((double)totalUpdateEvents, combinedUpdateEvents);
      }
    });
    
    final int updateEvents = sub.invokeInt(getClass(), "readIntStat", 
        new Object[] {new File(subArchive), "PubSubStats", "updateEvents"});
    assertTrue(updateEvents > 0);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, updateEvents);
    
    int puts = 0;
    for (int pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      int vmPuts = (int)pubs[pubVM].invokeInt(getClass(), "readIntStat", 
          new Object[] {new File(pubArchives[pubVM]), "PubSubStats", "puts"});
      assertTrue(vmPuts > 0);
      assertEquals(MAX_PUTS * NUM_PUB_THREADS, vmPuts);
      puts += vmPuts;
    }
    assertTrue(puts > 0);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, puts);
    
    // use regex "testPubAndSubCustomStats"
    
    final MultipleArchiveReader reader = new MultipleArchiveReader(".*testPubAndSubCustomStats.*\\.gfs");

    final int combinedUpdateEvents = reader.readIntStat(PubSubStats.TYPE_NAME, PubSubStats.UPDATE_EVENTS);
    assertTrue("Failed to read updateEvents stat values", combinedUpdateEvents > 0);
    
    final int combinedPuts = reader.readIntStat(PubSubStats.TYPE_NAME, PubSubStats.PUTS);
    assertTrue("Failed to read puts stat values", combinedPuts > 0);
    
    assertTrue("updateEvents is " + combinedUpdateEvents + " but puts is " + combinedPuts, 
        combinedUpdateEvents == combinedPuts);
  }
  
  static int readIntStat(File archive, String typeName, String statName) throws IOException {
    final MultipleArchiveReader reader = new MultipleArchiveReader(archive);
    return reader.readIntStat(typeName, statName);
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length == 2) {
      final String statType = args[0];
      final String statName = args[1];
      
      final MultipleArchiveReader reader = new MultipleArchiveReader();
      final int value = reader.readIntStat(statType, statName);
      System.out.println(statType + "#" + statName + "=" + value);
      
    } else if (args.length == 3) {
      final String archiveName = args[0];
      final String statType = args[1];
      final String statName = args[2];
      
      final File archive = new File(archiveName).getAbsoluteFile();
      assertTrue("File " + archive + " does not exist!", archive.exists());
      assertTrue(archive + " exists but is not a file!", archive.isFile());
      
      final MultipleArchiveReader reader = new MultipleArchiveReader(archive);
      final int value = reader.readIntStat(statType, statName);
      System.out.println(archive + ": " + statType + "#" + statName + "=" + value);
      
    } else if (args.length == 4) {
      final String statType1 = args[0];
      final String statName1 = args[1];
      final String statType2 = args[2];
      final String statName2 = args[3];
      
      final MultipleArchiveReader reader = new MultipleArchiveReader();
      final int value1 = reader.readIntStat(statType1, statName1);
      final int value2 = reader.readIntStat(statType2, statName2);
      
      assertTrue(statType1 + "#" + statName1 + "=" + value1 
          + " does not equal " + statType2 + "#" + statName2 + "=" + value2,
          value1 == value2);
    } else {
      assertEquals("Miminum two args are required: statType statName", 2, args.length);
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   * @see cacheperf.CachePerfStats
   */
  static class PubSubStats {
    
    private static final String TYPE_NAME = "PubSubStats";
    private static final String TYPE_DESCRIPTION = "Statistics for StatisticsDUnitTest with Pub/Sub.";
    
    private static final String INSTANCE_PREFIX = "pubSubStats_";

    private static final String PUTS = "puts";
    private static final String PUT_TIME = "putTime";

    private static final String UPDATE_EVENTS = "updateEvents";

    private static StatisticsType createType(StatisticsFactory f) {
      StatisticsTypeFactory stf = StatisticsTypeFactoryImpl.singleton();
      StatisticsType type = stf.createType(
          TYPE_NAME, 
          TYPE_DESCRIPTION,
          createDescriptors(f));
      return type;
    }
    
    private static StatisticDescriptor[] createDescriptors(StatisticsFactory f) {
      boolean largerIsBetter = true;
      return new StatisticDescriptor[] {
        f.createIntCounter
          ( 
          PUTS,
          "Number of puts completed.",
          "operations",
          largerIsBetter
          ),
        f.createLongCounter
          ( 
          PUT_TIME,
          "Total time spent doing puts.",
          "nanoseconds",
          !largerIsBetter
          ),
        f.createIntCounter
          ( 
          UPDATE_EVENTS,
          "Number of update events.",
          "events",
          largerIsBetter
          )
      };
    }
    
    private final Statistics statistics;

    PubSubStats(StatisticsFactory f, String name, int id) {
      this.statistics = f.createAtomicStatistics(
          createType(f), INSTANCE_PREFIX + "_" + name, id);
    }
    
    Statistics statistics() {
      return this.statistics;
    }
    
    void close() {
      this.statistics.close();
    }
    
    int getUpdateEvents() {
      return statistics().getInt(UPDATE_EVENTS);
    }
    
    void incUpdateEvents() {
      incUpdateEvents(1);
    }
    
    void incUpdateEvents(int amount) {
      incStat(UPDATE_EVENTS, amount);
    }
    
    int getPuts() {
      return statistics().getInt(PUTS);
    }
    
    void incPuts() {
      incPuts(1);
    }
    
    void incPuts(int amount) {
      incStat(PUTS, amount);
    }
    
    void incPutTime(long amount) {
      incStat(PUT_TIME, amount);
    }
    
    long startPut() {
      return NanoTimer.getTime();
    }

    void endPut(long start) {
      endPut(start, 1);
    }
    
    void endPut(long start, int amount) {
      long elapsed = NanoTimer.getTime() - start;
      incPuts(amount);
      incPutTime(elapsed);
    }
    
    private void incStat(String statName, int intValue) {
      statistics().incInt(statName, intValue);
    }

    private void incStat(String statName, long longValue) {
      statistics().incLong(statName, longValue);
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  static class UpdateListener extends CacheListenerAdapter<String, Number> {
    
    private final PubSubStats statistics;
    
    UpdateListener(PubSubStats statistics) {
      this.statistics = statistics;
    }
    
    @Override
    public void afterUpdate(EntryEvent<String, Number> event) {
      this.statistics.incUpdateEvents( 1 );
    }
  }
  
  /**
   * @author Kirk Lund
   * @since 7.0
   */
  static class RegionMembershipListener extends RegionMembershipListenerAdapter<String, Number> {
    
    private final List<DistributedMember> members = new ArrayList<DistributedMember>();
    
    int size() {
      return this.members.size();
    }
    
    List<DistributedMember> getMembers() {
      return Collections.unmodifiableList(new ArrayList<DistributedMember>(this.members));
    }
    
    boolean containsId(DistributedMember member) {
      for (DistributedMember peer : getMembers()) {
        if (peer.getId().equals(member.getId())) {
          return true;
        }
      }
      return false;
    }
    
    boolean contains(DistributedMember member) {
      return this.members.contains(member);
    }

    String debugContains(DistributedMember member) {
      StringBuffer sb = new StringBuffer();
      for (DistributedMember peer : getMembers()) {
        if (!peer.equals(member)) {
          InternalDistributedMember peerIDM = (InternalDistributedMember)peer;
          InternalDistributedMember memberIDM = (InternalDistributedMember)member;
          sb.append("peer port=").append(peerIDM.getPort()).append(" ");
          sb.append("member port=").append(memberIDM.getPort()).append(" ");
        }
      }
      return sb.toString();
    }
    
    @Override
    public void initialMembers(Region<String, Number> region, DistributedMember[] initialMembers) {
      for (int i = 0; i < initialMembers.length; i++) {
        this.members.add(initialMembers[i]);
      }
    }
    
    @Override
    public void afterRemoteRegionCreate(RegionEvent<String, Number> event) {
      this.members.add(event.getDistributedMember());
    }
    
    @Override
    public void afterRemoteRegionDeparture(RegionEvent<String, Number> event) {
      this.members.remove(event.getDistributedMember());
    }
    
    @Override
    public void afterRemoteRegionCrash(RegionEvent<String, Number> event) {
      this.members.remove(event.getDistributedMember());
    }
  }

  static class MultipleArchiveReader {
    
    private final File dir;
    private final String regex;
    
    MultipleArchiveReader(File dir, String regex) {
      this.dir = dir;
      this.regex = regex;
    }
    
    MultipleArchiveReader(File dir) {
      this.dir = dir;
      this.regex = null;
    }
    
    MultipleArchiveReader(String regex) {
      this(new File(System.getProperty("user.dir")).getAbsoluteFile(), regex);
    }
    
    MultipleArchiveReader() {
      this(new File(System.getProperty("user.dir")).getAbsoluteFile(), null);
    }
    
    int readIntStat(String typeName, String statName) throws IOException {
      // directory (maybe directories) with one or more archives
      if (this.dir.exists() && this.dir.isDirectory()) {
        final List<File> archives = findFilesWithSuffix(this.dir, this.regex, ".gfs");
        return readIntStatFromArchives(archives, typeName, statName);
        
      // one archive file
      } else if (this.dir.exists() && this.dir.isFile()) {
        final List<File> archives = new ArrayList<File>();
        archives.add(this.dir);
        return readIntStatFromArchives(archives, typeName, statName);
        
      // failure
      } else {
        throw new IllegalStateException(this.dir + " does not exist!");
      }
    }

    private int readIntStatFromArchives(List<File> archives, String typeName, String statName) throws IOException {
      final StatValue[] statValues = readStatValues(archives, typeName, statName);
      assertNotNull("statValues is null!", statValues);
      assertTrue("statValues is empty!", statValues.length > 0);
      
      int value = 0;
      for (int i = 0; i < statValues.length; i++) {
        statValues[i].setFilter(StatValue.FILTER_NONE);
        value += (int)statValues[i].getSnapshotsMaximum();
      }
      return value;
    }
    
    private static List<File> findFilesWithSuffix(final File dir, final String regex, final String suffix) {
      Pattern p = null;
      if (regex != null) {
        p = Pattern.compile(regex);
      }
      final Pattern pattern = p;
      
      return findFiles(
          dir,
          new FileFilter() {
            public boolean accept(File file) {
              boolean accept = true;
              if (regex != null) {
                final Matcher matcher = pattern.matcher(file.getName());
                accept = matcher.matches();
              }
              if (suffix != null) {
                accept = accept && file.getName().endsWith(suffix);
              }
              return accept;
            }
          },
          true);
    }

    private static List<File> findFiles(File dir, FileFilter filter, boolean recursive) {
      File[] tmpfiles = dir.listFiles(filter);
      List<File> matches;
      if (tmpfiles == null) {
        matches = new ArrayList<File>();
      } else {
        matches = new ArrayList<File>(Arrays.asList(tmpfiles));
      }
      if (recursive) {
        File[] files = dir.listFiles();
        if (files != null) {
          for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.isDirectory()) {
              matches.addAll(findFiles(file, filter, recursive));
            }
          }
        }
      }
      return matches;
    }
    
    private static StatValue[] readStatValues(final List<File> archives, final String typeName, final String statName) throws IOException {
      System.out.println("\nKIRK readStatValues reading archives:\n\n" + archives + "\n");
      final StatSpec statSpec = new StatSpec() {
        @Override
        public boolean archiveMatches(File value) {
          return true;
        }
        @Override
        public boolean typeMatches(String value) {
          return typeName.equals(value);
        }
        @Override
        public boolean statMatches(String value) {
          return statName.equals(value);
        }
        @Override
        public boolean instanceMatches(String textId, long numericId) {
          return true;
        }
        @Override
        public int getCombineType() {
          return StatSpec.FILE;
        }
      };
  
      final File[] archiveFiles = archives.toArray(new File[archives.size()]);
      final StatSpec[] filters = new StatSpec[] { statSpec };
      final StatArchiveReader reader = new StatArchiveReader(archiveFiles, filters, true);
      final StatValue[] values = reader.matchSpec(statSpec);
      return values;
    }
  }
}
