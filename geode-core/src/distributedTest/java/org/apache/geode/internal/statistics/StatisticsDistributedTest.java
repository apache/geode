/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.statistics;

import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

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

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.RegionMembershipListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.internal.statistics.StatArchiveReader.StatSpec;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.StatisticsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Distributed tests for {@link Statistics}.
 *
 * <p>
 * VM0 performs puts and VM1 receives updates. Both use custom statistics for start/end with
 * increment to add up puts and updates. Then validation tests values in stat resource instances and
 * uses StatArchiveReader. Both are tested against static counters in both VMs.
 *
 * <p>
 * This test mimics hydratest/locators/cacheDS.conf in an attempt to reproduce bug #45478. So far
 * this test passes consistently.
 *
 * @since GemFire 7.0
 */
@Category({StatisticsTest.class})
@SuppressWarnings({"rawtypes", "serial", "unused"})
public class StatisticsDistributedTest extends JUnit4CacheTestCase {

  private static final int MAX_PUTS = 1000;
  private static final int NUM_KEYS = 100;
  private static final int NUM_PUB_THREADS = 2;
  private static final int NUM_PUBS = 2;
  private static final boolean RANDOMIZE_PUTS = true;

  private static AtomicInteger updateEvents = new AtomicInteger();
  private static AtomicInteger puts = new AtomicInteger();
  private static AtomicReference<PubSubStats> subStatsRef = new AtomicReference<>();
  private static AtomicReferenceArray<PubSubStats> pubStatsRef =
      new AtomicReferenceArray<>(NUM_PUB_THREADS);
  private static AtomicReference<RegionMembershipListener> rmlRef = new AtomicReference<>();

  private File directory;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Override
  public final void postSetUp() throws Exception {
    this.directory = this.temporaryFolder.getRoot();
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    invokeInEveryVM(() -> cleanup());
    disconnectAllFromDS(); // because this test enabled stat sampling!
  }

  @Test
  public void testPubAndSubCustomStats() throws Exception {
    String regionName = "region_" + getName();
    VM[] pubs = new VM[NUM_PUBS];
    for (int pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      pubs[pubVM] = getHost(0).getVM(pubVM);
    }
    VM sub = getHost(0).getVM(NUM_PUBS);

    for (VM pub : pubs) {
      pub.invoke(() -> puts.set(0));
    }

    String subArchive =
        this.directory.getAbsolutePath() + File.separator + getName() + "_sub" + ".gfs";
    String[] pubArchives = new String[NUM_PUBS];
    for (int pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      pubArchives[pubVM] =
          this.directory.getAbsolutePath() + File.separator + getName() + "_pub-" + pubVM + ".gfs";
    }

    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubVM = i;
      pubs[pubVM].invoke("pub-connect-and-create-data-" + pubVM, () -> {
        Properties props = new Properties();
        props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
        props.setProperty(STATISTIC_SAMPLE_RATE, "1000");
        props.setProperty(STATISTIC_ARCHIVE_FILE, pubArchives[pubVM]);

        InternalDistributedSystem system = getSystem(props);

        // assert that sampler is working as expected
        GemFireStatSampler sampler = system.getStatSampler();
        assertTrue(sampler.isSamplingEnabled());
        assertTrue(sampler.isAlive());
        assertEquals(new File(pubArchives[pubVM]), sampler.getArchiveFileName());

        await("awaiting SampleCollector to exist")
            .until(() -> sampler.getSampleCollector() != null);

        SampleCollector sampleCollector = sampler.getSampleCollector();
        assertNotNull(sampleCollector);

        StatArchiveHandler archiveHandler = sampleCollector.getStatArchiveHandler();
        assertNotNull(archiveHandler);
        assertTrue(archiveHandler.isArchiving());

        // create cache and region
        Cache cache = getCache();
        RegionFactory<String, Number> factory = cache.createRegionFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);

        RegionMembershipListener rml = new RegionMembershipListener();
        rmlRef.set(rml);
        factory.addCacheListener(rml);
        Region<String, Number> region = factory.create(regionName);

        // create the keys
        if (region.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
          for (int key = 0; key < NUM_KEYS; key++) {
            region.create("KEY-" + key, null);
          }
        }
      });
    }

    DistributedMember subMember = sub.invoke("sub-connect-and-create-keys", () -> {
      Properties props = new Properties();
      props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
      props.setProperty(STATISTIC_SAMPLE_RATE, "1000");
      props.setProperty(STATISTIC_ARCHIVE_FILE, subArchive);

      InternalDistributedSystem system = getSystem(props);

      PubSubStats statistics = new PubSubStats(system, "sub-1", 1);
      subStatsRef.set(statistics);

      // assert that sampler is working as expected
      GemFireStatSampler sampler = system.getStatSampler();
      assertTrue(sampler.isSamplingEnabled());
      assertTrue(sampler.isAlive());
      assertEquals(new File(subArchive), sampler.getArchiveFileName());

      await("awaiting SampleCollector to exist")
          .until(() -> sampler.getSampleCollector() != null);

      SampleCollector sampleCollector = sampler.getSampleCollector();
      assertNotNull(sampleCollector);

      StatArchiveHandler archiveHandler = sampleCollector.getStatArchiveHandler();
      assertNotNull(archiveHandler);
      assertTrue(archiveHandler.isArchiving());

      // create cache and region with UpdateListener
      Cache cache = getCache();
      RegionFactory<String, Number> factory = cache.createRegionFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);

      CacheListener<String, Number> cl = new UpdateListener(statistics);
      factory.addCacheListener(cl);
      Region<String, Number> region = factory.create(regionName);

      // create the keys
      if (region.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
        for (int key = 0; key < NUM_KEYS; key++) {
          region.create("KEY-" + key, null);
        }
      }

      assertEquals(0, statistics.getUpdateEvents());
      return system.getDistributedMember();
    });

    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubVM = i;
      AsyncInvocation[] publishers = new AsyncInvocation[NUM_PUB_THREADS];
      for (int j = 0; j < NUM_PUB_THREADS; j++) {
        final int pubThread = j;
        publishers[pubThread] = pubs[pubVM]
            .invokeAsync("pub-connect-and-put-data-" + pubVM + "-thread-" + pubThread, () -> {
              PubSubStats statistics = new PubSubStats(basicGetSystem(), "pub-" + pubThread, pubVM);
              pubStatsRef.set(pubThread, statistics);

              RegionMembershipListener rml = rmlRef.get();
              Region<String, Number> region = getCache().getRegion(regionName);

              // assert that sub is in rml membership
              assertNotNull(rml);

              await("awaiting Membership to contain subMember")
                  .until(() -> rml.contains(subMember) && rml.size() == NUM_PUBS);

              // publish lots of puts cycling through the NUM_KEYS
              assertEquals(0, statistics.getPuts());

              // cycle through the keys randomly
              if (RANDOMIZE_PUTS) {
                Random randomGenerator = new Random();
                int key = 0;
                for (int idx = 0; idx < MAX_PUTS; idx++) {
                  long start = statistics.startPut();
                  key = randomGenerator.nextInt(NUM_KEYS);
                  region.put("KEY-" + key, idx);
                  statistics.endPut(start);
                }

                // cycle through the keys in order and wrapping back around
              } else {
                int key = 0;
                for (int idx = 0; idx < MAX_PUTS; idx++) {
                  long start = statistics.startPut();
                  region.put("KEY-" + key, idx);
                  key++; // cycle through the keys...
                  if (key >= NUM_KEYS) {
                    key = 0;
                  }
                  statistics.endPut(start);
                }
              }
              assertEquals(MAX_PUTS, statistics.getPuts());

              // wait for 2 samples to ensure all stats have been archived
              StatisticsType statSamplerType = getSystem().findType("StatSampler");
              Statistics[] statsArray = getSystem().findStatisticsByType(statSamplerType);
              assertEquals(1, statsArray.length);

              Statistics statSamplerStats = statsArray[0];
              int initialSampleCount = statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT);

              await("awaiting sampleCount >= 2").until(() -> statSamplerStats
                  .getInt(StatSamplerStats.SAMPLE_COUNT) >= initialSampleCount + 2);
            });
      }

      for (int pubThread = 0; pubThread < publishers.length; pubThread++) {
        publishers[pubThread].join();
        if (publishers[pubThread].exceptionOccurred()) {
          fail("Test failed", publishers[pubThread].getException());
        }
      }
    }

    sub.invoke("sub-wait-for-samples", () -> {
      // wait for 2 samples to ensure all stats have been archived
      StatisticsType statSamplerType = getSystem().findType("StatSampler");
      Statistics[] statsArray = getSystem().findStatisticsByType(statSamplerType);
      assertEquals(1, statsArray.length);

      Statistics statSamplerStats = statsArray[0];
      int initialSampleCount = statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT);

      await("awaiting sampleCount >= 2").until(
          () -> statSamplerStats.getInt(StatSamplerStats.SAMPLE_COUNT) >= initialSampleCount + 2);

      // now post total updateEvents to static
      PubSubStats statistics = subStatsRef.get();
      assertNotNull(statistics);
      updateEvents.set(statistics.getUpdateEvents());
    });

    // validate pub values against sub values
    int totalUpdateEvents = sub.invoke(() -> getUpdateEvents());

    // validate pub values against pub statistics against pub archive
    for (int i = 0; i < NUM_PUBS; i++) {
      final int pubIdx = i;
      pubs[pubIdx].invoke("pub-validation", () -> {
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

        StatArchiveReader reader = new StatArchiveReader(new File[] {archive}, null, false);

        double combinedPuts = 0;

        List resources = reader.getResourceInstList();
        assertNotNull(resources);
        assertFalse(resources.isEmpty());

        for (Iterator<ResourceInst> iter = resources.iterator(); iter.hasNext();) {
          ResourceInst ri = iter.next();
          if (!ri.getType().getName().equals(PubSubStats.TYPE_NAME)) {
            continue;
          }

          StatValue[] statValues = ri.getStatValues();
          for (int idx = 0; idx < statValues.length; idx++) {
            String statName = ri.getType().getStats()[idx].getName();
            assertNotNull(statName);

            if (statName.equals(PubSubStats.PUTS)) {
              StatValue sv = statValues[idx];
              sv.setFilter(StatValue.FILTER_NONE);

              double mostRecent = sv.getSnapshotsMostRecent();
              double min = sv.getSnapshotsMinimum();
              double max = sv.getSnapshotsMaximum();
              double maxMinusMin = sv.getSnapshotsMaximum() - sv.getSnapshotsMinimum();
              double mean = sv.getSnapshotsAverage();
              double stdDev = sv.getSnapshotsStandardDeviation();

              assertEquals(mostRecent, max, 0f);

              double summation = 0;
              double[] rawSnapshots = sv.getRawSnapshots();
              for (int j = 0; j < rawSnapshots.length; j++) {
                summation += rawSnapshots[j];
              }
              assertEquals(mean, summation / sv.getSnapshotsSize(), 0);

              combinedPuts += mostRecent;
            }
          }
        }

        // assert that sum of mostRecent values for all puts equals totalPuts
        assertEquals((double) totalPuts, combinedPuts, 0);
        puts.getAndAdd(totalPuts);
      });
    }

    // validate pub values against sub values
    int totalCombinedPuts = 0;
    for (int i = 0; i < NUM_PUBS; i++) {
      int pubIdx = i;
      int totalPuts = pubs[pubIdx].invoke(() -> getPuts());
      assertEquals(MAX_PUTS * NUM_PUB_THREADS, totalPuts);
      totalCombinedPuts += totalPuts;
    }
    assertEquals(totalCombinedPuts, totalUpdateEvents);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, totalCombinedPuts);

    // validate sub values against sub statistics against sub archive
    final int totalPuts = totalCombinedPuts;
    sub.invoke("sub-validation", () -> {
      PubSubStats statistics = subStatsRef.get();
      assertNotNull(statistics);
      int updateEvents = statistics.getUpdateEvents();
      assertEquals(totalPuts, updateEvents);
      assertEquals(totalUpdateEvents, updateEvents);
      assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, updateEvents);

      // assert that archive file contains same values as statistics
      File archive = new File(subArchive);
      assertTrue(archive.exists());

      StatArchiveReader reader = new StatArchiveReader(new File[] {archive}, null, false);

      double combinedUpdateEvents = 0;

      List resources = reader.getResourceInstList();
      for (Iterator<ResourceInst> iter = resources.iterator(); iter.hasNext();) {
        ResourceInst ri = iter.next();
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

            assertEquals(mostRecent, max, 0);

            double summation = 0;
            double[] rawSnapshots = sv.getRawSnapshots();
            for (int j = 0; j < rawSnapshots.length; j++) {
              summation += rawSnapshots[j];
            }
            assertEquals(mean, summation / sv.getSnapshotsSize(), 0);

            combinedUpdateEvents += mostRecent;
          }
        }
      }
      assertEquals((double) totalUpdateEvents, combinedUpdateEvents, 0);
    });

    int updateEvents =
        sub.invoke(() -> readIntStat(new File(subArchive), "PubSubStats", "updateEvents"));
    assertTrue(updateEvents > 0);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, updateEvents);

    int puts = 0;
    for (int pubVM = 0; pubVM < NUM_PUBS; pubVM++) {
      int currentPubVM = pubVM;
      int vmPuts = pubs[pubVM]
          .invoke(() -> readIntStat(new File(pubArchives[currentPubVM]), "PubSubStats", "puts"));
      assertTrue(vmPuts > 0);
      assertEquals(MAX_PUTS * NUM_PUB_THREADS, vmPuts);
      puts += vmPuts;
    }
    assertTrue(puts > 0);
    assertEquals(MAX_PUTS * NUM_PUB_THREADS * NUM_PUBS, puts);

    // use regex "testPubAndSubCustomStats"

    MultipleArchiveReader reader =
        new MultipleArchiveReader(this.directory, ".*" + getTestMethodName() + ".*\\.gfs");

    int combinedUpdateEvents = reader.readIntStat(PubSubStats.TYPE_NAME, PubSubStats.UPDATE_EVENTS);
    assertTrue("Failed to read updateEvents stat values", combinedUpdateEvents > 0);

    int combinedPuts = reader.readIntStat(PubSubStats.TYPE_NAME, PubSubStats.PUTS);
    assertTrue("Failed to read puts stat values", combinedPuts > 0);

    assertTrue("updateEvents is " + combinedUpdateEvents + " but puts is " + combinedPuts,
        combinedUpdateEvents == combinedPuts);
  }

  static int readIntStat(final File archive, final String typeName, final String statName)
      throws IOException {
    MultipleArchiveReader reader = new MultipleArchiveReader(archive);
    return reader.readIntStat(typeName, statName);
  }

  /** invoked by reflection */
  private static void cleanup() {
    updateEvents.set(0);
    rmlRef.set(null);
  }

  /** invoked by reflection */
  private static int getUpdateEvents() {
    return updateEvents.get();
  }

  /** invoked by reflection */
  private static int getPuts() {
    return puts.get();
  }

  public static void main(final String[] args) throws Exception {
    if (args.length == 2) {
      final String statType = args[0];
      final String statName = args[1];

      MultipleArchiveReader reader = new MultipleArchiveReader(new File("."));
      int value = reader.readIntStat(statType, statName);
      System.out.println(statType + "#" + statName + "=" + value);

    } else if (args.length == 3) {
      final String archiveName = args[0];
      final String statType = args[1];
      final String statName = args[2];

      File archive = new File(archiveName).getAbsoluteFile();
      assertTrue("File " + archive + " does not exist!", archive.exists());
      assertTrue(archive + " exists but is not a file!", archive.isFile());

      MultipleArchiveReader reader = new MultipleArchiveReader(archive);
      int value = reader.readIntStat(statType, statName);
      System.out.println(archive + ": " + statType + "#" + statName + "=" + value);

    } else if (args.length == 4) {
      final String statType1 = args[0];
      final String statName1 = args[1];
      final String statType2 = args[2];
      final String statName2 = args[3];

      MultipleArchiveReader reader = new MultipleArchiveReader(new File("."));
      int value1 = reader.readIntStat(statType1, statName1);
      int value2 = reader.readIntStat(statType2, statName2);

      assertTrue(statType1 + "#" + statName1 + "=" + value1 + " does not equal " + statType2 + "#"
          + statName2 + "=" + value2, value1 == value2);
    } else {
      assertEquals("Minimum two args are required: statType statName", 2, args.length);
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class PubSubStats {

    private static final String TYPE_NAME = "PubSubStats";
    private static final String TYPE_DESCRIPTION =
        "Statistics for StatisticsDistributedTest with Pub/Sub.";

    private static final String INSTANCE_PREFIX = "pubSubStats_";

    private static final String PUTS = "puts";
    private static final String PUT_TIME = "putTime";

    private static final String UPDATE_EVENTS = "updateEvents";

    private static StatisticsType createType(final StatisticsFactory f) {
      StatisticsTypeFactory stf = StatisticsTypeFactoryImpl.singleton();
      StatisticsType type = stf.createType(TYPE_NAME, TYPE_DESCRIPTION, createDescriptors(f));
      return type;
    }

    private static StatisticDescriptor[] createDescriptors(final StatisticsFactory f) {
      boolean largerIsBetter = true;
      return new StatisticDescriptor[] {
          f.createIntCounter(PUTS, "Number of puts completed.", "operations", largerIsBetter),
          f.createLongCounter(PUT_TIME, "Total time spent doing puts.", "nanoseconds",
              !largerIsBetter),
          f.createIntCounter(UPDATE_EVENTS, "Number of update events.", "events", largerIsBetter)};
    }

    private final Statistics statistics;

    PubSubStats(final StatisticsFactory f, final String name, final int id) {
      this.statistics = f.createAtomicStatistics(createType(f), INSTANCE_PREFIX + "_" + name, id);
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

    void incUpdateEvents(final int amount) {
      incStat(UPDATE_EVENTS, amount);
    }

    int getPuts() {
      return statistics().getInt(PUTS);
    }

    void incPuts() {
      incPuts(1);
    }

    void incPuts(final int amount) {
      incStat(PUTS, amount);
    }

    void incPutTime(final long amount) {
      incStat(PUT_TIME, amount);
    }

    long startPut() {
      return NanoTimer.getTime();
    }

    void endPut(final long start) {
      endPut(start, 1);
    }

    void endPut(final long start, final int amount) {
      long elapsed = NanoTimer.getTime() - start;
      incPuts(amount);
      incPutTime(elapsed);
    }

    private void incStat(final String statName, final int intValue) {
      statistics().incInt(statName, intValue);
    }

    private void incStat(final String statName, final long longValue) {
      statistics().incLong(statName, longValue);
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class UpdateListener extends CacheListenerAdapter<String, Number> {

    private final PubSubStats statistics;

    UpdateListener(final PubSubStats statistics) {
      this.statistics = statistics;
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Number> event) {
      this.statistics.incUpdateEvents(1);
    }
  }

  /**
   * @since GemFire 7.0
   */
  static class RegionMembershipListener extends RegionMembershipListenerAdapter<String, Number> {

    private final List<DistributedMember> members = new ArrayList<>();

    int size() {
      return this.members.size();
    }

    List<DistributedMember> getMembers() {
      return Collections.unmodifiableList(new ArrayList<>(this.members));
    }

    boolean containsId(final DistributedMember member) {
      for (DistributedMember peer : getMembers()) {
        if (peer.getId().equals(member.getId())) {
          return true;
        }
      }
      return false;
    }

    boolean contains(final DistributedMember member) {
      return this.members.contains(member);
    }

    String debugContains(final DistributedMember member) {
      StringBuilder sb = new StringBuilder();
      for (DistributedMember peer : getMembers()) {
        if (!peer.equals(member)) {
          InternalDistributedMember peerIDM = (InternalDistributedMember) peer;
          InternalDistributedMember memberIDM = (InternalDistributedMember) member;
          sb.append("peer port=").append(peerIDM.getPort()).append(" ");
          sb.append("member port=").append(memberIDM.getPort()).append(" ");
        }
      }
      return sb.toString();
    }

    @Override
    public void initialMembers(final Region<String, Number> region,
        final DistributedMember[] initialMembers) {
      for (int i = 0; i < initialMembers.length; i++) {
        this.members.add(initialMembers[i]);
      }
    }

    @Override
    public void afterRemoteRegionCreate(final RegionEvent<String, Number> event) {
      this.members.add(event.getDistributedMember());
    }

    @Override
    public void afterRemoteRegionDeparture(final RegionEvent<String, Number> event) {
      this.members.remove(event.getDistributedMember());
    }

    @Override
    public void afterRemoteRegionCrash(final RegionEvent<String, Number> event) {
      this.members.remove(event.getDistributedMember());
    }
  }

  static class MultipleArchiveReader {

    private final File dir;
    private final String regex;

    MultipleArchiveReader(final File dir, final String regex) {
      this.dir = dir;
      this.regex = regex;
    }

    MultipleArchiveReader(final File dir) {
      this.dir = dir;
      this.regex = null;
    }

    int readIntStat(final String typeName, final String statName) throws IOException {
      // directory (maybe directories) with one or more archives
      if (this.dir.exists() && this.dir.isDirectory()) {
        List<File> archives = findFilesWithSuffix(this.dir, this.regex, ".gfs");
        return readIntStatFromArchives(archives, typeName, statName);

        // one archive file
      } else if (this.dir.exists() && this.dir.isFile()) {
        List<File> archives = new ArrayList<File>();
        archives.add(this.dir);
        return readIntStatFromArchives(archives, typeName, statName);

        // failure
      } else {
        throw new IllegalStateException(this.dir + " does not exist!");
      }
    }

    private int readIntStatFromArchives(final List<File> archives, final String typeName,
        final String statName) throws IOException {
      StatValue[] statValues = readStatValues(archives, typeName, statName);
      assertNotNull("statValues is null!", statValues);
      assertTrue("statValues is empty!", statValues.length > 0);

      int value = 0;
      for (int i = 0; i < statValues.length; i++) {
        statValues[i].setFilter(StatValue.FILTER_NONE);
        value += (int) statValues[i].getSnapshotsMaximum();
      }
      return value;
    }

    private static List<File> findFilesWithSuffix(final File dir, final String regex,
        final String suffix) {
      Pattern p = null;
      if (regex != null) {
        p = Pattern.compile(regex);
      }
      final Pattern pattern = p;

      return findFiles(dir, (final File file) -> {
        boolean value = true;
        if (regex != null) {
          final Matcher matcher = pattern.matcher(file.getName());
          value = matcher.matches();
        }
        if (suffix != null) {
          value = value && file.getName().endsWith(suffix);
        }
        return value;
      }, true);
    }

    private static List<File> findFiles(final File dir, final FileFilter filter,
        final boolean recursive) {
      File[] tmpfiles = dir.listFiles(filter);
      List<File> matches;
      if (tmpfiles == null) {
        matches = new ArrayList<>();
      } else {
        matches = new ArrayList<>(Arrays.asList(tmpfiles));
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

    private static StatValue[] readStatValues(final List<File> archives, final String typeName,
        final String statName) throws IOException {
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

      File[] archiveFiles = archives.toArray(new File[archives.size()]);
      StatSpec[] filters = new StatSpec[] {statSpec};
      StatArchiveReader reader = new StatArchiveReader(archiveFiles, filters, true);
      StatValue[] values = reader.matchSpec(statSpec);
      return values;
    }
  }
}
