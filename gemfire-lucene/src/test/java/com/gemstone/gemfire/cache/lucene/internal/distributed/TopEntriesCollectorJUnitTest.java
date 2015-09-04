package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TopEntriesCollectorJUnitTest {

  @Test
  public void testReduce() throws Exception {
    EntryScore r1_1 = new EntryScore("1-1", .9f);
    EntryScore r1_2 = new EntryScore("1-2", .7f);
    EntryScore r1_3 = new EntryScore("1-3", .5f);

    EntryScore r2_1 = new EntryScore("2-1", .85f);
    EntryScore r2_2 = new EntryScore("2-2", .65f);

    EntryScore r3_1 = new EntryScore("3-1", .8f);
    EntryScore r3_2 = new EntryScore("3-2", .6f);
    EntryScore r3_3 = new EntryScore("3-3", .4f);

    TopEntriesCollectorManager manager = new TopEntriesCollectorManager();

    TopEntriesCollector c1 = manager.newCollector("c1");
    c1.collect(r1_1.key, r1_1.score);
    c1.collect(r1_2.key, r1_2.score);
    c1.collect(r1_3.key, r1_3.score);

    TopEntriesCollector c2 = manager.newCollector("c2");
    c2.collect(r2_1.key, r2_1.score);
    c2.collect(r2_2.key, r2_2.score);

    TopEntriesCollector c3 = manager.newCollector("c3");
    c3.collect(r3_1.key, r3_1.score);
    c3.collect(r3_2.key, r3_2.score);
    c3.collect(r3_3.key, r3_3.score);

    List<IndexResultCollector> collectors = new ArrayList<>();
    collectors.add(c1);
    collectors.add(c2);
    collectors.add(c3);

    TopEntries entries = manager.reduce(collectors);
    assertEquals(8, entries.getHits().size());
    TopEntriesJUnitTest.verifyResultOrder(entries.getHits(), r1_1, r2_1, r3_1, r1_2, r2_2, r3_2, r1_3, r3_3);
  }
  
  @Test
  public void testInitialization() {
    TopEntriesCollector collector = new TopEntriesCollector("name");
    assertEquals("name", collector.getName());
    assertEquals(0, collector.size());
  }
}
