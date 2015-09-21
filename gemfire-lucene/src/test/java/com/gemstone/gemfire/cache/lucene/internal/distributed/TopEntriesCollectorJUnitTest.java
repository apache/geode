package com.gemstone.gemfire.cache.lucene.internal.distributed;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
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
    c1.collect(r1_1.getKey(), r1_1.getScore());
    c1.collect(r1_2.getKey(), r1_2.getScore());
    c1.collect(r1_3.getKey(), r1_3.getScore());

    TopEntriesCollector c2 = manager.newCollector("c2");
    c2.collect(r2_1.getKey(), r2_1.getScore());
    c2.collect(r2_2.getKey(), r2_2.getScore());

    TopEntriesCollector c3 = manager.newCollector("c3");
    c3.collect(r3_1.getKey(), r3_1.getScore());
    c3.collect(r3_2.getKey(), r3_2.getScore());
    c3.collect(r3_3.getKey(), r3_3.getScore());

    List<TopEntriesCollector> collectors = new ArrayList<>();
    collectors.add(c1);
    collectors.add(c2);
    collectors.add(c3);

    TopEntriesCollector entries = manager.reduce(collectors);
    assertEquals(8, entries.getEntries().getHits().size());
    TopEntriesJUnitTest.verifyResultOrder(entries.getEntries().getHits(), r1_1, r2_1, r3_1, r1_2, r2_2, r3_2, r1_3, r3_3);
  }
  
  @Test
  public void testInitialization() {
    TopEntriesCollector collector = new TopEntriesCollector("name");
    assertEquals("name", collector.getName());
    assertEquals(0, collector.size());
  }

  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    TopEntriesCollectorManager manager = new TopEntriesCollectorManager("id", 213);
    TopEntriesCollectorManager copy = CopyHelper.deepCopy(manager);
    assertEquals("id", copy.getId());
    assertEquals(213, copy.getLimit());
  }
  
  @Test
  public void testCollectorSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    TopEntriesCollector collector = new TopEntriesCollector("collector", 345);
    TopEntriesCollector copy = CopyHelper.deepCopy(collector);
    assertEquals("collector", copy.getName());
    assertEquals(345, copy.getEntries().getLimit());
  }
}
