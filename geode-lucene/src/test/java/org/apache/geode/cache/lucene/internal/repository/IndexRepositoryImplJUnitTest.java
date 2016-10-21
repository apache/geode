/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.repository;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.filesystem.ChunkKey;
import org.apache.geode.cache.lucene.internal.filesystem.File;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.Type2;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Test of the {@link IndexRepository} and everything below it. This tests that we can save gemfire
 * objects or PDXInstance objects into a lucene index and search for those objects later.
 */
@Category(IntegrationTest.class)
public class IndexRepositoryImplJUnitTest {

  private IndexRepositoryImpl repo;
  private HeterogeneousLuceneSerializer mapper;
  private StandardAnalyzer analyzer = new StandardAnalyzer();
  private IndexWriter writer;
  private Region region;
  private Region userRegion;
  private LuceneIndexStats stats;
  private FileSystemStats fileSystemStats;

  @Before
  public void setUp() throws IOException {
    ConcurrentHashMap<String, File> fileRegion = new ConcurrentHashMap<String, File>();
    ConcurrentHashMap<ChunkKey, byte[]> chunkRegion = new ConcurrentHashMap<ChunkKey, byte[]>();
    fileSystemStats = mock(FileSystemStats.class);
    RegionDirectory dir = new RegionDirectory(fileRegion, chunkRegion, fileSystemStats);
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    writer = new IndexWriter(dir, config);
    String[] indexedFields = new String[] {"s", "i", "l", "d", "f", "s2", "missing"};
    mapper = new HeterogeneousLuceneSerializer(indexedFields);
    region = Mockito.mock(Region.class);
    userRegion = Mockito.mock(Region.class);
    stats = Mockito.mock(LuceneIndexStats.class);
    Mockito.when(userRegion.isDestroyed()).thenReturn(false);
    repo = new IndexRepositoryImpl(region, writer, mapper, stats, userRegion);
  }

  @Test
  public void testAddDocs() throws IOException, ParseException {
    repo.create("key1", new Type2("bacon maple bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    repo.create("key2",
        new Type2("McMinnville Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.create("key3",
        new Type2("Voodoo Doll doughnut", 1, 2L, 3.0, 4.0f, "Toasted coconut doughnut"));
    repo.create("key4",
        new Type2("Portland Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.commit();

    checkQuery("Cream", "s", "key2", "key4");
    checkQuery("NotARealWord", "s");
  }

  @Test
  public void testEmptyRepo() throws IOException, ParseException {
    checkQuery("NotARealWord", "s");
  }

  @Test
  public void testUpdateAndRemoveStringKeys() throws IOException, ParseException {
    updateAndRemove("key1", "key2", "key3", "key4");
  }

  @Test
  public void testUpdateAndRemoveBinaryKeys() throws IOException, ParseException {

    ByteWrapper key1 = randomKey();
    ByteWrapper key2 = randomKey();
    ByteWrapper key3 = randomKey();
    ByteWrapper key4 = randomKey();

    updateAndRemove(key1, key2, key3, key4);
  }

  @Test
  public void createShouldUpdateStats() throws IOException {
    repo.create("key1", new Type2("bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    verify(stats, times(1)).startUpdate();
    verify(stats, times(1)).endUpdate(anyLong());
  }

  @Test
  public void updateShouldUpdateStats() throws IOException {
    repo.update("key1", new Type2("bacon maple bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    verify(stats, times(1)).startUpdate();
    verify(stats, times(1)).endUpdate(anyLong());
  }

  @Test
  public void deleteShouldUpdateStats() throws IOException {
    repo.delete("key1");
    verify(stats, times(1)).startUpdate();
    verify(stats, times(1)).endUpdate(anyLong());
  }

  @Test
  public void commitShouldUpdateStats() throws IOException {
    repo.commit();
    verify(stats, times(1)).startCommit();
    verify(stats, times(1)).endCommit(anyLong());
  }

  @Test
  public void queryShouldUpdateStats() throws IOException, ParseException {
    repo.create("key2",
        new Type2("McMinnville Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.create("key4",
        new Type2("Portland Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.commit();
    checkQuery("Cream", "s", "key2", "key4");
    verify(stats, times(1)).startQuery();
    verify(stats, times(1)).endQuery(anyLong(), eq(2));
  }

  @Test
  public void addingDocumentsShouldUpdateDocumentsStat() throws IOException {
    repo.create("key1", new Type2("bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    repo.commit();
    ArgumentCaptor<IntSupplier> captor = ArgumentCaptor.forClass(IntSupplier.class);
    verify(stats).addDocumentsSupplier(captor.capture());
    IntSupplier supplier = captor.getValue();
    assertEquals(1, supplier.getAsInt());
  }

  @Test
  public void cleanupShouldCloseWriter() throws IOException {
    repo.cleanup();
    verify(stats).removeDocumentsSupplier(any());
    assertFalse(writer.isOpen());
  }

  private void updateAndRemove(Object key1, Object key2, Object key3, Object key4)
      throws IOException, ParseException {
    repo.create(key1, new Type2("bacon maple bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    repo.create(key2,
        new Type2("McMinnville Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.create(key3,
        new Type2("Voodoo Doll doughnut", 1, 2L, 3.0, 4.0f, "Toasted coconut doughnut"));
    repo.create(key4,
        new Type2("Portland Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.commit();

    repo.update(key3, new Type2("Boston Cream Pie", 1, 2L, 3.0, 4.0f, "Toasted coconut doughnut"));
    repo.delete(key4);
    repo.commit();

    // BooleanQuery q = new BooleanQuery();
    // q.add(new TermQuery(SerializerUtil.toKeyTerm("key3")), Occur.MUST_NOT);
    // writer.deleteDocuments(q);
    // writer.commit();

    // Make sure the updates and deletes were applied
    checkQuery("doughnut", "s", key2);
    checkQuery("Cream", "s", key2, key3);
  }

  private ByteWrapper randomKey() {
    Random rand = new Random();
    int size = rand.nextInt(2048) + 50;
    byte[] key = new byte[size];
    rand.nextBytes(key);
    return new ByteWrapper(key);
  }

  private void checkQuery(String queryTerm, String queryField, Object... expectedKeys)
      throws IOException, ParseException {
    Set<Object> expectedSet = new HashSet<Object>();
    expectedSet.addAll(Arrays.asList(expectedKeys));

    QueryParser parser = new QueryParser(queryField, analyzer);

    KeyCollector collector = new KeyCollector();
    repo.query(parser.parse(queryTerm), 100, collector);

    assertEquals(expectedSet, collector.results);
  }

  private static class KeyCollector implements IndexResultCollector {

    Set<Object> results = new HashSet<Object>();

    @Override
    public void collect(Object key, float score) {
      results.add(key);
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public int size() {
      return results.size();
    }
  }

  /**
   * A wrapper around a byte array that implements equals, for comparison checks.
   */
  private static class ByteWrapper implements Serializable {
    private byte[] bytes;


    public ByteWrapper(byte[] bytes) {
      super();
      this.bytes = bytes;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(bytes);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ByteWrapper other = (ByteWrapper) obj;
      if (!Arrays.equals(bytes, other.bytes))
        return false;
      return true;
    }
  }

}
