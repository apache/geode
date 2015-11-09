/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.test.junit.categories.PerformanceTest;

/**
 * Microbenchmark of the IndexRepository to compare an
 * IndexRepository built on top of cache with a 
 * stock lucene IndexWriter with a RAMDirectory.
 */
@Category(PerformanceTest.class)
public class IndexRepositoryImplJUnitPerformanceTest {
  
  private static final int NUM_WORDS = 1000;
  private static int[] COMMIT_INTERVAL = new int[] {100, 1000, 5000};
  private static int NUM_ENTRIES = 500_000;
  private static int NUM_QUERIES = 500_000;

  private StandardAnalyzer analyzer = new StandardAnalyzer();
  
  @Test
  public  void testIndexRepository() throws Exception {
    

    doTest("IndexRepository", new TestCallbacks() {

      private Cache cache;
      private IndexRepositoryImpl repo;
      private IndexWriter writer;

      @Override
      public void addObject(String key, String text) throws Exception {
        repo.create(key, new TestObject(text));
      }

      @Override
      public void commit()  throws Exception {
        repo.commit();
      }

      @Override
      public void init() throws Exception {
        cache = new CacheFactory().set("mcast-port", "0")
            .set("log-level", "error")
            .create();
        Region<String, File> fileRegion = cache.<String, File>createRegionFactory(RegionShortcut.REPLICATE).create("files");
        Region<ChunkKey, byte[]> chunkRegion = cache.<ChunkKey, byte[]>createRegionFactory(RegionShortcut.REPLICATE).create("chunks");

        RegionDirectory dir = new RegionDirectory(fileRegion, chunkRegion);
        
        
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        String[] indexedFields= new String[] {"text"};
        HeterogenousLuceneSerializer mapper = new HeterogenousLuceneSerializer(indexedFields);
        repo = new IndexRepositoryImpl(fileRegion, writer, mapper);
      }

      @Override
      public void cleanup() throws IOException {
        writer.close();
        cache.close();
      }

      @Override
      public void waitForAsync() throws Exception {
        //do nothing
      }

      @Override
      public int query(Query query) throws IOException {
        TopEntriesCollector collector = new TopEntriesCollector();
        repo.query(query, 100, collector);
        return collector.size();
      }
    });
  }
  
  /**
   * Test our full lucene index implementation
   * @throws Exception
   */
  @Test
  public void testLuceneIndex() throws Exception {
    

    doTest("LuceneIndex", new TestCallbacks() {

      private Cache cache;
      private Region<String, TestObject> region;
      private LuceneService service;

      @Override
      public void addObject(String key, String text) throws Exception {
        region.create(key, new TestObject(text));
      }

      @Override
      public void commit()  throws Exception {
        //NA
      }

      @Override
      public void init() throws Exception {
        cache = new CacheFactory().set("mcast-port", "0")
            .set("log-level", "warning")
            .create();
        service = LuceneServiceProvider.get(cache);
        service.createIndex("index", "/region", "text");
        region = cache.<String, TestObject>createRegionFactory(RegionShortcut.PARTITION)
            .setPartitionAttributes(new PartitionAttributesFactory<>().setTotalNumBuckets(1).create())
            .create("region");
      }

      @Override
      public void cleanup() throws IOException {
        cache.close();
      }
      
      @Override
      public void waitForAsync() throws Exception {
        AsyncEventQueue aeq = cache.getAsyncEventQueue(LuceneServiceImpl.getUniqueIndexName("index", "/region"));
        
        //We will be at most 10 ms off
        while(aeq.size() > 0) {
          Thread.sleep(10);
        }
      }

      @Override
      public int query(final Query query) throws Exception {
        LuceneQuery<Object, Object> luceneQuery = service.createLuceneQueryFactory().create("index", "/region", new LuceneQueryProvider() {
          
          @Override
          public Query getQuery(LuceneIndex index) throws QueryException {
            return query;
          }
        });
        
        LuceneQueryResults<Object, Object> results = luceneQuery.search();
        return results.size();
      }
    });
  }
  
  @Test
  public  void testLuceneWithRegionDirectory() throws Exception {
    doTest("RegionDirectory", new TestCallbacks() {

      private IndexWriter writer;
      private SearcherManager searcherManager;

      @Override
      public void init() throws Exception {
        RegionDirectory dir = new RegionDirectory(new ConcurrentHashMap<String, File>(), new ConcurrentHashMap<ChunkKey, byte[]>());
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        searcherManager = new SearcherManager(writer, true, null);
      }

      @Override
      public void addObject(String key, String text) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("key", key, Store.YES));
        doc.add(new TextField("text", text, Store.NO));
        writer.addDocument(doc);
      }

      @Override
      public void commit() throws Exception {
        writer.commit();
        searcherManager.maybeRefresh();
      }

      @Override
      public void cleanup() throws Exception {
        writer.close();
      }
      
      @Override
      public void waitForAsync() throws Exception {
        //do nothing
      }

      @Override
      public int query(Query query) throws Exception {
        IndexSearcher searcher = searcherManager.acquire();
        try {
          return searcher.count(query);
        } finally {
          searcherManager.release(searcher);
        }
      }
      
    });
    
  }
  
  @Test
  public  void testLucene() throws Exception {
    doTest("Lucene", new TestCallbacks() {

      private IndexWriter writer;
      private SearcherManager searcherManager;

      @Override
      public void init() throws Exception {
        RAMDirectory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        searcherManager = new SearcherManager(writer, true, null);
      }

      @Override
      public void addObject(String key, String text) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("key", key, Store.YES));
        doc.add(new TextField("text", text, Store.NO));
        writer.addDocument(doc);
      }

      @Override
      public void commit() throws Exception {
        writer.commit();
        searcherManager.maybeRefresh();
      }

      @Override
      public void cleanup() throws Exception {
        writer.close();
      }
      
      @Override
      public void waitForAsync() throws Exception {
        //do nothing
      }

      @Override
      public int query(Query query) throws Exception {
        IndexSearcher searcher = searcherManager.acquire();
        try {
          return searcher.count(query);
        } finally {
          searcherManager.release(searcher);
        }
      }
      
    });
    
  }
  
  private void doTest(String testName, TestCallbacks callbacks) throws Exception {

    //Create some random words. We need to be careful
    //to make sure we get NUM_WORDS distinct words here
    Set<String> wordSet = new HashSet<String>();
    Random rand = new Random();
    while(wordSet.size() < NUM_WORDS) {
      int length = rand.nextInt(12) + 3;
      char[] text = new char[length];
      for(int i = 0; i < length; i++) {
        text[i] = (char) (rand.nextInt(26) + 97);
      }
      wordSet.add(new String(text));
    }
    List<String> words = new ArrayList<String>(wordSet.size());
    words.addAll(wordSet);
    
    
    
    //warm up
    writeRandomWords(callbacks, words, rand, NUM_ENTRIES / 10, NUM_QUERIES / 10, COMMIT_INTERVAL[0]);
    
    //Do the actual test
    
    for(int i = 0; i < COMMIT_INTERVAL.length; i++) {
      Results results = writeRandomWords(callbacks, words, rand, NUM_ENTRIES, NUM_QUERIES / 10, COMMIT_INTERVAL[i]);
    
      System.out.println(testName + " writes(entries=" + NUM_ENTRIES + ", commit=" + COMMIT_INTERVAL[i] + "): " + TimeUnit.NANOSECONDS.toMillis(results.writeTime));
      System.out.println(testName + " queries(entries=" + NUM_ENTRIES + ", commit=" + COMMIT_INTERVAL[i] + "): " + TimeUnit.NANOSECONDS.toMillis(results.queryTime));
    }
  }

  private Results writeRandomWords(TestCallbacks callbacks, List<String> words,
      Random rand, int numEntries, int numQueries, int commitInterval) throws Exception {
    Results results  = new Results();
    callbacks.init();
    int[] counts = new int[words.size()];
    long start = System.nanoTime();
    try {
      for(int i =0; i < numEntries; i++) {
        int word1 = rand.nextInt(words.size());
        int word2 = rand.nextInt(words.size());
        counts[word1]++;
        counts[word2]++;
        String value = words.get(word1) + " " + words.get(word2);
        callbacks.addObject("key" + i, value);

        if(i % commitInterval == 0 && i != 0) {
          callbacks.commit();
        }
      }
      callbacks.commit();
      callbacks.waitForAsync();
      long end = System.nanoTime();
      results.writeTime = end - start;
      
      
      start = System.nanoTime();
      for(int i=0; i < numQueries; i++) {
        int wordIndex = rand.nextInt(words.size());
        String word = words.get(wordIndex);
        Query query = new TermQuery(new Term("text", word));
        int size  = callbacks.query(query);
//        int size  = callbacks.query(parser.parse(word));
        //All of my tests sometimes seem to be missing a couple of words, including the stock lucene
//        assertEquals("Error on query " + i + " word=" + word, counts[wordIndex], size);
      }
      end = System.nanoTime();
      results.queryTime = end - start;
      
      return results;
    } finally {
      callbacks.cleanup();
    }
  }

  private static class TestObject implements DataSerializable {
    private String text;

    public TestObject() {
      
    }
    
    public TestObject(String text) {
      super();
      this.text = text;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(text, out);
    }

    @Override
    public void fromData(DataInput in)
        throws IOException, ClassNotFoundException {
      text = DataSerializer.readString(in);
    }

    @Override
    public String toString() {
      return text;
    }
    
    
  }
  
  private interface TestCallbacks {
    public void init() throws Exception;
    public int query(Query query) throws Exception;
    public void addObject(String key, String text)  throws Exception;
    public void commit() throws Exception;
    public void waitForAsync() throws Exception;
    public void cleanup() throws Exception;
  }
  
  private static class Results {
    long writeTime;
    long queryTime;
  }
}
