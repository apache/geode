package com.gemstone.gemfire.cache.lucene.internal.repository;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.SerializerUtil;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.Type2;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test of the {@link IndexRepository} and everything below
 * it. This tests that we can save gemfire objects or PDXInstance
 * objects into a lucene index and search for those objects later.
 */
@Category(IntegrationTest.class)
public class IndexRepositoryImplJUnitTest {

  private IndexRepositoryImpl repo;
  private HeterogenousLuceneSerializer mapper;
  private DirectoryReader reader;
  private StandardAnalyzer analyzer = new StandardAnalyzer();
  private IndexWriter writer;

  @Before
  public void setUp() throws IOException {
    ConcurrentHashMap<String, File> fileRegion = new ConcurrentHashMap<String, File>();
    ConcurrentHashMap<ChunkKey, byte[]> chunkRegion = new ConcurrentHashMap<ChunkKey, byte[]>();
    RegionDirectory dir = new RegionDirectory(fileRegion, chunkRegion);
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    writer = new IndexWriter(dir, config);
    reader = DirectoryReader.open(writer, true);
    String[] indexedFields= new String[] {"s", "i", "l", "d", "f", "s2", "missing"};
    mapper = new HeterogenousLuceneSerializer(indexedFields);
    repo = new IndexRepositoryImpl(writer, mapper);
  }
  
  @Test
  public void testAddDocs() throws IOException, ParseException {
    repo.create("key1", new Type2("bacon maple bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    repo.create("key2", new Type2("McMinnville Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.create("key3", new Type2("Voodoo Doll doughnut", 1, 2L, 3.0, 4.0f, "Toasted coconut doughnut"));
    repo.create("key4", new Type2("Portland Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.commit();
    
    checkQuery("Cream", "s", "key2", "key4");
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

  private void updateAndRemove(Object key1, Object key2, Object key3,
      Object key4) throws IOException, ParseException {
    repo.create(key1, new Type2("bacon maple bar", 1, 2L, 3.0, 4.0f, "Grape Ape doughnut"));
    repo.create(key2, new Type2("McMinnville Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.create(key3, new Type2("Voodoo Doll doughnut", 1, 2L, 3.0, 4.0f, "Toasted coconut doughnut"));
    repo.create(key4, new Type2("Portland Cream doughnut", 1, 2L, 3.0, 4.0f, "Captain my Captain doughnut"));
    repo.commit();
    
    repo.update(key3, new Type2("Boston Cream Pie", 1, 2L, 3.0, 4.0f, "Toasted coconut doughnut"));
    repo.delete(key4);
    repo.commit();

//    BooleanQuery q = new BooleanQuery();
//    q.add(new TermQuery(SerializerUtil.toKeyTerm("key3")), Occur.MUST_NOT);
//    writer.deleteDocuments(q);
//    writer.commit();
    
    //Make sure the updates and deletes were applied
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

  private void checkQuery(String queryTerm, String queryField, Object ... expectedKeys)
      throws IOException, ParseException {
    QueryParser parser = new QueryParser(queryField, analyzer);
    
    DirectoryReader result = DirectoryReader.openIfChanged(reader);
    reader = result == null ? reader : result;
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs results = searcher.search(parser.parse(queryTerm), 100);
    
    Set<Object> expectedSet = new HashSet<Object>();
    expectedSet.addAll(Arrays.asList(expectedKeys));
    Set<Object> actualKeys = new HashSet<Object>();
    for(ScoreDoc scoreDoc: results.scoreDocs) {
      Document doc = searcher.doc(scoreDoc.doc);
      assertEquals(1, doc.getFields().size());
      actualKeys.add(SerializerUtil.getKey(doc));
    }
    assertEquals(expectedSet, actualKeys);
  }

  /**
   * A wrapper around a byte array that implements equals,
   * for comparison checks.
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
