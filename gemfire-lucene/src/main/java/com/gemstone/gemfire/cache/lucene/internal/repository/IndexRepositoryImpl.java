package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.SerializerUtil;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;

/**
 * A repository that writes to a single lucene index writer
 */
public class IndexRepositoryImpl implements IndexRepository {
  
  private final IndexWriter writer;
  private final LuceneSerializer serializer;
  
  public IndexRepositoryImpl(IndexWriter writer, LuceneSerializer serializer) {
    this.writer = writer;
    this.serializer = serializer;
  }

  @Override
  public void create(Object key, Object value) throws IOException {
      Document doc = new Document();
      SerializerUtil.addKey(key, doc);
      serializer.toDocument(value, doc);
      writer.addDocument(doc);
  }

  @Override
  public void update(Object key, Object value) throws IOException {
    Document doc = new Document();
    SerializerUtil.addKey(key, doc);
    serializer.toDocument(value, doc);
    writer.updateDocument(SerializerUtil.getKeyTerm(doc), doc);
  }

  @Override
  public void delete(Object key) throws IOException {
    Term keyTerm = SerializerUtil.toKeyTerm(key);
    writer.deleteDocuments(keyTerm);
  }

  @Override
  public void commit() throws IOException {
    writer.commit();
  }

}
