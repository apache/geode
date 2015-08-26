package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

/**
 * A repository that writes to a single lucene index writer
 */
public class SingleIndexRepositoryImpl implements SingleIndexRepository {
  
  private final IndexWriter writer;
  private final ObjectToDocumentMapper mapper;
  
  public SingleIndexRepositoryImpl(IndexWriter writer, ObjectToDocumentMapper mapper) {
    this.writer = writer;
    this.mapper = mapper;
  }

  @Override
  public void create(Object key, Object value) throws IOException {
      writer.addDocument(mapper.transform(key, value));
  }

  @Override
  public void update(Object key, Object value) throws IOException {
    writer.updateDocument(mapper.keyTerm(key), mapper.transform(key, value));
  }

  @Override
  public void delete(Object key) throws IOException {
    writer.deleteDocuments(mapper.keyTerm(key));
  }

  @Override
  public void commit() throws IOException {
    writer.commit();
  }

}
