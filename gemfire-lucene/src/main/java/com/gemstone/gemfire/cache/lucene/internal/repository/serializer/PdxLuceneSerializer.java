package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import org.apache.lucene.document.Document;

import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * LuceneSerializer which can handle any PdxInstance
 */
class PdxLuceneSerializer implements LuceneSerializer {

  private String[] indexedFields;

  public PdxLuceneSerializer(String[] indexedFields) {
    this.indexedFields = indexedFields;
  }

  @Override
  public void toDocument(Object value, Document doc) {
    PdxInstance pdx = (PdxInstance) value;
    for(String field : indexedFields) {
      if(pdx.hasField(field)) {
        Object fieldValue = pdx.getField(field);
        SerializerUtil.addField(doc, field, fieldValue);
      }
    }
  }
}