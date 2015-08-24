package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;

public class LuceneResultStructImpl implements LuceneResultStruct {
  Object key;
  float score;

  public LuceneResultStructImpl(Object key, float score) {
    this.key = key;
    this.score = score;
  }

  @Override
  public Object getProjectedField(String fieldName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getKey() {
    return key;
  }

  @Override
  public Object getValue() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public float getScore() {
    return score;
  }

  @Override
  public Object[] getNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object[] getResultValues() {
    // TODO Auto-generated method stub
    return null;
  }
}
