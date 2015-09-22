package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.cache.lucene.LuceneResultStruct;

public class LuceneResultStructImpl<K,V> implements LuceneResultStruct<K,V> {
  K key;
  V value;
  float score;

  public LuceneResultStructImpl(K key, V value, float score) {
    this.key = key;
    this.value = value;
    this.score = score;
  }

  @Override
  public Object getProjectedField(String fieldName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public float getScore() {
    return score;
  }

  @Override
  public Object[] getNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] getResultValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + Float.floatToIntBits(score);
    result = prime * result + ((value == null) ? 0 : value.hashCode());
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
    LuceneResultStructImpl other = (LuceneResultStructImpl) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "LuceneResultStructImpl [key=" + key + ", value=" + value
        + ", score=" + score + "]";
  }
}
