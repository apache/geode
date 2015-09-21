package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * Holds one entry matching search query and its metadata
 */
public class EntryScore implements DataSerializableFixedID {
  // Key of the entry matching search query
  private Object key;

  // The score of this document for the query.
  private float score;

  public EntryScore() {
  }

  public EntryScore(Object key, float score) {
    this.key = key;
    this.score = score;
  }

  public Object getKey() {
    return key;
  }

  public float getScore() {
    return score;
  }

  @Override
  public String toString() {
    return "key=" + key + " score=" + score;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_ENTRY_SCORE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(key, out);
    out.writeFloat(score);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    key = DataSerializer.readObject(in);
    score = in.readFloat();
  }
}
