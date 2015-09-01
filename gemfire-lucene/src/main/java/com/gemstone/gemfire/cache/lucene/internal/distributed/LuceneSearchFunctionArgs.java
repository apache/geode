package com.gemstone.gemfire.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataSerializable;

/**
 * Contains function arguments for text / lucene search
 */
public class LuceneSearchFunctionArgs implements VersionedDataSerializable {
  private static final long serialVersionUID = 1L;

  @Override
  public void toData(DataOutput out) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub

  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

  public Set<Integer> getBuckets() {
    return null;
  }
}
