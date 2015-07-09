package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.VersionedDataSerializable;
import com.gemstone.gemfire.internal.Version;

/**
 * Defines the arguments to the flush queue request.
 * 
 * @author bakera
 */
@SuppressWarnings("serial")
public class HDFSFlushQueueArgs implements VersionedDataSerializable {

  private static Version[] serializationVersions = new Version[]{ Version.GFE_81 };

  private HashSet<Integer> buckets;

  private long maxWaitTimeMillis;

  public HDFSFlushQueueArgs() {
  }

  public HDFSFlushQueueArgs(Set<Integer> buckets, long maxWaitTime) {
    this.buckets = new HashSet<Integer>(buckets);
    this.maxWaitTimeMillis = maxWaitTime;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet(buckets, out);
    out.writeLong(maxWaitTimeMillis);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.buckets = DataSerializer.readHashSet(in);
    this.maxWaitTimeMillis = in.readLong();
  }

  @Override
  public Version[] getSerializationVersions() {
    return serializationVersions;
  }

  public Set<Integer> getBuckets() {
    return (Set<Integer>) buckets;
  }

  public void setBuckets(Set<Integer> buckets) {
    this.buckets = new HashSet<Integer>(buckets);
  }

  public boolean isSynchronous() {
    return maxWaitTimeMillis == 0;
  }

  public long getMaxWaitTime() {
    return this.maxWaitTimeMillis;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getCanonicalName()).append("@")
    .append(System.identityHashCode(this))
    .append(" buckets:").append(buckets)
    .append(" maxWaitTime:").append(maxWaitTimeMillis);
    return sb.toString();
  }
}
