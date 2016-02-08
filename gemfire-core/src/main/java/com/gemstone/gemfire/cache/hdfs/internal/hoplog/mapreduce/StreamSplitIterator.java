/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHDFSQueuePersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.SequenceFileHoplog;

/**
 * An iterator that iterates over a split in a sequential hoplog.
 * @author dsmith
 */
public class StreamSplitIterator extends HDFSSplitIterator {

  public StreamSplitIterator(FileSystem fs, Path[] path, long[] start, long[] len, long startTime, long endTime) throws IOException {
    super(fs, path, start, len, startTime, endTime);
  }

  public PersistedEventImpl getDeserializedValue() throws ClassNotFoundException, IOException {
    return UnsortedHDFSQueuePersistedEvent.fromBytes(iterator.getValue());
  }

  @Override
  protected AbstractHoplog getHoplog(FileSystem fs, Path path) throws IOException {
    return new SequenceFileHoplog(fs, path, null);
  }
}
