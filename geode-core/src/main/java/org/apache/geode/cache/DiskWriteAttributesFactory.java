/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache;

import java.util.Properties;

import org.apache.geode.internal.cache.DiskWriteAttributesImpl;
import org.apache.geode.internal.cache.xmlcache.CacheXml;

/**
 * Factory for getting DiskWriteAttribute objects
 *
 * @since GemFire 5.1
 * @deprecated as of 6.5 use {@link DiskStoreFactory} instead
 */
@Deprecated
public class DiskWriteAttributesFactory implements java.io.Serializable {
  private static final long serialVersionUID = -4077746249663727235L;

  private final Properties props = new Properties();

  //////////// *** Methods to get instances of DWA *** //////////////

  /**
   *
   * Creates a new instance of DiskWriteAttributesFactory ready to create a
   * <code>DiskWriteAttributes</code> with default settings. The default
   * <code>DiskWriteAttributes</code> thus created will have following behaviour.
   * <ul>
   * <li>synchronous = false
   * <li>auto-compact = true
   * <li>allow-force-compaction = false
   * <li>max-oplog-size = 1024 MB
   * <li>time-interval = 1 sec
   * <li>byte-threshold = 0 bytes
   *
   * </ul>
   */
  public DiskWriteAttributesFactory() {

  }

  /**
   * Creates a new instance of DiskWriteAttributesFactory Factory ready to create a
   * <code>DiskWriteAttributes</code> with the same settings as those in the specified
   * <code>DiskWriteAttributes</code>.
   *
   * @param dwa the <code>DiskWriteAttributes</code> used to initialize this
   *        DiskWriteAttributesFactory
   */
  public DiskWriteAttributesFactory(DiskWriteAttributes dwa) {

    this.props.setProperty(CacheXml.BYTES_THRESHOLD, String.valueOf(dwa.getBytesThreshold()));
    long maxOplogSizeInBytes = convertToBytes(dwa.getMaxOplogSize());
    this.props.setProperty(CacheXml.MAX_OPLOG_SIZE, String.valueOf(maxOplogSizeInBytes));
    this.props.setProperty(CacheXml.ROLL_OPLOG, String.valueOf(dwa.isRollOplogs()));
    this.props.setProperty(DiskWriteAttributesImpl.SYNCHRONOUS_PROPERTY,
        String.valueOf(dwa.isSynchronous()));
    if (dwa.getTimeInterval() > -1) {
      this.props.setProperty(CacheXml.TIME_INTERVAL, String.valueOf(dwa.getTimeInterval()));
    }

  }

  /**
   * Sets whether or not the writing to the disk is synchronous.
   *
   * @param isSynchronous boolean if true indicates synchronous writes
   * @deprecated as of 6.5 use {@link AttributesFactory#setDiskSynchronous} instead
   */
  @Deprecated
  public void setSynchronous(boolean isSynchronous) {
    this.props.setProperty(DiskWriteAttributesImpl.SYNCHRONOUS_PROPERTY,
        String.valueOf(isSynchronous));
  }

  /**
   * Sets whether or not the rolling of Oplog is enabled .
   *
   * @param rollingEnabled true if oplogs are to be compacted automatically; false if no compaction.
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setAutoCompact} instead
   */
  @Deprecated
  public void setRollOplogs(boolean rollingEnabled) {
    this.props.setProperty(CacheXml.ROLL_OPLOG, String.valueOf(rollingEnabled));
  }

  /**
   * Sets the threshold at which an oplog will become compactable. While the percentage of live
   * records in the oplog exceeds this threshold the oplog will not be compacted. Once the
   * percentage of live is less than or equal to the threshold the oplog can be compacted. The lower
   * the threshold the longer the compactor will wait before compacting an oplog. The threshold is a
   * percentage in the range 0..100. The default is 50%.
   * <P>
   * Examples: A threshold of 100 causes any oplog that is no longer being written to to be
   * compactable. A threshold of 0 causes only oplogs that have no live records to be compactable in
   * which case the compact can simply remove the oplog file. A threshold of 50 causes an oplog to
   * become compactable when half of its live records become dead.
   *
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setCompactionThreshold} instead
   */
  @Deprecated
  public void setCompactionThreshold(int compactionThreshold) {
    if (compactionThreshold < 0) {
      throw new IllegalArgumentException(
          String.format("%s has to be positive number and the value given %s is not acceptable",
              new Object[] {CacheXml.COMPACTION_THRESHOLD,
                  Integer.valueOf(compactionThreshold)}));
    } else if (compactionThreshold > 100) {
      throw new IllegalArgumentException(
          String.format(
              "%s has to be a number that does not exceed %s so the value given %s is not acceptable",
              new Object[] {CacheXml.COMPACTION_THRESHOLD,
                  Integer.valueOf(compactionThreshold), Integer.valueOf(100)}));
    }
    this.props.setProperty(CacheXml.COMPACTION_THRESHOLD, String.valueOf(compactionThreshold));
  }

  /**
   * Sets the maximum oplog size in bytes. When the active oplog size hits the maximum a new oplog
   * will be created.
   * <P>
   * Note that this method sets the same attribute as {@link #setMaxOplogSize}. The last set of the
   * attribute determines its value.
   *
   * @param maxOplogSize the maximum size of the oplog in bytes.
   * @throws IllegalArgumentException if the value specified is a negative number
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setMaxOplogSize} instead
   */
  @Deprecated
  public void setMaxOplogSizeInBytes(long maxOplogSize) {

    if (maxOplogSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Maximum Oplog size specified has to be a non-negative number and the value given %s is not acceptable",
              Long.valueOf(maxOplogSize)));
    }
    this.props.setProperty(CacheXml.MAX_OPLOG_SIZE, String.valueOf(maxOplogSize));
  }

  /**
   * Sets the maximum oplog size in megabytes. When the active oplog size hits the maximum a new
   * oplog will be created.
   * <P>
   * Note that this method sets the same attribute as {@link #setMaxOplogSizeInBytes}. The last set
   * of the attribute determines its value.
   *
   * @param maxOplogSize the maximum size of the oplog in megabytes.
   * @throws IllegalArgumentException if the value specified is a negative number
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setMaxOplogSize} instead
   */
  @Deprecated
  public void setMaxOplogSize(int maxOplogSize) {

    if (maxOplogSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Maximum Oplog size specified has to be a non-negative number and the value given %s is not acceptable",
              Integer.valueOf(maxOplogSize)));
    }
    long maxOplogSizeInBytes = convertToBytes(maxOplogSize);
    this.props.setProperty(CacheXml.MAX_OPLOG_SIZE, String.valueOf(maxOplogSizeInBytes));
  }

  /**
   * Takes an int which is supposed to be in megabytes and converts it to a long. This conversion
   * takes into account that multiplication can lead to Integer.MAX_VALUE being exceeded
   *
   * @return the converted value
   */
  private long convertToBytes(int megaBytes) {
    long bytes = megaBytes;
    bytes = bytes * 1024 * 1024;
    return bytes;
  }

  /**
   * Sets the number of milliseconds that can elapse before unwritten data is written to disk. It
   * has significance only in case of asynchronous mode of writing.
   *
   * @param timeInterval Time interval in milliseconds
   * @throws IllegalArgumentException if the value specified is a negative number
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setTimeInterval} instead
   */
  @Deprecated
  public void setTimeInterval(long timeInterval) {
    if (timeInterval < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Time Interval specified has to be a non-negative number and the value given %s is not acceptable",
              Long.valueOf(timeInterval)));
    }

    this.props.setProperty(CacheXml.TIME_INTERVAL, String.valueOf(timeInterval));
  }

  /**
   * Sets the number of unwritten bytes of data that can be enqueued before being written to disk.
   * It has significance only in case of asynchronous mode of writing.
   *
   * @param bytesThreshold the maximum number of bytes to enqueue before async data is flushed.
   * @throws IllegalArgumentException if the value specified is a negative number
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setQueueSize} instead
   */
  @Deprecated
  public void setBytesThreshold(long bytesThreshold) {
    if (bytesThreshold < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Queue size specified has to be a non-negative number and the value given %s is not acceptable",
              Long.valueOf(bytesThreshold)));
    }

    this.props.setProperty(CacheXml.BYTES_THRESHOLD, String.valueOf(bytesThreshold));
  }

  /**
   * Creates a <code>DiskWriteAttributes</code> with the current settings.
   *
   * @return the newly created <code>DiskWriteAttributes</code>
   * @throws IllegalStateException if the current settings has compaction enabled with maximum Oplog
   *         Size specified as infinite ( represented by 0 ) *
   * @since GemFire 5.1
   * @deprecated as of 6.5 use {@link DiskStoreFactory#create} instead
   */
  @Deprecated
  public DiskWriteAttributes create() {
    return new DiskWriteAttributesImpl(this.props);
  }

}
