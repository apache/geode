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
package org.apache.geode.internal.cache;

import java.util.Properties;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.xmlcache.CacheXml;

/**
 * Implementation of DiskWriteAttributes
 *
 * @see AttributesFactory#setDiskWriteAttributes
 * @see RegionAttributes#getDiskWriteAttributes
 * @see Region#writeToDisk
 *
 *
 * @since GemFire 5.1
 */
@SuppressWarnings({"deprecation", "unused"})
public class DiskWriteAttributesImpl implements DiskWriteAttributes {
  private static final long serialVersionUID = -4269181954992768424L;

  /** Are writes synchronous? */
  private final boolean isSynchronous;

  /**
   * The the number of milliseconds that can elapse before unwritten data is written to disk.
   */
  private final long timeInterval;

  /**
   * The number of bytes of region entry data to queue up before writing to disk.
   */
  private final long bytesThreshold;

  /** Whether compaction is to be permitted or not. Defaults to true * */
  private final boolean compactOplogs;

  /** stored in bytes as a long but specified in megabytes by client applications **/
  private final long maxOplogSize;

  /** default max in bytes **/
  private static final long DEFAULT_MAX_OPLOG_SIZE =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "DEFAULT_MAX_OPLOG_SIZE", 1024L).longValue()
          * (1024 * 1024); // 1 GB

  /** default max limit in bytes **/
  private static final long DEFAULT_MAX_OPLOG_SIZE_LIMIT = (long) Integer.MAX_VALUE * (1024 * 1024);

  private static final boolean DEFAULT_ROLL_OPLOGS = true;

  private static final boolean DEFAULT_ALLOW_FORCE_COMPACTION = false;

  private static final boolean DEFAULT_IS_SYNCHRONOUS = false; // the pre 6.5 default

  // private static final long DEFAULT_BYTES_THRESHOLD = 0;

  static final long DEFAULT_TIME_INTERVAL = 1000; // 1 sec

  private static final int DEFAULT_COMPACTION_THRESHOLD = 50;

  public static final String SYNCHRONOUS_PROPERTY = "synchronous";

  /**
   * Default disk directory size in megabytes
   *
   * @since GemFire 5.1
   */
  public static final int DEFAULT_DISK_DIR_SIZE = DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;

  private static final DiskWriteAttributes DEFAULT_ASYNC_DWA;
  static {
    Properties props = new Properties();
    props.setProperty(SYNCHRONOUS_PROPERTY, "false");
    DEFAULT_ASYNC_DWA = new DiskWriteAttributesImpl(props);
  }

  private static final DiskWriteAttributes DEFAULT_SYNC_DWA;
  static {
    Properties props = new Properties();
    props.setProperty(SYNCHRONOUS_PROPERTY, "true");
    DEFAULT_SYNC_DWA = new DiskWriteAttributesImpl(props);
  }

  /////////////////////// Constructors ///////////////////////

  /**
   *
   * Creates a new <code>DiskWriteAttributes</code> object using the properties specified. The
   * properties that can be specified are:
   * <ul>
   * <li>synchronous : boolean to specify whether DiskWrites will be synchronous (true)/asynchronous
   * (false)
   * <li>auto-compact : boolean to specify whether to automatically compact disk files so they use
   * less disk space (true)
   * <li>allow-force-compaction : boolean to specify whether to manual compaction of disk files is
   * allowed (false)
   * <li>compaction-threshold: The threshold at which an oplog becomes compactable. Must be in the
   * range 0..100 inclusive. (50)
   * <li>max-oplog-size: The maximum size of an oplog. 0 would mean infinity
   * <li>time-interval: The number of milliseconds that can elapse before unwritten data is written
   * to disk.
   * <li>bytes-threshold: The number of unwritten bytes of data that can be enqueued before being
   * written to disk
   * </ul>
   * The above properties are case sensitive and if a propery which is not in the list above is
   * passed no action is taken. If a property which is present above is not specified then the
   * following default values will be uses
   *
   * <ul>
   * <li>synchronous = false
   * <li>auto-compact = true
   * <li>allow-force-compaction = false
   * <li>compaction-threshold = 50 %
   * <li>max-oplog-size = 1 GB
   * <li>time-interval = 1000 milliseconds
   * <li>byte-threshold = 0 bytes
   * </ul>
   *
   *
   * @throws IllegalArgumentException If any of the properties specified are not in the expected
   *         format.
   * @throws IllegalStateException if max-oplog-size is set to infinity(0) and compaction is set to
   *         true
   */

  public DiskWriteAttributesImpl(Properties properties) {

    String isSynchronousString = properties.getProperty(SYNCHRONOUS_PROPERTY);
    if (isSynchronousString == null) {
      this.isSynchronous = DEFAULT_IS_SYNCHRONOUS;
    } else {
      verifyBooleanString(isSynchronousString, SYNCHRONOUS_PROPERTY);
      this.isSynchronous = Boolean.valueOf(isSynchronousString).booleanValue();
    }

    String compactOplogsString = properties.getProperty(CacheXml.ROLL_OPLOG);
    if (compactOplogsString == null) {
      this.compactOplogs = DEFAULT_ROLL_OPLOGS;
    } else {
      verifyBooleanString(compactOplogsString, CacheXml.ROLL_OPLOG);
      this.compactOplogs = Boolean.valueOf(compactOplogsString).booleanValue();
    }

    String bytesThresholdString = properties.getProperty(CacheXml.BYTES_THRESHOLD);
    if (bytesThresholdString != null) {
      if (this.isSynchronous) {
        // log warning, no use setting time if is synchronous
      }
      this.bytesThreshold = verifyLongInString(bytesThresholdString, CacheXml.BYTES_THRESHOLD);
    } else {
      this.bytesThreshold = 0L;
    }

    String timeIntervalString = properties.getProperty(CacheXml.TIME_INTERVAL);
    if (timeIntervalString != null) {
      if (this.isSynchronous) {
        // log warning, no use setting time if is synchronous
      }
      this.timeInterval = verifyLongInString(timeIntervalString, CacheXml.TIME_INTERVAL);
    } else {
      if (!this.isSynchronous && this.bytesThreshold == 0) {
        this.timeInterval = DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL;
      } else {
        this.timeInterval = 0;
      }
    }



    String maxOplogSizeString = properties.getProperty(CacheXml.MAX_OPLOG_SIZE);

    if (maxOplogSizeString != null) {
      long opSize = verifyLongInString(maxOplogSizeString, CacheXml.MAX_OPLOG_SIZE);
      if (opSize == 0 && this.compactOplogs == true) {
        throw new IllegalStateException(
            "Compaction cannot be set to true if max-oplog-size is set to infinite (infinite is represented by size zero : 0)");

      }
      if (opSize == 0 || opSize == DEFAULT_MAX_OPLOG_SIZE_LIMIT) {
        if (this.compactOplogs) {
          throw new IllegalArgumentException(
              "Cannot set maxOplogs size to infinity (0) if compaction is set to true");
        } else {
          this.maxOplogSize = DEFAULT_MAX_OPLOG_SIZE_LIMIT; // infinity
        }
      } else {
        this.maxOplogSize = opSize;
      }
    } else {
      this.maxOplogSize = DEFAULT_MAX_OPLOG_SIZE;
    }
  }

  /**
   * Verifys if the propertyString passed is a valid boolean value or null else throws an
   * IllegalArgument exception
   *
   * @throws IllegalArgumentException if the property string passed does not represent a boolean or
   *         null
   *
   */
  private void verifyBooleanString(String propertyString, String property) {
    if (!(propertyString.equalsIgnoreCase("true") || propertyString.equalsIgnoreCase("false"))) {
      throw new IllegalArgumentException(
          String.format("%s property has to be true or false or null and cannot be %s",
              new Object[] {property, propertyString}));
    }
  }

  /**
   * Verifys if the string passed, is in a number format which is acceptable and returns the long
   * value of the string.
   *
   * @return the long value of the string
   */
  private long verifyLongInString(String propertyString, String property) {
    long returnValue;
    try {
      returnValue = Long.valueOf(propertyString).longValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("%s has to be a valid number and not %s",
              new Object[] {property, propertyString}));
    }

    if (returnValue < 0) {
      throw new IllegalArgumentException(
          String.format("%s has to be positive number and the value given %s is not acceptable",
              new Object[] {property, Long.valueOf(returnValue)}));
    }

    return returnValue;
  }

  /**
   * Verifys if the string passed, is in a number format which is acceptable and returns the int
   * value of the string.
   *
   * @return the int value of the string
   */
  private int verifyPercentInString(String propertyString, String property) {
    int returnValue;
    try {
      returnValue = Integer.valueOf(propertyString).intValue();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("%s has to be a valid number and not %s",
              new Object[] {property, propertyString}));
    }

    if (returnValue < 0) {
      throw new IllegalArgumentException(
          String.format("%s has to be positive number and the value given %s is not acceptable",
              new Object[] {property, Integer.valueOf(returnValue)}));
    } else if (returnValue > 100) {
      throw new IllegalArgumentException(
          String.format(
              "%s has to be a number that does not exceed %s so the value given %s is not acceptable",

              new Object[] {property, Integer.valueOf(returnValue), Integer.valueOf(100)}));
    }

    return returnValue;
  }

  // //////////////////// Instance Methods //////////////////////

  /**
   * Returns whether or not this <code>DiskWriteAttributes</code> configures synchronous writes.
   */
  public boolean isSynchronous() {
    return this.isSynchronous;
  }

  /**
   * Returns true if the oplogs is to be rolled
   */
  public boolean isRollOplogs() {
    return this.compactOplogs;
  }

  /** Get the max Oplog Size in megabytes. The value is stored in bytes so division is necessary **/
  public int getMaxOplogSize() {
    return (int) (maxOplogSize / (1024 * 1024));
  }


  /** Get the max Oplog Size in bytes **/
  long getMaxOplogSizeInBytes() {
    return maxOplogSize;
  }

  /**
   * Returns the number of milliseconds that can elapse before unwritten data is written to disk. If
   * this <code>DiskWriteAttributes</code> configures synchronous writing, then
   * <code>timeInterval</code> has no meaning.
   */
  public long getTimeInterval() {
    return this.timeInterval;
  }

  /**
   * Returns the number of unwritten bytes of data that can be enqueued before being written to
   * disk. If this <code>DiskWriteAttributes</code> configures synchronous writing, then
   * <code>bytesThreshold</code> has no meaning.
   */
  public long getBytesThreshold() {
    return this.bytesThreshold;
  }


  /**
   * Two <code>DiskWriteAttributes</code> are equal if the both specify the synchronous writes, or
   * they both specify asynchronous writes with the same time interval and bytes threshold.
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DiskWriteAttributesImpl)) {
      return false;
    }

    DiskWriteAttributesImpl other = (DiskWriteAttributesImpl) o;
    if (other.isSynchronous() != isSynchronous()) {
      return false;
    }
    boolean result =
        other.isRollOplogs() == isRollOplogs() && other.getMaxOplogSize() == getMaxOplogSize();
    if (!isSynchronous()) {
      result = result && other.getTimeInterval() == getTimeInterval()
          && other.getBytesThreshold() == getBytesThreshold();
    }
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   *
   * Note that we just need to make sure that equal objects return equal hashcodes; nothing really
   * elaborate is done here.
   */
  @Override
  public int hashCode() {
    int result = 0;

    if (this.isSynchronous()) {
      if (this.isRollOplogs()) {
        result += 2;
      }
    } else {
      result += 1; // asynchronous
      result += this.getTimeInterval();
      result += this.getBytesThreshold();
    }
    result += this.getMaxOplogSize();
    return result;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    if (this.isSynchronous()) {
      sb.append("Synchronous writes to disk");

    } else {
      sb.append("Asynchronous writes to disk after a threshold of ");
      sb.append(this.getTimeInterval());
      sb.append("ms or ");
      sb.append(this.getBytesThreshold());
      sb.append(" bytes");
    }
    sb.append(". MaxOplog size is        : " + maxOplogSize);
    sb.append(". RollOplogs is          : " + compactOplogs);
    return sb.toString();
  }

  /**
   * Get the default max oplog size limit in megabytes
   *
   * @return the default max oplog size limit in megabytes
   */
  public static int getDefaultMaxOplogSizeLimit() {
    return (int) (DEFAULT_MAX_OPLOG_SIZE_LIMIT / (1024 * 1024));
  }

  /**
   *
   * Returns the default compaction oplog value
   *
   * @return the default compaction oplog value
   */
  public static boolean getDefaultRollOplogsValue() {
    return DEFAULT_ROLL_OPLOGS;
  }

  /**
   *
   * Gets the default max oplog size in megabytes
   *
   * @return the default max oplog size in megabytes
   */
  public static int getDefaultMaxOplogSize() {
    return (int) (DEFAULT_MAX_OPLOG_SIZE / (1024 * 1024));
  }

  /**
   * Returns a default instance of DiskWriteAttributes
   *
   * @return the default DiskWriteAttributes instance
   */
  public static DiskWriteAttributes getDefaultAsyncInstance() {
    return DEFAULT_ASYNC_DWA;
  }

  public static DiskWriteAttributes getDefaultSyncInstance() {
    return DEFAULT_SYNC_DWA;
  }
}
