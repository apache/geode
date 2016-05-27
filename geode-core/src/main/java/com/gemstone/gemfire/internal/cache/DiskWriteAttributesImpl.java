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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

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
public final class DiskWriteAttributesImpl implements DiskWriteAttributes
{
  private static final long serialVersionUID = -4269181954992768424L;

  /** Are writes synchronous? */
  private final boolean isSynchronous;

  /**
   * The the number of milliseconds that can elapse before unwritten data is
   * written to disk.
   */
  private final long timeInterval;

  /**
   * The number of bytes of region entry data to queue up before writing to
   * disk.
   */
  private final long bytesThreshold;

  /** Whether compaction is to be permitted or not. Defaults to true * */
  private final boolean compactOplogs;

  /** stored in bytes as a long but specified in megabytes by client applications**/
  private final long maxOplogSize;

  /** default max in bytes **/
  private static final long DEFAULT_MAX_OPLOG_SIZE =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "DEFAULT_MAX_OPLOG_SIZE", 1024L).longValue() * (1024 * 1024); // 1 GB

  /** default max limit in bytes **/
  private static final long DEFAULT_MAX_OPLOG_SIZE_LIMIT = (long)Integer.MAX_VALUE * (1024*1024);

  private static final boolean DEFAULT_ROLL_OPLOGS = true;

  private static final boolean DEFAULT_ALLOW_FORCE_COMPACTION = false;

  private static final boolean DEFAULT_IS_SYNCHRONOUS = false; // the pre 6.5 default

//  private static final long DEFAULT_BYTES_THRESHOLD = 0;

  static final long DEFAULT_TIME_INTERVAL = 1000; // 1 sec

  private static final int DEFAULT_COMPACTION_THRESHOLD = 50;

  public static final String SYNCHRONOUS_PROPERTY = "synchronous";

  /**
   * The property used to specify the base directory for Sql Fabric persistence
   * of Gateway Queues, Tables, Data Dictionary etc.
   */
  public static final String SYS_PERSISTENT_DIR = "sys-disk-dir";

  /**
   * The system property for {@link #SYS_PERSISTENT_DIR}.
   */
  public static final String SYS_PERSISTENT_DIR_PROP = "sqlfabric."
      + SYS_PERSISTENT_DIR;

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
   * Creates a new <code>DiskWriteAttributes</code> object using the
   * properties specified. The properties that can be specified are:
   * <ul>
   * <li>synchronous : boolean to specify whether DiskWrites will be
   * synchronous (true)/asynchronous (false)
   * <li>auto-compact : boolean to specify whether to automatically compact
   * disk files so they use less disk space (true)
   * <li>allow-force-compaction : boolean to specify whether to manual compaction
   * of disk files is allowed (false)
   * <li>compaction-threshold: The threshold at which an oplog becomes compactable.
   *   Must be in the range 0..100 inclusive. (50)
   * <li>max-oplog-size: The maximum size of an oplog. 0 would mean infinity
   * <li>time-interval: The number of milliseconds that can elapse before
   * unwritten data is written to disk.
   * <li>bytes-threshold: The number of unwritten bytes of data that can be
   * enqueued before being written to disk
   * </ul>
   * The above properties are case sensitive and if a propery which is not in
   * the list above is passed no action is taken. If a property which is present
   * above is not specified then the following default values will be uses
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
   * @param properties
   * 
   * @throws IllegalArgumentException
   *           If any of the properties specified are not in the expected
   *           format.
   * @throws IllegalStateException
   *           if max-oplog-size is set to infinity(0) and compaction is set to
   *           true
   */

  public DiskWriteAttributesImpl(Properties properties) {

    String isSynchronousString = properties.getProperty(SYNCHRONOUS_PROPERTY);
    if (isSynchronousString == null) {
      this.isSynchronous = DEFAULT_IS_SYNCHRONOUS;
    }
    else {
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

    String bytesThresholdString = properties
        .getProperty(CacheXml.BYTES_THRESHOLD);
    if (bytesThresholdString != null) {
      if (this.isSynchronous) {
        // log warning, no use setting time if is synchronous
      }
      this.bytesThreshold = verifyLongInString(bytesThresholdString,
          CacheXml.BYTES_THRESHOLD);
    }
    else {
      this.bytesThreshold = 0L;
    }
    
    String timeIntervalString = properties.getProperty(CacheXml.TIME_INTERVAL);
    if (timeIntervalString != null) {
      if (this.isSynchronous) {
        // log warning, no use setting time if is synchronous
      }
      this.timeInterval = verifyLongInString(timeIntervalString,
          CacheXml.TIME_INTERVAL);
    }
    else {
      if (!this.isSynchronous && this.bytesThreshold == 0 ) {
        this.timeInterval =DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL;
      }else {
        this.timeInterval = 0;
      }
    }
    
    

    String maxOplogSizeString = properties.getProperty(CacheXml.MAX_OPLOG_SIZE);

    if (maxOplogSizeString != null) {
      long opSize = verifyLongInString(maxOplogSizeString,
          CacheXml.MAX_OPLOG_SIZE);
      if (opSize == 0 && this.compactOplogs == true) {
        throw new IllegalStateException(LocalizedStrings.DiskWriteAttributesImpl_COMPACTION_CANNOT_BE_SET_TO_TRUE_IF_MAXOPLOGSIZE_IS_SET_TO_INFINITE_INFINITE_IS_REPRESENTED_BY_SIZE_ZERO_0.toLocalizedString());

      }
      if (opSize == 0 || opSize == DEFAULT_MAX_OPLOG_SIZE_LIMIT) {
        if (this.compactOplogs) {
          throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_CANNOT_SET_MAXOPLOGS_SIZE_TO_INFINITY_0_IF_COMPACTION_IS_SET_TO_TRUE.toLocalizedString());
        }
        else {
          this.maxOplogSize = DEFAULT_MAX_OPLOG_SIZE_LIMIT; // infinity
        }
      }
      else {
        this.maxOplogSize = opSize;
      }
    }
    else {
      this.maxOplogSize = DEFAULT_MAX_OPLOG_SIZE;
    }
  }

  /**
   * Verifys if the propertyString passed is a valid boolean value or null else
   * throws an IllegalArgument exception
   * 
   * @param propertyString
   * @param property
   * @throws IllegalArgumentException
   *           if the property string passed does not represent a boolean or
   *           null
   *  
   */
  private void verifyBooleanString(String propertyString, String property)
  {
    if (!(propertyString.equalsIgnoreCase("true")
        || propertyString.equalsIgnoreCase("false"))) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_PROPERTY_HAS_TO_BE_TRUE_OR_FALSE_OR_NULL_AND_CANNOT_BE_1.toLocalizedString(new Object[] {property, propertyString}));
    }
  }

  /**
   * Verifys if the string passed, is in a number format which is acceptable and
   * returns the long value of the string.
   * 
   * @param propertyString
   * @param property
   * @return the long value of the string
   */
  private long verifyLongInString(String propertyString, String property)
  {
    long returnValue;
    try {
      returnValue = Long.valueOf(propertyString).longValue();
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_A_VALID_NUMBER_AND_NOT_1.toLocalizedString(new Object[] {property, propertyString}));
    }

    if (returnValue < 0) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE.toLocalizedString(
            new Object[] {property, Long.valueOf(returnValue)}));
    }

    return returnValue;
  }

  /**
   * Verifys if the string passed, is in a number format which is acceptable and
   * returns the int value of the string.
   * 
   * @param propertyString
   * @param property
   * @return the int value of the string
   */
  private int verifyPercentInString(String propertyString, String property)
  {
    int returnValue;
    try {
      returnValue = Integer.valueOf(propertyString).intValue();
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_A_VALID_NUMBER_AND_NOT_1.toLocalizedString(new Object[] {property, propertyString}));
    }

    if (returnValue < 0) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE.toLocalizedString(
            new Object[] {property, Integer.valueOf(returnValue)}));
    } else if (returnValue > 100) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_LESS_THAN_2_BUT_WAS_1.toLocalizedString(new Object[] {property, Integer.valueOf(returnValue), Integer.valueOf(100)}));
    }

    return returnValue;
  }

  // //////////////////// Instance Methods //////////////////////

  /**
   * Returns whether or not this <code>DiskWriteAttributes</code> configures
   * synchronous writes.
   */
  public boolean isSynchronous()
  {
    return this.isSynchronous;
  }

  /**
   * Returns true if the oplogs is to be rolled
   */
  public boolean isRollOplogs()
  {
    return this.compactOplogs;
  }

  /** Get the max Oplog Size in megabytes. The value is stored in bytes so division is necessary **/
  public int getMaxOplogSize()
  {
    return (int)(maxOplogSize/(1024 * 1024));
  }


  /** Get the max Oplog Size in bytes **/
  long getMaxOplogSizeInBytes()
  {
    return maxOplogSize;
  }
  
  /**
   * Returns the number of milliseconds that can elapse before unwritten data is
   * written to disk. If this <code>DiskWriteAttributes</code> configures
   * synchronous writing, then <code>timeInterval</code> has no meaning.
   */
  public long getTimeInterval()
  {
    return this.timeInterval;
  }

  /**
   * Returns the number of unwritten bytes of data that can be enqueued before
   * being written to disk. If this <code>DiskWriteAttributes</code>
   * configures synchronous writing, then <code>bytesThreshold</code> has no
   * meaning.
   */
  public long getBytesThreshold()
  {
    return this.bytesThreshold;
  }


  /**
   * Two <code>DiskWriteAttributes</code> are equal if the both specify the
   * synchronous writes, or they both specify asynchronous writes with the same
   * time interval and bytes threshold.
   */
  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof DiskWriteAttributesImpl)) {
      return false;
    }

    DiskWriteAttributesImpl other = (DiskWriteAttributesImpl)o;
    if (other.isSynchronous() != isSynchronous()) {
      return false;
    }
    boolean result = other.isRollOplogs() == isRollOplogs()
      && other.getMaxOplogSize() == getMaxOplogSize();
    if (!isSynchronous()) {
      result = result
        && other.getTimeInterval() == getTimeInterval()
        && other.getBytesThreshold() == getBytesThreshold();
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#hashCode()
   * 
   * Note that we just need to make sure that equal objects return equal
   * hashcodes; nothing really elaborate is done here.
   */
  @Override
  public int hashCode() {
    int result = 0;
    
    if (this.isSynchronous()) {
      if (this.isRollOplogs()) {
        result += 2;
      }
    }
    else {
      result += 1; // asynchronous
      result += this.getTimeInterval();
      result += this.getBytesThreshold();
    }
    result += this.getMaxOplogSize();
    return result;
  }
  
  @Override
  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    if (this.isSynchronous()) {
      sb.append("Synchronous writes to disk");

    }
    else {
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
   * @return the default max oplog size limit in megabytes
   */
  public static int getDefaultMaxOplogSizeLimit()
  {
    return (int)(DEFAULT_MAX_OPLOG_SIZE_LIMIT/(1024*1024));
  }

  /**
   * 
   * Returns the default compaction oplog value
   * 
   * @return the default compaction oplog value
   */
  public static boolean getDefaultRollOplogsValue()
  {
    return DEFAULT_ROLL_OPLOGS;
  }

  /**
   * 
   * Gets the default max oplog size in megabytes
   * 
   * @return the default max oplog size in megabytes
   */
  public static int getDefaultMaxOplogSize()
  {
    return (int)(DEFAULT_MAX_OPLOG_SIZE/(1024*1024));
  }

  /**
   * Returns a default instance of  DiskWriteAttributes
   * @return the default DiskWriteAttributes instance
   */
  public static DiskWriteAttributes getDefaultAsyncInstance()
  {
    return DEFAULT_ASYNC_DWA;
  }
  public static DiskWriteAttributes getDefaultSyncInstance()
  {
    return DEFAULT_SYNC_DWA;
  }


  // Asif: Sql Fabric related helper methods.
  // These static functions need to be moved to a better place.
  // preferably in sql Fabric source tree but since GatewayImpl is also
  // utilizing it, we have no option but to keep it here.
  public static String generateOverFlowDirName(String dirName) {
    dirName = generatePersistentDirName(dirName);
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      throw new CacheClosedException(
          "DiskWriteAttributesImpl::generateOverFlowDirName: no cache found.");
    }
    /* [sumedh] no need of below since sys-disk-dir is VM specific anyways
    char fileSeparator = System.getProperty("file.separator").charAt(0);
    DistributedMember member = cache.getDistributedSystem()
        .getDistributedMember();
    String host = member.getHost();
    int pid = member.getProcessId();
    final StringBuilder temp = new StringBuilder(dirName);
    temp.append(fileSeparator);
    temp.append(host);
    temp.append('-');
    temp.append(pid);
    return temp.toString();
    */
    return dirName;
  }

  public static String generatePersistentDirName(String dirPath) {
    String baseDir = System.getProperty(SYS_PERSISTENT_DIR_PROP);
    if (baseDir == null) {
    //Kishor : TODO : Removing old wan related code
      //baseDir = GatewayQueueAttributes.DEFAULT_OVERFLOW_DIRECTORY;
      baseDir = ".";
    }
    if (dirPath != null) {
      File dirProvided = new File(dirPath);
      // Is the directory path absolute?
      // For Windows this will check for drive letter. However, we want
      // to allow for no drive letter so prepend the drive.
      boolean isAbsolute = dirProvided.isAbsolute();
      if (!isAbsolute) {
        String driveName;
        // get the current drive for Windows and prepend
        if ((dirPath.charAt(0) == '/' || dirPath.charAt(0) == '\\')
            && (driveName = getCurrentDriveName()) != null) {
          isAbsolute = true;
          dirPath = driveName + dirPath;
        }
      }
      if (!isAbsolute) {
        // relative path so resolve it relative to parent dir
        dirPath = new File(baseDir, dirPath).getAbsolutePath();
      }
    }
    else {
      dirPath = baseDir;
    }
    return dirPath;
  }

  /**
   * Get the drive name of current working directory for windows else return
   * null for non-Windows platform (somewhat of a hack -- see if something
   * cleaner can be done for this).
   */
  public static String getCurrentDriveName() {
    if (System.getProperty("os.name").startsWith("Windows")) {
      try {
        // get the current drive
        return new File(".").getCanonicalPath().substring(0, 2);
      } catch (IOException ex) {
        throw new IllegalArgumentException(
            "Failed in setting the overflow directory", ex);
      }
    }
    return null;
  }
}
