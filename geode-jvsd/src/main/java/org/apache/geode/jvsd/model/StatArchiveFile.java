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
package org.apache.geode.jvsd.model;

import static org.apache.geode.jvsd.model.StatArchiveFormat.ARCHIVE_VERSION;
import static org.apache.geode.jvsd.model.StatArchiveFormat.BOOLEAN_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.BYTE_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.CHAR_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.COMPACT_VALUE_2_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.DOUBLE_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.FLOAT_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.HEADER_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.ILLEGAL_RESOURCE_INST_ID;
import static org.apache.geode.jvsd.model.StatArchiveFormat.ILLEGAL_RESOURCE_INST_ID_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.ILLEGAL_STAT_OFFSET;
import static org.apache.geode.jvsd.model.StatArchiveFormat.INT_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.INT_TIMESTAMP_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.LONG_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.MAX_BYTE_RESOURCE_INST_ID;
import static org.apache.geode.jvsd.model.StatArchiveFormat.MIN_1BYTE_COMPACT_VALUE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.RESOURCE_INSTANCE_CREATE_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.RESOURCE_INSTANCE_DELETE_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.RESOURCE_INSTANCE_INITIALIZE_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.RESOURCE_TYPE_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.SAMPLE_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.SHORT_CODE;
import static org.apache.geode.jvsd.model.StatArchiveFormat.SHORT_RESOURCE_INST_ID_TOKEN;
import static org.apache.geode.jvsd.model.StatArchiveFormat.WCHAR_CODE;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

/**
 * StatArchiveFile provides APIs to read statistic snapshots from an archive
 * file.
 */
public class StatArchiveFile {

  private static final String GEMFIRE_LOG_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS z";

  protected static final NumberFormat nf = NumberFormat.getNumberInstance();

  static {
    nf.setMaximumFractionDigits(2);
    nf.setGroupingUsed(false);
  }

  private InputStream is;
  private DataInputStream dataIn;
  private ValueFilter[] filters;
  private final File archive;
  private /* final */ int archiveVersion;
  private /* final */ ArchiveInfo info;
  private final boolean compressed;
  private boolean updateOK;
  private final boolean dump;
  private boolean closed = false;
  protected int resourceInstSize = 0;
  protected ResourceInst[] resourceInstTable = null;
  private ResourceType[] resourceTypeTable = null;
  private final TimeStampSeries timeSeries = new TimeStampSeries();
  private final DateFormat timeFormatter = new SimpleDateFormat(GEMFIRE_LOG_FORMAT);
  private final static int BUFFER_SIZE = 1024 * 1024;
  private final ArrayList<ComboValue> fileComboValues = new ArrayList<ComboValue>();

  public StatArchiveFile(String archiveName) throws IOException {
    this(archiveName, false, null);
  }

  public StatArchiveFile(String archiveName, boolean dump,
      ValueFilter[] filters)
      throws IOException {
    this.archive = new File(archiveName);
    this.dump = dump;
    this.compressed = archive.getPath().endsWith(".gz");
    this.is = new FileInputStream(this.archive);
    if (this.compressed) {
      this.dataIn = new DataInputStream(
          new BufferedInputStream(new GZIPInputStream(this.is, BUFFER_SIZE),
              BUFFER_SIZE));
    } else {
      this.dataIn = new DataInputStream(
          new BufferedInputStream(this.is, BUFFER_SIZE));
    }
    this.updateOK = this.dataIn.markSupported();
    this.filters = createFilters(filters);

    update(false);
  }

  private String getArchiveFileName() {
    return archive.getAbsolutePath();
  }

  public List<ResourceInst> getResourceInstList() {
    List<ResourceInst> result = new ArrayList<>();
    for (ResourceInst r : resourceInstTable) {
      if (r != null) {
        result.add(r);
      }
    }
    return result;
  }

  public List<ResourceType> getResourceTypeList() {
    List<ResourceType> result = new ArrayList<>();
    for (ResourceType t : resourceTypeTable) {
      if (t != null) {
        result.add(t);
      }
    }
    return result;
  }

  private ValueFilter[] createFilters(ValueFilter[] allFilters) {
    if (allFilters == null) {
      return new ValueFilter[0];
    }
    ArrayList<ValueFilter> l = new ArrayList<>();
    for (ValueFilter allFilter : allFilters) {
      if (allFilter.archiveMatches(archive)) {
        l.add(allFilter);
      }
    }
    if (l.size() == allFilters.length) {
      return allFilters;
    } else {
      ValueFilter[] result = new ValueFilter[l.size()];
      return l.toArray(result);
    }
  }

  void matchSpec(StatSpec spec, List<AbstractValue> matchedValues) {
    if (spec.getCombineType() == StatSpec.FILE) {
      // search for previous ComboValue
      for (Object fileComboValue : this.fileComboValues) {
        ComboValue v = (ComboValue) fileComboValue;
        if (!spec.statMatches(v.getDescriptor().getName())) {
          continue;
        }
        if (!spec.typeMatches(v.getType().getName())) {
          continue;
        }
        ResourceInst[] resources = v.getResources();
        for (ResourceInst resource : resources) {
          if (!spec.instanceMatches(resource.getName(),
              resource.getId())) {
          }
          // note: we already know the archive file matches
        }
        matchedValues.add(v);
        return;
      }
      ArrayList<AbstractValue> l = new ArrayList<AbstractValue>();
      matchSpec(new RawStatSpec(spec), l);
      if (l.size() != 0) {
        ComboValue cv = new ComboValue(l);
        // save this in file's combo value list
        this.fileComboValues.add(cv);
        matchedValues.add(cv);
      }
    } else {
      for (int instIdx = 0; instIdx < resourceInstSize; instIdx++) {
        resourceInstTable[instIdx].matchSpec(spec, matchedValues);
      }
    }
  }

  /**
   * Formats an archive timestamp in way consistent with GemFire log dates. It
   * will also be formatted to reflect the time zone the archive was created
   * in.
   *
   * @param ts The difference, measured in milliseconds, between the time marked
   *           by this time stamp and midnight, January 1, 1970 UTC.
   */
  public String formatTimeMillis(long ts) {
    synchronized (timeFormatter) {
      return timeFormatter.format(new Date(ts));
    }
  }

  /**
   * sets the time zone this archive was written in.
   */
  void setTimeZone(TimeZone z) {
    timeFormatter.setTimeZone(z);
  }

  /**
   * Returns the time series for this archive.
   */
  TimeStampSeries getTimeStamps() {
    return timeSeries;
  }

  /**
   * Checks to see if the archive has changed since the StatArchiverReader
   * instance was created or last updated. If the archive has additional samples
   * then those are read the resource instances maintained by the reader are
   * updated. <p>Once closed a reader can no longer be updated.
   *
   * @return true if update read some new data.
   * @throws java.io.IOException if <code>archiveName</code> could not be opened
   *                             read, or closed.
   */
  public boolean update(boolean doReset) throws IOException {
    if (this.closed) {
      return false;
    }
    if (!this.updateOK) {
      throw new RuntimeException(
          "Update of this type of file is not supported");
    }

    if (doReset) {
      this.dataIn.reset();
    }

    int updateTokenCount = 0;
    while (this.readToken()) {
      updateTokenCount++;
    }
    return updateTokenCount != 0;
  }

  public void dump(PrintWriter stream) {
    stream.print("archive=" + archive);
    if (info != null) {
      info.dump(stream);
    }
    for (ResourceType aResourceTypeTable : resourceTypeTable) {
      if (aResourceTypeTable != null) {
        aResourceTypeTable.dump(stream);
      }
    }
    stream.print("time=");
    timeSeries.dump(stream);
    for (ResourceInst inst : resourceInstTable) {
      if (inst != null) {
        inst.dump(stream);
      }
    }
  }

  /**
   * Closes the archive.
   */
  public void close() throws IOException {
    if (!this.closed) {
      this.closed = true;
      this.is.close();
      this.dataIn.close();
      this.is = null;
      this.dataIn = null;
      int typeCount = 0;
      if (this.resourceTypeTable != null) { // fix for bug 32320
        for (int i = 0; i < this.resourceTypeTable.length; i++) {
          if (this.resourceTypeTable[i] != null) {
            if (this.resourceTypeTable[i].close()) {
              this.resourceTypeTable[i] = null;
            } else {
              typeCount++;
            }
          }
        }
        ResourceType[] newTypeTable = new ResourceType[typeCount];
        typeCount = 0;
        for (ResourceType aResourceTypeTable : this.resourceTypeTable) {
          if (aResourceTypeTable != null) {
            newTypeTable[typeCount] = aResourceTypeTable;
            typeCount++;
          }
        }
        this.resourceTypeTable = newTypeTable;
      }

      if (this.resourceInstTable != null) { // fix for bug 32320
        int instCount = 0;
        for (int i = 0; i < this.resourceInstTable.length; i++) {
          if (this.resourceInstTable[i] != null) {
            if (this.resourceInstTable[i].close()) {
              this.resourceInstTable[i] = null;
            } else {
              instCount++;
            }
          }
        }
        ResourceInst[] newInstTable = new ResourceInst[instCount];
        instCount = 0;
        for (ResourceInst aResourceInstTable : this.resourceInstTable) {
          if (aResourceInstTable != null) {
            newInstTable[instCount] = aResourceInstTable;
            instCount++;
          }
        }
        this.resourceInstTable = newInstTable;
        this.resourceInstSize = instCount;
      }
      // optimize memory usage of timeSeries now that no more samples
      this.timeSeries.shrink();
      // filters are no longer needed since file will not be read from
      this.filters = null;
    }
  }

  /**
   * Returns global information about the read archive. Returns null if no
   * information is available.
   */
  public ArchiveInfo getArchiveInfo() {
    return this.info;
  }

  private void readHeaderToken() throws IOException {
    byte archiveVersion = dataIn.readByte();
    long startTimeStamp = dataIn.readLong();
    long systemId = dataIn.readLong();
    long systemStartTimeStamp = dataIn.readLong();
    int timeZoneOffset = dataIn.readInt();
    String timeZoneName = dataIn.readUTF();
    String systemDirectory = dataIn.readUTF();
    String productVersion = dataIn.readUTF();
    String os = dataIn.readUTF();
    String machine = dataIn.readUTF();
    if (archiveVersion <= 1) {
      throw new RuntimeException("Archive version " + archiveVersion
          + " is no longer supported");
    }
    if (archiveVersion > ARCHIVE_VERSION) {
      throw new RuntimeException("Unsupported archive version "
          + archiveVersion + ". The supported version is " + ARCHIVE_VERSION);
    }
    this.archiveVersion = archiveVersion;
    this.info = new ArchiveInfo(this, archiveVersion,
        startTimeStamp, systemStartTimeStamp,
        timeZoneOffset, timeZoneName,
        systemDirectory, systemId,
        productVersion, os, machine);
    // Clear all previously read types and instances
    this.resourceInstSize = 0;
    this.resourceInstTable = new ResourceInst[1024];
    this.resourceTypeTable = new ResourceType[256];
    timeSeries.setBase(startTimeStamp);
    if (dump) {
      info.dump(new PrintWriter(System.out));
    }
  }

  boolean loadType(String typeName) {
    // note we don't have instance data or descriptor data yet
    if (filters == null || filters.length == 0) {
      return true;
    } else {
      for (ValueFilter filter : filters) {
        if (filter.typeMatches(typeName)) {
          return true;
        }
      }
      //System.out.println("DEBUG: don't load type=" + typeName);
      return false;
    }
  }

  boolean loadStatDescriptor(StatDescriptor stat, ResourceType type) {
    // note we don't have instance data yet
    if (!type.isLoaded()) {
      return false;
    }
    if (filters == null || filters.length == 0) {
      return true;
    } else {
      for (ValueFilter filter : filters) {
        if (filter.statMatches(stat.getName())
            && filter.typeMatches(type.getName())) {
          return true;
        }
      }
      //System.out.println("DEBUG: don't load stat=" + stat.getName());
      stat.unload();
      return false;
    }
  }

  boolean loadInstance(String textId, long numericId, ResourceType type) {
    if (!type.isLoaded()) {
      return false;
    }
    if (filters == null || filters.length == 0) {
      return true;
    } else {
      for (ValueFilter filter : filters) {
        if (filter.typeMatches(type.getName())) {
          if (filter.instanceMatches(textId, numericId)) {
            StatDescriptor[] stats = type.getStats();
            for (int j = 0; j < stats.length; j++) {
              if (stats[j].isLoaded()) {
                if (filter.statMatches(stats[j].getName())) {
                  return true;
                }
              }
            }
          }
        }
      }
      //System.out.println("DEBUG: don't load instance=" + textId);
      //type.unload();
      return false;
    }
  }

  boolean loadStat(StatDescriptor stat, ResourceInst resource) {
    ResourceType type = resource.getType();
    if (!resource.isLoaded() || !type.isLoaded() || !stat.isLoaded()) {
      return false;
    }
    if (filters == null || filters.length == 0) {
      return true;
    } else {
      String textId = resource.getName();
      long numericId = resource.getId();
      for (ValueFilter filter : filters) {
        if (filter.statMatches(stat.getName())
            && filter.typeMatches(type.getName())
            && filter.instanceMatches(textId, numericId)) {
          return true;
        }
      }
      return false;
    }
  }

  private void readResourceTypeToken() throws IOException {
    int resourceTypeId = dataIn.readInt();
    String resourceTypeName = dataIn.readUTF();
    String resourceTypeDesc = dataIn.readUTF();
    int statCount = dataIn.readUnsignedShort();
    while (resourceTypeId >= resourceTypeTable.length) {
      ResourceType[] tmp = new ResourceType[resourceTypeTable.length + 128];
      System.arraycopy(resourceTypeTable, 0, tmp, 0,
          resourceTypeTable.length);
      resourceTypeTable = tmp;
    }
    assert (resourceTypeTable[resourceTypeId] == null);

    ResourceType rt;
    if (loadType(resourceTypeName)) {
      rt = new ResourceType(
          resourceTypeName,
          resourceTypeDesc,
          statCount);
      if (dump) {
        System.out.println("ResourceType id=" + resourceTypeId
            + " name=" + resourceTypeName
            + " statCount=" + statCount
            + " desc=" + resourceTypeDesc);
      }
    } else {
      rt = new ResourceType(resourceTypeName, statCount);
      if (dump) {
        System.out.println("Not loading ResourceType id=" + resourceTypeId
            + " name=" + resourceTypeName);
      }
    }
    resourceTypeTable[resourceTypeId] = rt;
    for (int i = 0; i < statCount; i++) {
      String statName = dataIn.readUTF();
      byte typeCode = dataIn.readByte();
      boolean isCounter = dataIn.readBoolean();
      boolean largerBetter = isCounter; // default
      if (this.archiveVersion >= 4) {
        largerBetter = dataIn.readBoolean();
      }
      String units = dataIn.readUTF();
      String desc = dataIn.readUTF();
      rt.addStatDescriptor(this, i, statName, isCounter, largerBetter,
          typeCode, units, desc);
      if (dump) {
        System.out.println("  " + i + "=" + statName + " isCtr=" + isCounter
            + " largerBetter=" + largerBetter
            + " typeCode=" + typeCode + " units=" + units
            + " desc=" + desc);
      }
    }
  }

  private void readResourceInstanceCreateToken(
      boolean initialize) throws IOException {
    int resourceInstId = dataIn.readInt();
    String name = dataIn.readUTF();
    long id = dataIn.readLong();
    int resourceTypeId = dataIn.readInt();
    while (resourceInstId >= resourceInstTable.length) {
      ResourceInst[] tmp = new ResourceInst[resourceInstTable.length + 128];
      System.arraycopy(resourceInstTable, 0, tmp, 0,
          resourceInstTable.length);
      resourceInstTable = tmp;
    }
    assert (resourceInstTable[resourceInstId] == null);
    if ((resourceInstId + 1) > this.resourceInstSize) {
      this.resourceInstSize = resourceInstId + 1;
    }
    boolean loadInstance = loadInstance(name, id,
        resourceTypeTable[resourceTypeId]);
    resourceInstTable[resourceInstId] = new ResourceInst(this,
        name, id, resourceTypeTable[resourceTypeId], loadInstance);
    if (dump) {
      System.out.println(
          (loadInstance ? "Loaded" : "Did not load") + " resource instance " + resourceInstId);
      System.out.println(
          "  name=" + name + " id=" + id + " typeId=" + resourceTypeId);
    }
    if (initialize) {
      StatDescriptor[] stats = resourceInstTable[resourceInstId].getType().getStats();
      for (int i = 0; i < stats.length; i++) {
        long v;
        switch (stats[i].getTypeCode()) {
          case BOOLEAN_CODE:
            v = dataIn.readByte();
            break;
          case BYTE_CODE:
          case CHAR_CODE:
            v = dataIn.readByte();
            break;
          case WCHAR_CODE:
            v = dataIn.readUnsignedShort();
            break;
          case SHORT_CODE:
            v = dataIn.readShort();
            break;
          case INT_CODE:
          case FLOAT_CODE:
          case LONG_CODE:
          case DOUBLE_CODE:
            v = readCompactValue();
            break;
          default:
            throw new IOException("Unexpected typecode value"
                + stats[i].getTypeCode());
        }
        resourceInstTable[resourceInstId].initialValue(i, v);
      }
    }
  }

  private void readResourceInstanceDeleteToken() throws IOException {
    int resourceInstId = dataIn.readInt();
    assert (resourceInstTable[resourceInstId] != null);
    resourceInstTable[resourceInstId].makeInactive();
    if (dump) {
      System.out.println("Delete resource instance " + resourceInstId);
    }
  }

  private int readResourceInstId() throws IOException {
      /*
        if (this.archiveVersion <= 1) {
        return dataIn.readInt();
        }
      */
    int token = dataIn.readUnsignedByte();
    if (token <= MAX_BYTE_RESOURCE_INST_ID) {
      return token;
    } else if (token == ILLEGAL_RESOURCE_INST_ID_TOKEN) {
      return ILLEGAL_RESOURCE_INST_ID;
    } else if (token == SHORT_RESOURCE_INST_ID_TOKEN) {
      return dataIn.readUnsignedShort();
    } else { /* token == INT_RESOURCE_INST_ID_TOKEN */
      return dataIn.readInt();
    }
  }

  private int readTimeDelta() throws IOException {
    int result = dataIn.readUnsignedShort();
    if (result == INT_TIMESTAMP_TOKEN) {
      result = dataIn.readInt();
    }
    return result;
  }

  private long readCompactValue() throws IOException {
    long v = dataIn.readByte();
    if (dump) {
      // System.out.print("compactValue(byte1)=" + v);
    }
    if (v < MIN_1BYTE_COMPACT_VALUE) {
      if (v == COMPACT_VALUE_2_TOKEN) {
        v = dataIn.readShort();
        if (dump) {
          //System.out.print("compactValue(short)=" + v);
        }
      } else {
        int bytesToRead = ((byte) v - COMPACT_VALUE_2_TOKEN) + 2;
        v = dataIn.readByte(); // note the first byte will be a signed byte.
        if (dump) {
          System.out.print("compactValue(" + bytesToRead + ")=" + v);
        }
        bytesToRead--;
        while (bytesToRead > 0) {
          v <<= 8;
          v |= dataIn.readUnsignedByte();
          bytesToRead--;
        }
      }
    }
    return v;
  }

  private void readSampleToken() throws IOException {
    int millisSinceLastSample = readTimeDelta();
    if (dump) {
      System.out.println("ts=" + millisSinceLastSample);
    }
    int resourceInstId = readResourceInstId();
    while (resourceInstId != ILLEGAL_RESOURCE_INST_ID) {
      if (dump) {
        System.out.print("  instId=" + resourceInstId);
      }
      StatDescriptor[] stats = resourceInstTable[resourceInstId].getType().getStats();
      int statOffset = dataIn.readUnsignedByte();
      while (statOffset != ILLEGAL_STAT_OFFSET) {
        long statDeltaBits;
        switch (stats[statOffset].getTypeCode()) {
          case BOOLEAN_CODE:
            statDeltaBits = dataIn.readByte();
            break;
          case BYTE_CODE:
          case CHAR_CODE:
            statDeltaBits = dataIn.readByte();
            break;
          case WCHAR_CODE:
            statDeltaBits = dataIn.readUnsignedShort();
            break;
          case SHORT_CODE:
            statDeltaBits = dataIn.readShort();
            break;
          case INT_CODE:
          case FLOAT_CODE:
          case LONG_CODE:
          case DOUBLE_CODE:
            statDeltaBits = readCompactValue();
            break;
          default:
            throw new IOException("Unexepcted typecode value "
                + stats[statOffset].getTypeCode());
        }
        if (resourceInstTable[resourceInstId].addValueSample(statOffset,
            statDeltaBits)) {
          if (dump) {
            System.out.print(" [" + statOffset + "]=" + statDeltaBits);
          }
        }
        statOffset = dataIn.readUnsignedByte();
      }
      if (dump) {
        System.out.println();
      }
      resourceInstId = readResourceInstId();
    }
    timeSeries.addTimeStamp(millisSinceLastSample);
    for (ResourceInst inst : resourceInstTable) {
      if (inst != null && inst.isActive()) {
        inst.addTimeStamp();
      }
    }
  }

  /**
   * Returns true if token read, false if eof.
   */
  private boolean readToken() throws IOException {
    byte token;
    try {
      if (this.updateOK) {
        this.dataIn.mark(BUFFER_SIZE);
      }
      token = this.dataIn.readByte();
      switch (token) {
        case HEADER_TOKEN:
          readHeaderToken();
          break;
        case RESOURCE_TYPE_TOKEN:
          readResourceTypeToken();
          break;
        case RESOURCE_INSTANCE_CREATE_TOKEN:
          readResourceInstanceCreateToken(false);
          break;
        case RESOURCE_INSTANCE_INITIALIZE_TOKEN:
          readResourceInstanceCreateToken(true);
          break;
        case RESOURCE_INSTANCE_DELETE_TOKEN:
          readResourceInstanceDeleteToken();
          break;
        case SAMPLE_TOKEN:
          readSampleToken();
          break;
        default:
          throw new IOException("Unexpected token byte value " + token);
      }
      return true;
    } catch (EOFException ignore) {
      return false;
    }
  }

  /**
   * Returns the approximate amount of memory used to implement this object.
   */
  protected int getMemoryUsed() {
    int result = 0;
    for (int i = 0; i < resourceInstTable.length; i++) {
      if (resourceInstTable[i] != null) {
        result += resourceInstTable[i].getMemoryUsed();
      }
    }
    return result;
  }

//  /**
//   * Creates a StatArchiveReader that will read the named archive file.
//   *
//   * @param autoClose if its <code>true</code> then the reader will close input
//   *                  files as soon as it finds their end.
//   * @throws java.io.IOException if <code>archiveName</code> could not be opened
//   *                             read, or closed.
//   */
//  public StatArchiveFile(String[] archiveNames, ValueFilter[] filters,
//      boolean autoClose)
//      throws IOException {
//    this.archives = new StatArchiveFile[archiveNames.length];
//    this.dump = Boolean.getBoolean("StatArchiveReader.dumpall");
//    for (int i = 0; i < archiveNames.length; i++) {
//      this.archives[i] = new StatArchiveFile(this, new File(archiveNames[i]),
//          dump, filters);
//    }
//
//    update(false, autoClose);
//
//    if (this.dump || Boolean.getBoolean("StatArchiveReader.dump")) {
//      this.dump(new PrintWriter(System.out));
//    }
//  }
//
//  public StatArchiveFile(String[] archiveNames) throws IOException {
//    this(archiveNames, null, true);
//  }

//  /**
//   * Returns an array of stat values that match the specified spec. If nothing
//   * matches then an empty array is returned.
//   */
//  public StatValue[] matchSpec(StatSpec spec) {
//    if (spec.getCombineType() == StatSpec.GLOBAL) {
//      StatValue[] allValues = matchSpec(new RawStatSpec(spec));
//      if (allValues.length == 0) {
//        return allValues;
//      } else {
//        ComboValue cv = new ComboValue(allValues);
//        // need to save this in reader's combo value list
//        return new StatValue[]{cv};
//      }
//    } else {
//      List l = new ArrayList();
//      com.pivotal.jvsd.model.stats.StatArchiveFile.StatArchiveFile[] archives = getArchives();
//      for (int i = 0; i < archives.length; i++) {
//        StatArchiveFile f = archives[i];
//        if (spec.archiveMatches(f.getFile())) {
//          f.matchSpec(spec, l);
//        }
//      }
//      StatValue[] result = new StatValue[l.size()];
//      return (StatValue[]) l.toArray(result);
//    }
//  }

//  /**
//   * Checks to see if any archives have changed since the StatArchiverReader
//   * instance was created or last updated. If an archive has additional samples
//   * then those are read the resource instances maintained by the reader are
//   * updated. <p>Once closed a reader can no longer be updated.
//   *
//   * @return true if update read some new data.
//   * @throws java.io.IOException if an archive could not be opened read, or
//   *                             closed.
//   */
//  public boolean update() throws IOException {
//    return update(true, false);
//  }
//
//  private boolean update(boolean doReset,
//      boolean autoClose) throws IOException {
//    if (this.closed) {
//      return false;
//    }
//    boolean result = false;
//    com.pivotal.jvsd.model.stats.StatArchiveFile.StatArchiveFile[] archives = getArchives();
//    for (int i = 0; i < archives.length; i++) {
//      StatArchiveFile f = archives[i];
//      if (f.update(doReset)) {
//        result = true;
//      }
//      if (autoClose) {
//        f.close();
//      }
//    }
//    return result;
//  }

//  /**
//   * Returns an unmodifiable list of all the {@link ResourceInst} this reader
//   * contains.
//   */
//  public List getResourceInstList() {
//    return new ResourceInstList();
//  }
//
//  public StatArchiveFile[] getArchives() {
//    return this.archives;
//  }


  protected static double bitsToDouble(int type, long bits) {
    switch (type) {
      case BOOLEAN_CODE:
      case BYTE_CODE:
      case CHAR_CODE:
      case WCHAR_CODE:
      case SHORT_CODE:
      case INT_CODE:
      case LONG_CODE:
        return bits;
      case FLOAT_CODE:
        return Float.intBitsToFloat((int) bits);
      case DOUBLE_CODE:
        return Double.longBitsToDouble(bits);
      default:
        throw new RuntimeException("Unexpected typecode: " + type);
    }
  }

  /**
   * Wraps an instance of StatSpec but alwasy returns a combine type of NONE.
   */
  private static class RawStatSpec implements StatSpec {
    private final StatSpec spec;

    RawStatSpec(StatSpec wrappedSpec) {
      this.spec = wrappedSpec;
    }

    public int getCombineType() {
      return StatSpec.NONE;
    }

    public boolean typeMatches(String typeName) {
      return spec.typeMatches(typeName);
    }

    public boolean statMatches(String statName) {
      return spec.statMatches(statName);
    }

    public boolean instanceMatches(String textId, long numericId) {
      return spec.instanceMatches(textId, numericId);
    }

    public boolean archiveMatches(File archive) {
      return spec.archiveMatches(archive);
    }
  }

//  private class ResourceInstList extends AbstractList {
//    protected ResourceInstList() {
//      // nothing needed.
//    }
//
//    @Override
//    public com.pivotal.jvsd.model.stats.StatArchiveFile.ResourceInst get(
//        int idx) {
//      int archiveIdx = 0;
//      com.pivotal.jvsd.model.stats.StatArchiveFile.StatArchiveFile[] archives = getArchives();
//      for (int i = 0; i < archives.length; i++) {
//        StatArchiveFile f = archives[i];
//        if (idx < (archiveIdx + f.resourceInstSize)) {
//          return f.resourceInstTable[idx - archiveIdx];
//        }
//        archiveIdx += f.resourceInstSize;
//      }
//      return null;
//    }
//
//    @Override
//    public int size() {
//      int result = 0;
//      com.pivotal.jvsd.model.stats.StatArchiveFile.StatArchiveFile[] archives = getArchives();
//      for (int i = 0; i < archives.length; i++) {
//        result += archives[i].resourceInstSize;
//      }
//      return result;
//    }
//  }

  /**
   * Describes a single statistic.
   */
  public static class StatDescriptor {
    private boolean loaded;
    private String name;
    private final int offset;
    private final boolean isCounter;
    private final boolean largerBetter;
    private final byte typeCode;
    private String units;
    private String desc;

    protected void dump(PrintWriter stream) {
      stream.println("  " + name + ": type=" + typeCode + " offset=" + offset
          + (isCounter ? " counter" : "")
          + " units=" + units
          + " largerBetter=" + largerBetter
          + " desc=" + desc);
    }

    protected StatDescriptor(String name, int offset, boolean isCounter,
        boolean largerBetter,
        byte typeCode, String units, String desc) {
      this.loaded = true;
      this.name = name;
      this.offset = offset;
      this.isCounter = isCounter;
      this.largerBetter = largerBetter;
      this.typeCode = typeCode;
      this.units = units;
      this.desc = desc;
    }

    public boolean isLoaded() {
      return this.loaded;
    }

    void unload() {
      this.loaded = false;
      this.name = null;
      this.units = null;
      this.desc = null;
    }

    /**
     * Returns the type code of this statistic.
     */
    public byte getTypeCode() {
      return this.typeCode;
    }

    /**
     * Returns the name of this statistic.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns true if this statistic's value will always increase.
     */
    public boolean isCounter() {
      return this.isCounter;
    }

    /**
     * Returns true if larger values indicate better performance.
     */
    public boolean isLargerBetter() {
      return this.largerBetter;
    }

    /**
     * Returns a string that describes the units this statistic measures.
     */
    public String getUnits() {
      return this.units;
    }

    /**
     * Returns a textual description of this statistic.
     */
    public String getDescription() {
      return this.desc;
    }

    /**
     * Returns the offset of this stat in its type.
     */
    public int getOffset() {
      return this.offset;
    }
  }

  public static interface StatValue {
    /**
     * {@link org.apache.geode.jvsd.model.StatArchiveFile.StatValue} filter
     * that causes the statistic values to be unfiltered. This causes the raw
     * values written to the archive to be used. <p>This is the default filter
     * for non-counter statistics. To determine if a statistic is not a counter
     * use {@link org.apache.geode.jvsd.model.StatArchiveFile.StatDescriptor#isCounter}.
     */
    public static final int FILTER_NONE = 0;
    /**
     * {@link org.apache.geode.jvsd.model.StatArchiveFile.StatValue} filter
     * that causes the statistic values to be filtered to reflect how often they
     * change per second.  Since the difference between two samples is used to
     * calculate the value this causes the {@link org.apache.geode.jvsd.model.StatArchiveFile.StatValue}
     * to have one less sample than {@link #FILTER_NONE}. The instance time
     * stamp that does not have a per second value is the instance's first time
     * stamp {@link org.apache.geode.jvsd.model.StatArchiveFile.ResourceInst#getFirstTimeMillis}.
     * <p>This is the default filter for counter statistics. To determine if a
     * statistic is a counter use {@link org.apache.geode.jvsd.model.StatArchiveFile.StatDescriptor#isCounter}.
     */
    public static final int FILTER_PERSEC = 1;
    /**
     * {@link org.apache.geode.jvsd.model.StatArchiveFile.StatValue} filter
     * that causes the statistic values to be filtered to reflect how much they
     * changed between sample periods.  Since the difference between two samples
     * is used to calculate the value this causes the {@link
     * org.apache.geode.jvsd.model.StatArchiveFile.StatValue} to have one less
     * sample than {@link #FILTER_NONE}. The instance time stamp that does not
     * have a per second value is the instance's first time stamp {@link
     * org.apache.geode.jvsd.model.StatArchiveFile.ResourceInst#getFirstTimeMillis}.
     */
    public static final int FILTER_PERSAMPLE = 2;

    /**
     * Creates and returns a trimmed version of this stat value. Any samples
     * taken before <code>startTime</code> and after <code>endTime</code> are
     * discarded from the resulting value. Set a time parameter to
     * <code>-1</code> to not trim that side.
     */
    public StatValue createTrimmed(long startTime, long endTime);

    /**
     * Returns true if value has data that has been trimmed off it by a start
     * timestamp.
     */
    public boolean isTrimmedLeft();

    /**
     * Gets the {@link org.apache.geode.jvsd.model.StatArchiveFile.ResourceType
     * type} of the resources that this value belongs to.
     */
    public ResourceType getType();

    /**
     * Gets the {@link org.apache.geode.jvsd.model.StatArchiveFile.ResourceInst
     * resources} that this value belongs to.
     */
    public ResourceInst[] getResources();

    /**
     * Returns an array of timestamps for each unfiltered snapshot in this
     * value. Each returned time stamp is the number of millis since midnight,
     * Jan 1, 1970 UTC.
     */
    public long[] getRawAbsoluteTimeStamps();

    /**
     * Returns an array of timestamps for each unfiltered snapshot in this
     * value. Each returned time stamp is the number of millis since midnight,
     * Jan 1, 1970 UTC. The resolution is seconds.
     */
    public long[] getRawAbsoluteTimeStampsWithSecondRes();

    /**
     * Returns an array of doubles containing the unfiltered value of this
     * statistic for each point in time that it was sampled.
     */
    public double[] getRawSnapshots();

    /**
     * Returns an array of doubles containing the filtered value of this
     * statistic for each point in time that it was sampled.
     */
    public double[] getSnapshots();

    /**
     * Returns the number of samples taken of this statistic's value.
     */
    public int getSnapshotsSize();

    /**
     * Returns the smallest of all the samples taken of this statistic's value.
     */
    public double getSnapshotsMinimum();

    /**
     * Returns the largest of all the samples taken of this statistic's value.
     */
    public double getSnapshotsMaximum();

    /**
     * Returns the average of all the samples taken of this statistic's value.
     */
    public double getSnapshotsAverage();

    /**
     * Returns the standard deviation of all the samples taken of this
     * statistic's value.
     */
    public double getSnapshotsStandardDeviation();

    /**
     * Returns the most recent value of all the samples taken of this
     * statistic's value.
     */
    public double getSnapshotsMostRecent();

    /**
     * Returns true if sample whose value was different from previous values has
     * been added to this StatValue since the last time this method was called.
     */
    public boolean hasValueChanged();

    /**
     * Returns the current filter used to calculate this statistic's values. It
     * will be one of these values: <ul> <li> {@link #FILTER_NONE} <li> {@link
     * #FILTER_PERSAMPLE} <li> {@link #FILTER_PERSEC} </ul>
     */
    public int getFilter();

    /**
     * Sets the current filter used to calculate this statistic's values. The
     * default filter is {@link #FILTER_NONE} unless the statistic is a counter,
     * {@link org.apache.geode.jvsd.model.StatArchiveFile.StatDescriptor#isCounter},
     * in which case its {@link #FILTER_PERSEC}.
     *
     * @param filter It must be one of these values: <ul> <li> {@link
     *               #FILTER_NONE} <li> {@link #FILTER_PERSAMPLE} <li> {@link
     *               #FILTER_PERSEC} </ul>
     * @throws IllegalArgumentException if <code>filter</code> is not a valid
     *                                  filter constant.
     */
    public void setFilter(int filter);

    /**
     * Returns a description of this statistic.
     */
    public StatDescriptor getDescriptor();
  }

  protected static abstract class AbstractValue implements StatValue {
    protected StatDescriptor descriptor;
    protected int filter;

    protected long startTime = -1;
    protected long endTime = -1;

    protected boolean statsValid = false;
    protected int size;
    protected double min;
    protected double max;
    protected double avg;
    protected double stddev;
    protected double mostRecent;

    public void calcStats() {
      if (!statsValid) {
        getSnapshots();
      }
    }

    public int getSnapshotsSize() {
      calcStats();
      return this.size;
    }

    public double getSnapshotsMinimum() {
      calcStats();
      return this.min;
    }

    public double getSnapshotsMaximum() {
      calcStats();
      return this.max;
    }

    public double getSnapshotsAverage() {
      calcStats();
      return this.avg;
    }

    public double getSnapshotsStandardDeviation() {
      calcStats();
      return this.stddev;
    }

    public double getSnapshotsMostRecent() {
      calcStats();
      return this.mostRecent;
    }

    public StatDescriptor getDescriptor() {
      return this.descriptor;
    }

    public int getFilter() {
      return this.filter;
    }

    public void setFilter(int filter) {
      if (filter != this.filter) {
        if (filter != FILTER_NONE
            && filter != FILTER_PERSEC
            && filter != FILTER_PERSAMPLE) {
          throw new IllegalArgumentException("Filter value " + filter
              + " must be " + FILTER_NONE + ", " + FILTER_PERSEC + " or "
              + FILTER_PERSAMPLE);
        }
        this.filter = filter;
        this.statsValid = false;
      }
    }

    /**
     * Calculates each stat given the result of calling getSnapshots
     */
    protected void calcStats(double[] values) {
      if (statsValid) {
        return;
      }
      size = values.length;
      if (size == 0) {
        min = 0.0;
        max = 0.0;
        avg = 0.0;
        stddev = 0.0;
        mostRecent = 0.0;
      } else {
        min = values[0];
        max = values[0];
        mostRecent = values[values.length - 1];
        double total = values[0];
        for (int i = 1; i < size; i++) {
          total += values[i];
          if (values[i] < min) {
            min = values[i];
          } else if (values[i] > max) {
            max = values[i];
          }
        }
        avg = total / size;
        stddev = 0.0;
        if (size > 1) {
          for (int i = 0; i < size; i++) {
            double dv = values[i] - avg;
            stddev += (dv * dv);
          }
          stddev /= (size - 1);
          stddev = Math.sqrt(stddev);
        }
      }
      statsValid = true;
    }

    /**
     * Returns a string representation of this object.
     */
    @Override
    public String toString() {
      calcStats();
      StringBuilder result = new StringBuilder();
      result.append(getDescriptor().getName());
      String units = getDescriptor().getUnits();
      if (units != null && units.length() > 0) {
        result.append(' ').append(units);
      }
      if (filter == FILTER_PERSEC) {
        result.append("/sec");
      } else if (filter == FILTER_PERSAMPLE) {
        result.append("/sample");
      }
      result.append(": samples=")
          .append(getSnapshotsSize());
      if (startTime != -1) {
        result.append(" startTime=\"")
            .append(new Date(startTime))
            .append("\"");
      }
      if (endTime != -1) {
        result.append(" endTime=\"")
            .append(new Date(endTime))
            .append("\"");
      }
      result.append(" min=")
          .append(nf.format(min));
      result.append(" max=")
          .append(nf.format(max));
      result.append(" average=")
          .append(nf.format(avg));
      result.append(" stddev=")
          .append(nf.format(stddev));
      result.append(" last=") // for bug 42532
          .append(nf.format(mostRecent));
      return result.toString();
    }
  }

  /**
   * A ComboValue is a value that is the logical combination of a set of other
   * stat values. <p> For now ComboValue has a simple implementation that does
   * not suppport updates.
   */
  private static class ComboValue extends AbstractValue {
    private final ResourceType type;
    private final StatValue[] values;

    /**
     * Creates a ComboValue by adding all the specified values together.
     */
    ComboValue(List<AbstractValue> valueList) {
      this((StatValue[]) valueList.toArray(new StatValue[valueList.size()]));
    }

    /**
     * Creates a ComboValue by adding all the specified values together.
     */
    ComboValue(StatValue[] values) {
      this.values = values;
      this.filter = this.values[0].getFilter();
      String typeName = this.values[0].getType().getName();
      String statName = this.values[0].getDescriptor().getName();
      int bestTypeIdx = 0;
      for (int i = 1; i < this.values.length; i++) {
        if (this.filter != this.values[i].getFilter()) {
          /* I'm not sure why this would happen.
           * If it really can happen then this code should change
           * the filter since a client has no way to select values
           * based on the filter.
           */
          throw new IllegalArgumentException(
              "Can't combine values with different filters");
        }
        if (!typeName.equals(this.values[i].getType().getName())) {
          throw new IllegalArgumentException(
              "Can't combine values with different types");
        }
        if (!statName.equals(this.values[i].getDescriptor().getName())) {
          throw new IllegalArgumentException("Can't combine different stats");
        }
        if (this.values[i].getDescriptor().isCounter()) {
          // its a counter which is not the default
          if (!this.values[i].getDescriptor().isLargerBetter()) {
            // this guy has non-defaults for both use him
            bestTypeIdx = i;
          } else if (this.values[bestTypeIdx].getDescriptor().isCounter()
              == this.values[bestTypeIdx].getDescriptor().isLargerBetter()) {
            // as long as we haven't already found a guy with defaults
            // make this guy the best type
            bestTypeIdx = i;
          }
        } else {
          // its a gauge, see if it has a non-default largerBetter
          if (this.values[i].getDescriptor().isLargerBetter()) {
            // as long as we haven't already found a guy with defaults
            if (this.values[bestTypeIdx].getDescriptor().isCounter()
                == this.values[bestTypeIdx].getDescriptor().isLargerBetter()) {
              // make this guy the best type
              bestTypeIdx = i;
            }
          }
        }
      }
      this.type = this.values[bestTypeIdx].getType();
      this.descriptor = this.values[bestTypeIdx].getDescriptor();
    }

    private ComboValue(ComboValue original, long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.type = original.getType();
      this.descriptor = original.getDescriptor();
      this.filter = original.getFilter();
      this.values = new StatValue[original.values.length];
      for (int i = 0; i < this.values.length; i++) {
        this.values[i] = original.values[i].createTrimmed(startTime, endTime);
      }
    }

    public StatValue createTrimmed(long startTime, long endTime) {
      if (startTime == this.startTime && endTime == this.endTime) {
        return this;
      } else {
        return new ComboValue(this, startTime, endTime);
      }
    }

    public ResourceType getType() {
      return this.type;
    }

    public ResourceInst[] getResources() {
      Set<ResourceInst> set = new HashSet<ResourceInst>();
      for (StatValue value : values) {
        set.addAll(Arrays.asList(value.getResources()));
      }
      ResourceInst[] result = new ResourceInst[set.size()];
      return (ResourceInst[]) set.toArray(result);
    }

    public boolean hasValueChanged() {
      return true;
    }

    public static boolean closeEnough(long v1, long v2, long delta) {
      return (v1 == v2) || ((Math.abs(v1 - v2) / 2) <= delta);
    }

    /**
     * Return true if v is closer to prev. Return false if v is closer to next.
     * Return true if v is the same distance from both.
     */
    public static boolean closer(long v, long prev, long next) {
      return Math.abs(v - prev) <= Math.abs(v - next);
    }


    /**
     * Return true if the current ts must be inserted instead of being mapped to
     * the tsAtInsertPoint
     */
    private static boolean mustInsert(int nextIdx, long[] valueTimeStamps,
        long tsAtInsertPoint) {
      return (nextIdx < valueTimeStamps.length)
          && (valueTimeStamps[nextIdx] <= tsAtInsertPoint);
    }

    public long[] getRawAbsoluteTimeStampsWithSecondRes() {
      return getRawAbsoluteTimeStamps();
    }

    public long[] getRawAbsoluteTimeStamps() {
      if (values.length == 0) {
        return new long[0];
      }
//       for (int i=0; i < values.length; i++) {
//         System.out.println("DEBUG: inst# " + i + " has "
//                            + values[i].getRawAbsoluteTimeStamps().length
//                            + " timestamps");
//       }
      long[] valueTimeStamps = values[0].getRawAbsoluteTimeStamps();
      int tsCount = valueTimeStamps.length + 1;
      long[] ourTimeStamps = new long[(tsCount * 2) + 1];
      System.arraycopy(valueTimeStamps, 0, ourTimeStamps, 0,
          valueTimeStamps.length);
      // Note we add a MAX sample to make the insert logic simple
      ourTimeStamps[valueTimeStamps.length] = Long.MAX_VALUE;
      for (int i = 1; i < values.length; i++) {
        valueTimeStamps = values[i].getRawAbsoluteTimeStamps();
        if (valueTimeStamps.length == 0) {
          continue;
        }
        int ourIdx = 0;
        int j = 0;
        long tsToInsert = valueTimeStamps[0] - 1000; // default to 1 second
        if (valueTimeStamps.length > 1) {
          tsToInsert = valueTimeStamps[0] - (valueTimeStamps[1] - valueTimeStamps[0]);
        }
        // tsToInsert is now initialized to a value that we can pretend
        // was the previous timestamp inserted.
        while (j < valueTimeStamps.length) {
          long timeDelta = (valueTimeStamps[j] - tsToInsert) / 2;
          tsToInsert = valueTimeStamps[j];
          long tsAtInsertPoint = ourTimeStamps[ourIdx];
          while (tsToInsert > tsAtInsertPoint
              && !closeEnough(tsToInsert, tsAtInsertPoint, timeDelta)) {
//             System.out.println("DEBUG: skipping " + ourIdx + " because it was not closeEnough");
            ourIdx++;
            tsAtInsertPoint = ourTimeStamps[ourIdx];
          }
          if (closeEnough(tsToInsert, tsAtInsertPoint, timeDelta)
              && !mustInsert(j + 1, valueTimeStamps, tsAtInsertPoint)) {
            // It was already in our list so just go to the next one
            j++;
            ourIdx++; // never put the next timestamp at this index
            while (!closer(tsToInsert, ourTimeStamps[ourIdx - 1],
                ourTimeStamps[ourIdx])
                && !mustInsert(j, valueTimeStamps, ourTimeStamps[ourIdx])) {
//               System.out.println("DEBUG: skipping mergeTs[" + (ourIdx-1) + "]="
//                                  + tsAtInsertPoint + " because it was closer to the next one");
              ourIdx++; // it is closer to the next one so skip forward on more
            }
          } else {
            // its not in our list so add it
            int endRunIdx = j + 1;
            while (endRunIdx < valueTimeStamps.length
                && valueTimeStamps[endRunIdx] < tsAtInsertPoint
                && !closeEnough(valueTimeStamps[endRunIdx], tsAtInsertPoint,
                timeDelta)) {
              endRunIdx++;
            }
            int numToCopy = endRunIdx - j;
//             System.out.println("DEBUG: tsToInsert=" + tsToInsert
//                                + " tsAtInsertPoint=" + tsAtInsertPoint
//                                + " timeDelta=" + timeDelta
//                                + " vDelta=" + (Math.abs(tsToInsert-tsAtInsertPoint)/2)
//                                + " numToCopy=" + numToCopy);
//             if (j > 0) {
//               System.out.println("DEBUG: prevTsToInsert=" + valueTimeStamps[j-1]);
//             }
//             if (ourIdx > 0) {
//               System.out.println("DEBUG ourTimeStamps[" + (ourIdx-1) + "]=" + ourTimeStamps[ourIdx-1]);
//             }

//             if (numToCopy > 1) {
//               System.out.println("DEBUG: endRunTs=" + valueTimeStamps[endRunIdx-1]);
//             }
            if (tsCount + numToCopy > ourTimeStamps.length) {
              // grow our timestamp array
              long[] tmp = new long[(tsCount + numToCopy) * 2];
              System.arraycopy(ourTimeStamps, 0, tmp, 0, tsCount);
              ourTimeStamps = tmp;
            }
            // make room for insert
            System.arraycopy(ourTimeStamps, ourIdx,
                ourTimeStamps, ourIdx + numToCopy,
                tsCount - ourIdx);
            // insert the elements
            if (numToCopy == 1) {
              ourTimeStamps[ourIdx] = valueTimeStamps[j];
            } else {
              System.arraycopy(valueTimeStamps, j,
                  ourTimeStamps, ourIdx,
                  numToCopy);
            }
            ourIdx += numToCopy;
            tsCount += numToCopy;
            // skip over all inserted elements
            j += numToCopy;
          }
//           System.out.println("DEBUG: inst #" + i
//                              + " valueTs[" + (j-1) + "]=" + tsToInsert
//                              + " found/inserted at"
//                              + " mergeTs[" + (ourIdx-1) + "]=" + ourTimeStamps[ourIdx-1]);
        }
      }
//       for (int i=0; i < tsCount; i++) {
//         System.out.println("DEBUG: mergedTs[" + i + "]=" + ourTimeStamps[i]);
//         if (i > 0 && ourTimeStamps[i] <= ourTimeStamps[i-1]) {
//           System.out.println("DEBUG: ERROR ts was not greater than previous");
//         }
//       }
      // Now make one more pass over all the timestamps and make sure they
      // will all fit into the current merged timestamps in ourTimeStamps
//       boolean changed;
//       do {
//         changed = false;
//         for (int i=0; i < values.length; i++) {
//           valueTimeStamps = values[i].getRawAbsoluteTimeStamps();
//           if (valueTimeStamps.length == 0) {
//             continue;
//           }
//           int ourIdx = 0;
//           for (int j=0; j < valueTimeStamps.length; j++) {
//             while ((ourIdx < (tsCount-1))
//                    && !isClosest(valueTimeStamps[j], ourTimeStamps, ourIdx)) {
//               ourIdx++;
//             }
//             if (ourIdx == (tsCount-1)) {
//               // we are at the end so we need to append all our remaining stamps [j..valueTimeStamps.length-1]
//               // and then reappend the Long.MAX_VALUE that
//               // is currently at tsCount-1
//               int numToCopy = valueTimeStamps.length - j;
//               if (tsCount+numToCopy > ourTimeStamps.length) {
//                 // grow our timestamp array
//                 long[] tmp = new long[tsCount+numToCopy+1];
//                 System.arraycopy(ourTimeStamps, 0, tmp, 0, tsCount);
//                 ourTimeStamps = tmp;
//               }
//               System.arraycopy(valueTimeStamps, j,
//                                ourTimeStamps, ourIdx,
//                                numToCopy);
//               tsCount += numToCopy;
//               ourTimeStamps[tsCount-1] = Long.MAX_VALUE;
//               //changed = true;
//               System.out.println("DEBUG: had to add " + numToCopy
//                                  + " timestamps for inst#" + i + " starting at index " + j + " starting with ts=" + valueTimeStamps[j]);
//               break; // our of the for j loop since we just finished off this guy
//             } else {
//               ourIdx++;
//             }
//           }
//         }
//       } while (changed);
      // remove the max ts we added
      tsCount--;
      {
        int startIdx = 0;
        int endIdx = tsCount - 1;
        if (startTime != -1) {
          assert (ourTimeStamps[startIdx] >= startTime);
        }
        if (endTime != -1) {
          assert (endIdx == startIdx - 1 || ourTimeStamps[endIdx] < endTime);
        }
        tsCount = (endIdx - startIdx) + 1;

        // shrink and trim our timestamp array
        long[] tmp = new long[tsCount];
        System.arraycopy(ourTimeStamps, startIdx, tmp, 0, tsCount);
        ourTimeStamps = tmp;
      }
      return ourTimeStamps;
    }

    public double[] getRawSnapshots() {
      return getRawSnapshots(getRawAbsoluteTimeStamps());
    }

    /**
     * Returns true if the timeStamp at curIdx is the one that ts is the closest
     * to. We know that timeStamps[curIdx-1], if it exists, was not the
     * closest.
     */
    private static boolean isClosest(long ts, long[] timeStamps, int curIdx) {
      if (curIdx >= (timeStamps.length - 1)) {
        // curIdx is the last one so it must be the closest
        return true;
      }
      if (ts == timeStamps[curIdx]) {
        return true;
      }
      return closer(ts, timeStamps[curIdx], timeStamps[curIdx + 1]);
    }

    public boolean isTrimmedLeft() {
      for (StatValue value : this.values) {
        if (value.isTrimmedLeft()) {
          return true;
        }
      }
      return false;
    }

    private double[] getRawSnapshots(long[] ourTimeStamps) {
      double[] result = new double[ourTimeStamps.length];
//       System.out.println("DEBUG: producing " + result.length + " values");
      if (result.length > 0) {
        for (StatValue value : values) {
          long[] valueTimeStamps = value.getRawAbsoluteTimeStamps();
          double[] valueSnapshots = value.getRawSnapshots();
          double currentValue = 0.0;
          int curIdx = 0;
          if (value.isTrimmedLeft() && valueSnapshots.length > 0) {
            currentValue = valueSnapshots[0];
          }
//           System.out.println("DEBUG: inst#" + i + " has " + valueSnapshots.length + " values");
          for (int j = 0; j < valueSnapshots.length; j++) {
//             System.out.println("DEBUG: Doing v with"
//                                + " vTs[" + j + "]=" + valueTimeStamps[j]
//                                + " at mergeTs[" + curIdx + "]=" + ourTimeStamps[curIdx]);
            while (!isClosest(valueTimeStamps[j], ourTimeStamps, curIdx)) {
              if (descriptor.isCounter()) {
                result[curIdx] += currentValue;
              }
//               System.out.println("DEBUG: skipping curIdx=" + curIdx
//                                  + " valueTimeStamps[" + j + "]=" + valueTimeStamps[j]
//                                  + " ourTimeStamps[" + curIdx + "]=" + ourTimeStamps[curIdx]
//                                  + " ourTimeStamps[" + (curIdx+1) + "]=" + ourTimeStamps[curIdx+1]);

              curIdx++;
            }
            if (curIdx >= result.length) {
              // Add this to workaround bug 30288
              int samplesSkipped = valueSnapshots.length - j;
              StringBuilder msg = new StringBuilder(100);
              msg.append("WARNING: dropping last ");
              if (samplesSkipped == 1) {
                msg.append("sample because it");
              } else {
                msg.append(samplesSkipped).append(" samples because they");
              }
              msg.append(" could not fit in the merged result.");
              System.out.println(msg.toString());
              break;
            }
            currentValue = valueSnapshots[j];
            result[curIdx] += currentValue;
            curIdx++;
          }
          if (descriptor.isCounter()) {
            for (int j = curIdx; j < result.length; j++) {
              result[j] += currentValue;
            }
          }
        }
      }
      return result;
    }

    public double[] getSnapshots() {
      double[] result;
      if (filter != FILTER_NONE) {
        long[] timestamps = getRawAbsoluteTimeStamps();
        double[] snapshots = getRawSnapshots(timestamps);
        if (snapshots.length <= 1) {
          return new double[0];
        }
        result = new double[snapshots.length - 1];
        for (int i = 0; i < result.length; i++) {
          double valueDelta = snapshots[i + 1] - snapshots[i];
          if (filter == FILTER_PERSEC) {
            long timeDelta = timestamps[i + 1] - timestamps[i];
            result[i] = valueDelta / (timeDelta / 1000.0);
//             if (result[i] > valueDelta) {
//               System.out.println("DEBUG:  timeDelta     was " + timeDelta + " ms.");
//               System.out.println("DEBUG: valueDelta     was " + valueDelta);
//               System.out.println("DEBUG: valueDelta/sec was " + result[i]);
//             }
          } else {
            result[i] = valueDelta;
          }
        }
      } else {
        result = getRawSnapshots();
      }
      calcStats(result);
      return result;
    }
  }

  /**
   * Provides the value series related to a single statistics.
   */
  private static class SimpleValue extends AbstractValue {
    private final ResourceInst resource;

    private boolean useNextBits = false;
    private long nextBits;
    private final BitSeries series;
    private boolean valueChangeNoticed = false;


    public StatValue createTrimmed(long startTime, long endTime) {
      if (startTime == this.startTime && endTime == this.endTime) {
        return this;
      } else {
        return new SimpleValue(this, startTime, endTime);
      }
    }

    protected SimpleValue(ResourceInst resource, StatDescriptor sd) {
      this.resource = resource;
      if (sd.isCounter()) {
        this.filter = FILTER_PERSEC;
      } else {
        this.filter = FILTER_NONE;
      }
      this.descriptor = sd;
      this.series = new BitSeries();
      this.statsValid = false;
    }

    private SimpleValue(SimpleValue in, long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.useNextBits = in.useNextBits;
      this.nextBits = in.nextBits;
      this.resource = in.resource;
      this.series = in.series;
      this.descriptor = in.descriptor;
      this.filter = in.filter;
      this.statsValid = false;
      this.valueChangeNoticed = true;
    }

    public ResourceType getType() {
      return this.resource.getType();
    }

    public ResourceInst[] getResources() {
      return new ResourceInst[]{this.resource};
    }

    public boolean isTrimmedLeft() {
      return getStartIdx() != 0;
    }

    private int getStartIdx() {
      int startIdx = 0;
      if (startTime != -1) {
        long startTimeStamp = startTime - resource.getTimeBase();
        long[] timestamps = resource.getAllRawTimeStamps();
        for (int i = resource.getFirstTimeStampIdx();
             i < resource.getFirstTimeStampIdx() + series.getSize();
             i++) {
          if (timestamps[i] >= startTimeStamp) {
            break;
          }
          startIdx++;
        }
      }
      return startIdx;
    }

    private int getEndIdx(int startIdx) {
      int endIdx = series.getSize() - 1;
      if (endTime != -1) {
        long endTimeStamp = endTime - resource.getTimeBase();
        long[] timestamps = resource.getAllRawTimeStamps();
        endIdx = startIdx - 1;
        for (int i = resource.getFirstTimeStampIdx() + startIdx;
             i < resource.getFirstTimeStampIdx() + series.getSize();
             i++) {
          if (timestamps[i] >= endTimeStamp) {
            break;
          }
          endIdx++;
        }
        assert (endIdx == startIdx - 1 || timestamps[endIdx] < endTimeStamp);
      }
      return endIdx;
    }

    public double[] getSnapshots() {
      double[] result;
      int startIdx = getStartIdx();
      int endIdx = getEndIdx(startIdx);
      int resultSize = (endIdx - startIdx) + 1;

      if (filter != FILTER_NONE && resultSize > 1) {
        long[] timestamps = null;
        if (filter == FILTER_PERSEC) {
          timestamps = resource.getAllRawTimeStamps();
        }
        result = new double[resultSize - 1];
        int tsIdx = resource.getFirstTimeStampIdx() + startIdx;
        double[] values = series.getValuesEx(descriptor.getTypeCode(), startIdx,
            resultSize);
        for (int i = 0; i < result.length; i++) {
          double valueDelta = values[i + 1] - values[i];
          if (filter == FILTER_PERSEC) {
            double timeDelta = (timestamps[tsIdx + i + 1] - timestamps[tsIdx + i]); // millis
            valueDelta /= (timeDelta / 1000); // per second
          }
          result[i] = valueDelta;
        }
      } else {
        result = series.getValuesEx(descriptor.getTypeCode(), startIdx,
            resultSize);
      }
      calcStats(result);
      return result;
    }

    public double[] getRawSnapshots() {
      int startIdx = getStartIdx();
      int endIdx = getEndIdx(startIdx);
      int resultSize = (endIdx - startIdx) + 1;
      return series.getValuesEx(descriptor.getTypeCode(), startIdx, resultSize);
    }

    public long[] getRawAbsoluteTimeStampsWithSecondRes() {
      long[] result = getRawAbsoluteTimeStamps();
      for (int i = 0; i < result.length; i++) {
        result[i] += 500;
        result[i] /= 1000;
        result[i] *= 1000;
      }
      return result;
    }

    public long[] getRawAbsoluteTimeStamps() {
      int startIdx = getStartIdx();
      int endIdx = getEndIdx(startIdx);
      int resultSize = (endIdx - startIdx) + 1;
      if (resultSize <= 0) {
        return new long[0];
      } else {
        long[] result = new long[resultSize];
        long[] timestamps = resource.getAllRawTimeStamps();
        int tsIdx = resource.getFirstTimeStampIdx() + startIdx;
        long base = resource.getTimeBase();
        for (int i = 0; i < resultSize; i++) {
          result[i] = base + timestamps[tsIdx + i];
        }
        return result;
      }
    }

    public boolean hasValueChanged() {
      if (valueChangeNoticed) {
        valueChangeNoticed = false;
        return true;
      } else {
        return false;
      }
    }

    protected int getMemoryUsed() {
      int result = 0;
      if (series != null) {
        result += series.getMemoryUsed();
      }
      return result;
    }

    protected void dump(PrintWriter stream) {
      calcStats();
      stream.print("  " + descriptor.getName() + "=");
      stream.print("[size=" + getSnapshotsSize()
          + " min=" + nf.format(min)
          + " max=" + nf.format(max)
          + " avg=" + nf.format(avg)
          + " stddev=" + nf.format(stddev) + "]");
      if (Boolean.getBoolean("StatArchiveReader.dumpall")) {
        series.dump(stream);
      } else {
        stream.println();
      }
    }

    protected void shrink() {
      this.series.shrink();
    }

    protected void initialValue(long v) {
      this.series.initialBits(v);
    }

    protected void prepareNextBits(long bits) {
      useNextBits = true;
      nextBits = bits;
    }

    protected void addSample() {
      statsValid = false;
      if (useNextBits) {
        useNextBits = false;
        series.addBits(nextBits);
        valueChangeNoticed = true;
      } else {
        series.addBits(0);
      }
    }
  }

  private static abstract class BitInterval {
    /**
     * Returns number of items added to values
     */
    abstract int fill(double[] values, int valueOffset, int typeCode,
        int skipCount);

    abstract void dump(PrintWriter stream);

    abstract boolean attemptAdd(long addBits, long addInterval, int addCount);

    int getMemoryUsed() {
      return 0;
    }

    protected int count;

    public final int getSampleCount() {
      return this.count;
    }

    static BitInterval create(long bits, long interval, int count) {
      if (interval == 0) {
        if (bits <= Integer.MAX_VALUE && bits >= Integer.MIN_VALUE) {
          return new BitZeroIntInterval((int) bits, count);
        } else {
          return new BitZeroLongInterval(bits, count);
        }
      } else if (count <= 3) {
        if (interval <= Byte.MAX_VALUE && interval >= Byte.MIN_VALUE) {
          return new BitExplicitByteInterval(bits, interval, count);
        } else if (interval <= Short.MAX_VALUE && interval >= Short.MIN_VALUE) {
          return new BitExplicitShortInterval(bits, interval, count);
        } else if (interval <= Integer.MAX_VALUE && interval >= Integer.MIN_VALUE) {
          return new BitExplicitIntInterval(bits, interval, count);
        } else {
          return new BitExplicitLongInterval(bits, interval, count);
        }
      } else {
        boolean smallBits = false;
        boolean smallInterval = false;
        if (bits <= Integer.MAX_VALUE && bits >= Integer.MIN_VALUE) {
          smallBits = true;
        }
        if (interval <= Integer.MAX_VALUE && interval >= Integer.MIN_VALUE) {
          smallInterval = true;
        }
        if (smallBits) {
          if (smallInterval) {
            return new BitNonZeroIntIntInterval((int) bits, (int) interval,
                count);
          } else {
            return new BitNonZeroIntLongInterval((int) bits, interval, count);
          }
        } else {
          if (smallInterval) {
            return new BitNonZeroLongIntInterval(bits, (int) interval, count);
          } else {
            return new BitNonZeroLongLongInterval(bits, interval, count);
          }
        }
      }
    }
  }

  private static abstract class BitNonZeroInterval extends BitInterval {
    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 4;
    }

    abstract long getBits();

    abstract long getInterval();

    @Override
    int fill(double[] values, int valueOffset, int typeCode, int skipCount) {
      int fillcount = values.length - valueOffset; // space left in values
      int maxCount = count - skipCount; // maximum values this interval can produce
      if (fillcount > maxCount) {
        fillcount = maxCount;
      }
      long base = getBits();
      long interval = getInterval();
      base += skipCount * interval;
      for (int i = 0; i < fillcount; i++) {
        values[valueOffset + i] = bitsToDouble(typeCode, base);
        base += interval;
      }
      return fillcount;
    }

    @Override
    void dump(PrintWriter stream) {
      stream.print(getBits());
      if (count > 1) {
        long interval = getInterval();
        if (interval != 0) {
          stream.print("+=" + interval);
        }
        stream.print("r" + count);
      }
    }

    BitNonZeroInterval(int count) {
      this.count = count;
    }

    @Override
    boolean attemptAdd(long addBits, long addInterval, int addCount) {
      // addCount >= 2; count >= 2
      if (addInterval == getInterval()) {
        if (addBits == (getBits() + (addInterval * (count - 1)))) {
          count += addCount;
          return true;
        }
      }
      return false;
    }
  }

  private static class BitNonZeroIntIntInterval extends BitNonZeroInterval {
    int bits;
    int interval;

    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 8;
    }

    @Override
    long getBits() {
      return this.bits;
    }

    @Override
    long getInterval() {
      return this.interval;
    }

    BitNonZeroIntIntInterval(int bits, int interval, int count) {
      super(count);
      this.bits = bits;
      this.interval = interval;
    }
  }

  private static class BitNonZeroIntLongInterval extends BitNonZeroInterval {
    int bits;
    long interval;

    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 12;
    }

    @Override
    long getBits() {
      return this.bits;
    }

    @Override
    long getInterval() {
      return this.interval;
    }

    BitNonZeroIntLongInterval(int bits, long interval, int count) {
      super(count);
      this.bits = bits;
      this.interval = interval;
    }
  }

  private static class BitNonZeroLongIntInterval extends BitNonZeroInterval {
    long bits;
    int interval;

    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 12;
    }

    @Override
    long getBits() {
      return this.bits;
    }

    @Override
    long getInterval() {
      return this.interval;
    }

    BitNonZeroLongIntInterval(long bits, int interval, int count) {
      super(count);
      this.bits = bits;
      this.interval = interval;
    }
  }

  private static class BitNonZeroLongLongInterval extends BitNonZeroInterval {
    long bits;
    long interval;

    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 16;
    }

    @Override
    long getBits() {
      return this.bits;
    }

    @Override
    long getInterval() {
      return this.interval;
    }

    BitNonZeroLongLongInterval(long bits, long interval, int count) {
      super(count);
      this.bits = bits;
      this.interval = interval;
    }
  }

  private static abstract class BitZeroInterval extends BitInterval {
    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 4;
    }

    abstract long getBits();

    @Override
    int fill(double[] values, int valueOffset, int typeCode, int skipCount) {
      int fillcount = values.length - valueOffset; // space left in values
      int maxCount = count - skipCount; // maximum values this interval can produce
      if (fillcount > maxCount) {
        fillcount = maxCount;
      }
      double value = bitsToDouble(typeCode, getBits());
      for (int i = 0; i < fillcount; i++) {
        values[valueOffset + i] = value;
      }
      return fillcount;
    }

    @Override
    void dump(PrintWriter stream) {
      stream.print(getBits());
      if (count > 1) {
        stream.print("r" + count);
      }
    }

    BitZeroInterval(int count) {
      this.count = count;
    }

    @Override
    boolean attemptAdd(long addBits, long addInterval, int addCount) {
      // addCount >= 2; count >= 2
      if (addInterval == 0 && addBits == getBits()) {
        count += addCount;
        return true;
      }
      return false;
    }
  }

  private static class BitZeroIntInterval extends BitZeroInterval {
    int bits;

    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 4;
    }

    @Override
    long getBits() {
      return bits;
    }

    BitZeroIntInterval(int bits, int count) {
      super(count);
      this.bits = bits;
    }
  }

  private static class BitZeroLongInterval extends BitZeroInterval {
    long bits;

    @Override
    int getMemoryUsed() {
      return super.getMemoryUsed() + 8;
    }

    @Override
    long getBits() {
      return bits;
    }

    BitZeroLongInterval(long bits, int count) {
      super(count);
      this.bits = bits;
    }
  }

  private static class BitExplicitByteInterval extends BitInterval {
    long firstValue;
    long lastValue;
    byte[] bitIntervals = null;

    @Override
    int getMemoryUsed() {
      int result = super.getMemoryUsed() + 4 + 8 + 8 + 4;
      if (bitIntervals != null) {
        result += bitIntervals.length;
      }
      return result;
    }

    @Override
    int fill(double[] values, int valueOffset, int typeCode, int skipCount) {
      int fillcount = values.length - valueOffset; // space left in values
      int maxCount = count - skipCount; // maximum values this interval can produce
      if (fillcount > maxCount) {
        fillcount = maxCount;
      }
      long bitValue = firstValue;
      for (int i = 0; i < skipCount; i++) {
        bitValue += bitIntervals[i];
      }
      for (int i = 0; i < fillcount; i++) {
        bitValue += bitIntervals[skipCount + i];
        values[valueOffset + i] = bitsToDouble(typeCode, bitValue);
      }
      return fillcount;
    }

    @Override
    void dump(PrintWriter stream) {
      stream.print("(byteIntervalCount=" + count + " start=" + firstValue);
      for (int i = 0; i < count; i++) {
        if (i != 0) {
          stream.print(", ");
        }
        stream.print(bitIntervals[i]);
      }
      stream.print(")");
    }

    BitExplicitByteInterval(long bits, long interval, int addCount) {
      count = addCount;
      firstValue = bits;
      lastValue = bits + (interval * (addCount - 1));
      bitIntervals = new byte[count * 2];
      bitIntervals[0] = 0;
      for (int i = 1; i < count; i++) {
        bitIntervals[i] = (byte) interval;
      }
    }

    @Override
    boolean attemptAdd(long addBits, long addInterval, int addCount) {
      // addCount >= 2; count >= 2
      if (addCount <= 11) {
        if (addInterval <= Byte.MAX_VALUE && addInterval >= Byte.MIN_VALUE) {
          long firstInterval = addBits - lastValue;
          if (firstInterval <= Byte.MAX_VALUE && firstInterval >= Byte.MIN_VALUE) {
            lastValue = addBits + (addInterval * (addCount - 1));
            if ((count + addCount) >= bitIntervals.length) {
              byte[] tmp = new byte[(count + addCount) * 2];
              System.arraycopy(bitIntervals, 0, tmp, 0, bitIntervals.length);
              bitIntervals = tmp;
            }
            bitIntervals[count++] = (byte) firstInterval;
            for (int i = 1; i < addCount; i++) {
              bitIntervals[count++] = (byte) addInterval;
            }
            return true;
          }
        }
      }
      return false;
    }
  }

  private static class BitExplicitShortInterval extends BitInterval {
    long firstValue;
    long lastValue;
    short[] bitIntervals = null;

    @Override
    int getMemoryUsed() {
      int result = super.getMemoryUsed() + 4 + 8 + 8 + 4;
      if (bitIntervals != null) {
        result += bitIntervals.length * 2;
      }
      return result;
    }

    @Override
    int fill(double[] values, int valueOffset, int typeCode, int skipCount) {
      int fillcount = values.length - valueOffset; // space left in values
      int maxCount = count - skipCount; // maximum values this interval can produce
      if (fillcount > maxCount) {
        fillcount = maxCount;
      }
      long bitValue = firstValue;
      for (int i = 0; i < skipCount; i++) {
        bitValue += bitIntervals[i];
      }
      for (int i = 0; i < fillcount; i++) {
        bitValue += bitIntervals[skipCount + i];
        values[valueOffset + i] = bitsToDouble(typeCode, bitValue);
      }
      return fillcount;
    }

    @Override
    void dump(PrintWriter stream) {
      stream.print("(shortIntervalCount=" + count + " start=" + firstValue);
      for (int i = 0; i < count; i++) {
        if (i != 0) {
          stream.print(", ");
        }
        stream.print(bitIntervals[i]);
      }
      stream.print(")");
    }

    BitExplicitShortInterval(long bits, long interval, int addCount) {
      count = addCount;
      firstValue = bits;
      lastValue = bits + (interval * (addCount - 1));
      bitIntervals = new short[count * 2];
      bitIntervals[0] = 0;
      for (int i = 1; i < count; i++) {
        bitIntervals[i] = (short) interval;
      }
    }

    @Override
    boolean attemptAdd(long addBits, long addInterval, int addCount) {
      // addCount >= 2; count >= 2
      if (addCount <= 6) {
        if (addInterval <= Short.MAX_VALUE && addInterval >= Short.MIN_VALUE) {
          long firstInterval = addBits - lastValue;
          if (firstInterval <= Short.MAX_VALUE && firstInterval >= Short.MIN_VALUE) {
            lastValue = addBits + (addInterval * (addCount - 1));
            if ((count + addCount) >= bitIntervals.length) {
              short[] tmp = new short[(count + addCount) * 2];
              System.arraycopy(bitIntervals, 0, tmp, 0, bitIntervals.length);
              bitIntervals = tmp;
            }
            bitIntervals[count++] = (short) firstInterval;
            for (int i = 1; i < addCount; i++) {
              bitIntervals[count++] = (short) addInterval;
            }
            return true;
          }
        }
      }
      return false;
    }
  }

  private static class BitExplicitIntInterval extends BitInterval {
    long firstValue;
    long lastValue;
    int[] bitIntervals = null;

    @Override
    int getMemoryUsed() {
      int result = super.getMemoryUsed() + 4 + 8 + 8 + 4;
      if (bitIntervals != null) {
        result += bitIntervals.length * 4;
      }
      return result;
    }

    @Override
    int fill(double[] values, int valueOffset, int typeCode, int skipCount) {
      int fillcount = values.length - valueOffset; // space left in values
      int maxCount = count - skipCount; // maximum values this interval can produce
      if (fillcount > maxCount) {
        fillcount = maxCount;
      }
      long bitValue = firstValue;
      for (int i = 0; i < skipCount; i++) {
        bitValue += bitIntervals[i];
      }
      for (int i = 0; i < fillcount; i++) {
        bitValue += bitIntervals[skipCount + i];
        values[valueOffset + i] = bitsToDouble(typeCode, bitValue);
      }
      return fillcount;
    }

    @Override
    void dump(PrintWriter stream) {
      stream.print("(intIntervalCount=" + count + " start=" + firstValue);
      for (int i = 0; i < count; i++) {
        if (i != 0) {
          stream.print(", ");
        }
        stream.print(bitIntervals[i]);
      }
      stream.print(")");
    }

    BitExplicitIntInterval(long bits, long interval, int addCount) {
      count = addCount;
      firstValue = bits;
      lastValue = bits + (interval * (addCount - 1));
      bitIntervals = new int[count * 2];
      bitIntervals[0] = 0;
      for (int i = 1; i < count; i++) {
        bitIntervals[i] = (int) interval;
      }
    }

    @Override
    boolean attemptAdd(long addBits, long addInterval, int addCount) {
      // addCount >= 2; count >= 2
      if (addCount <= 4) {
        if (addInterval <= Integer.MAX_VALUE && addInterval >= Integer.MIN_VALUE) {
          long firstInterval = addBits - lastValue;
          if (firstInterval <= Integer.MAX_VALUE && firstInterval >= Integer.MIN_VALUE) {
            lastValue = addBits + (addInterval * (addCount - 1));
            if ((count + addCount) >= bitIntervals.length) {
              int[] tmp = new int[(count + addCount) * 2];
              System.arraycopy(bitIntervals, 0, tmp, 0, bitIntervals.length);
              bitIntervals = tmp;
            }
            bitIntervals[count++] = (int) firstInterval;
            for (int i = 1; i < addCount; i++) {
              bitIntervals[count++] = (int) addInterval;
            }
            return true;
          }
        }
      }
      return false;
    }
  }

  private static class BitExplicitLongInterval extends BitInterval {
    long[] bitArray = null;

    @Override
    int getMemoryUsed() {
      int result = super.getMemoryUsed() + 4 + 4;
      if (bitArray != null) {
        result += bitArray.length * 8;
      }
      return result;
    }

    @Override
    int fill(double[] values, int valueOffset, int typeCode, int skipCount) {
      int fillcount = values.length - valueOffset; // space left in values
      int maxCount = count - skipCount; // maximum values this interval can produce
      if (fillcount > maxCount) {
        fillcount = maxCount;
      }
      for (int i = 0; i < fillcount; i++) {
        values[valueOffset + i] = bitsToDouble(typeCode,
            bitArray[skipCount + i]);
      }
      return fillcount;
    }

    @Override
    void dump(PrintWriter stream) {
      stream.print("(count=" + count + " ");
      for (int i = 0; i < count; i++) {
        if (i != 0) {
          stream.print(", ");
        }
        stream.print(bitArray[i]);
      }
      stream.print(")");
    }

    BitExplicitLongInterval(long bits, long interval, int addCount) {
      count = addCount;
      bitArray = new long[count * 2];
      for (int i = 0; i < count; i++) {
        bitArray[i] = bits;
        bits += interval;
      }
    }

    @Override
    boolean attemptAdd(long addBits, long addInterval, int addCount) {
      // addCount >= 2; count >= 2
      if (addCount <= 3) {
        if ((count + addCount) >= bitArray.length) {
          long[] tmp = new long[(count + addCount) * 2];
          System.arraycopy(bitArray, 0, tmp, 0, bitArray.length);
          bitArray = tmp;
        }
        for (int i = 0; i < addCount; i++) {
          bitArray[count++] = addBits;
          addBits += addInterval;
        }
        return true;
      }
      return false;
    }
  }

  private static class BitSeries {
    int count; // number of items in this series
    long currentStartBits;
    long currentEndBits;
    long currentInterval;
    int currentCount;
    int intervalIdx; // index of most recent BitInterval
    BitInterval intervals[];

    /**
     * Returns the amount of memory used to implement this series.
     */
    protected int getMemoryUsed() {
      int result = 4 + 8 + 8 + 8 + 4 + 4 + 4;
      if (intervals != null) {
        result += 4 * intervals.length;
        for (int i = 0; i <= intervalIdx; i++) {
          result += intervals[i].getMemoryUsed();
        }
      }
      return result;
    }

    /**
     * Gets the first "resultSize" values of this series skipping over the first
     * "samplesToSkip" ones. The first value in a series is at index 0. The
     * maximum result size can be obtained by calling "getSize()".
     */
    public double[] getValuesEx(int typeCode, int samplesToSkip,
        int resultSize) {
      double[] result = new double[resultSize];
      int firstInterval = 0;
      int idx = 0;
      while (samplesToSkip > 0
          && firstInterval <= intervalIdx
          && intervals[firstInterval].getSampleCount() <= samplesToSkip) {
        samplesToSkip -= intervals[firstInterval].getSampleCount();
        firstInterval++;
      }
      for (int i = firstInterval; i <= intervalIdx; i++) {
        idx += intervals[i].fill(result, idx, typeCode, samplesToSkip);
        samplesToSkip = 0;
      }
      if (currentCount != 0) {
        idx += BitInterval.create(currentStartBits, currentInterval,
            currentCount).fill(result, idx, typeCode, samplesToSkip);
      }
      // assert
      if (idx != resultSize) {
        throw new RuntimeException("GetValuesEx didn't fill the last "
            + (resultSize - idx) + " entries of its result");
      }
      return result;
    }

    void dump(PrintWriter stream) {
      stream.print("[size=" + count + " intervals=" + (intervalIdx + 1)
          + " memused=" + getMemoryUsed() + " ");
      for (int i = 0; i <= intervalIdx; i++) {
        if (i != 0) {
          stream.print(", ");
        }
        intervals[i].dump(stream);
      }
      if (currentCount != 0) {
        if (intervalIdx != -1) {
          stream.print(", ");
        }
        BitInterval.create(currentStartBits, currentInterval,
            currentCount).dump(stream);
      }
      stream.println("]");
    }

    BitSeries() {
      count = 0;
      currentStartBits = 0;
      currentEndBits = 0;
      currentInterval = 0;
      currentCount = 0;
      intervalIdx = -1;
      intervals = null;
    }

    void initialBits(long bits) {
      this.currentEndBits = bits;
    }

    int getSize() {
      return this.count;
    }

    void addBits(long deltaBits) {
      long bits = currentEndBits + deltaBits;
      if (currentCount == 0) {
        currentStartBits = bits;
        currentCount = 1;
      } else if (currentCount == 1) {
        currentInterval = deltaBits;
        currentCount++;
      } else if (deltaBits == currentInterval) {
        currentCount++;
      } else {
        // we need to move currentBits into a BitInterval
        if (intervalIdx == -1) {
          intervals = new BitInterval[2];
          intervalIdx = 0;
          intervals[0] = BitInterval.create(currentStartBits, currentInterval,
              currentCount);
        } else {
          if (!intervals[intervalIdx].attemptAdd(currentStartBits,
              currentInterval, currentCount)) {
            // wouldn't fit in current bit interval so add a new one
            intervalIdx++;
            if (intervalIdx >= intervals.length) {
              BitInterval[] tmp = new BitInterval[intervals.length * 2];
              System.arraycopy(intervals, 0, tmp, 0, intervals.length);
              intervals = tmp;
            }
            intervals[intervalIdx] = BitInterval.create(currentStartBits,
                currentInterval, currentCount);
          }
        }
        // now start a new currentBits
        currentStartBits = bits;
        currentCount = 1;
      }
      currentEndBits = bits;
      count++;
    }

    /**
     * Free up any unused memory
     */
    void shrink() {
      if (intervals != null) {
        int currentSize = intervalIdx + 1;
        if (currentSize < intervals.length) {
          BitInterval[] tmp = new BitInterval[currentSize];
          System.arraycopy(intervals, 0, tmp, 0, currentSize);
          intervals = tmp;
        }
      }
    }
  }

  private static class TimeStampSeries {
    static private final int GROW_SIZE = 256;
    int count; // number of items in this series
    long base; // millis since midnight, Jan 1, 1970 UTC.
    long[] timeStamps = new long[GROW_SIZE]; // elapsed millis from base

    void dump(PrintWriter stream) {
      stream.print("[size=" + count);
      for (int i = 0; i < count; i++) {
        if (i != 0) {
          stream.print(", ");
          stream.print(timeStamps[i] - timeStamps[i - 1]);
        } else {
          stream.print(" " + timeStamps[i]);
        }
      }
      stream.println("]");
    }

    void shrink() {
      if (count < timeStamps.length) {
        long[] tmp = new long[count];
        System.arraycopy(timeStamps, 0, tmp, 0, count);
        timeStamps = tmp;
      }
    }

    TimeStampSeries() {
      count = 0;
      base = 0;
    }

    void setBase(long base) {
      this.base = base;
    }

    int getSize() {
      return this.count;
    }

    void addTimeStamp(int ts) {
      if (count >= timeStamps.length) {
        long[] tmp = new long[timeStamps.length + GROW_SIZE];
        System.arraycopy(timeStamps, 0, tmp, 0, timeStamps.length);
        timeStamps = tmp;
      }
      if (count != 0) {
        timeStamps[count] = timeStamps[count - 1] + ts;
      } else {
        timeStamps[count] = ts;
      }
      count++;
    }

    long getBase() {
      return this.base;
    }

    /**
     * Provides direct access to underlying data. Do not modify contents and use
     * getSize() to keep from reading past end of array.
     */
    long[] getRawTimeStamps() {
      return this.timeStamps;
    }

    long getMilliTimeStamp(int idx) {
      return this.base + this.timeStamps[idx];
    }

    /**
     * Returns an array of time stamp values the first of which has the
     * specified index. Each returned time stamp is the number of millis since
     * midnight, Jan 1, 1970 UTC.
     */
    double[] getTimeValuesSinceIdx(int idx) {
      int resultSize = this.count - idx;
      double[] result = new double[resultSize];
      for (int i = 0; i < resultSize; i++) {
        result[i] = getMilliTimeStamp(idx + i);
      }
      return result;
    }
  }

  /**
   * Defines a statistic resource type. Each resource instance must be of a
   * single type. The type defines what statistics each instance of it will
   * support. The type also has a description of itself.
   */
  public static class ResourceType {
    private boolean loaded;
    //    private final int id;
    private final String name;
    private String desc;
    private final StatDescriptor[] stats;
    private Map<String, StatDescriptor> descriptorMap;

    public void dump(PrintWriter stream) {
      if (loaded) {
        stream.println(name + ": " + desc);
        for (StatDescriptor stat : stats) {
          stat.dump(stream);
        }
      }
    }

    protected ResourceType(String name, int statCount) {
      this.loaded = false;
      this.name = name;
      this.desc = null;
      this.stats = new StatDescriptor[statCount];
      this.descriptorMap = null;
    }

    protected ResourceType(String name, String desc, int statCount) {
      this.loaded = true;
      this.name = name;
      this.desc = desc;
      this.stats = new StatDescriptor[statCount];
      this.descriptorMap = new HashMap<String, StatDescriptor>();
    }

    public boolean isLoaded() {
      return this.loaded;
    }

    /**
     * Frees up any resources no longer needed after the archive file is closed.
     * Returns true if this guy is no longer needed.
     */
    protected boolean close() {
      if (isLoaded()) {
        for (int i = 0; i < stats.length; i++) {
          if (stats[i] != null) {
            if (!stats[i].isLoaded()) {
              stats[i] = null;
            }
          }
        }
        return false;
      } else {
        return true;
      }
    }

    void unload() {
      this.loaded = false;
      this.desc = null;
      for (StatDescriptor stat : this.stats) {
        stat.unload();
      }
      this.descriptorMap.clear();
      this.descriptorMap = null;
    }

    protected void addStatDescriptor(StatArchiveFile archive, int offset,
        String name, boolean isCounter,
        boolean largerBetter,
        byte typeCode, String units, String desc) {
      StatDescriptor descriptor = new StatDescriptor(name, offset, isCounter,
          largerBetter, typeCode, units, desc);
      this.stats[offset] = descriptor;
      if (archive.loadStatDescriptor(descriptor, this)) {
        descriptorMap.put(name, descriptor);
      }
    }

//    private int getId() {
//      return this.id;
//    }

    /**
     * Returns the name of this resource type.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns an array of descriptors for each statistic this resource type
     * supports.
     */
    public StatDescriptor[] getStats() {
      return this.stats;
    }

    /**
     * Gets a stat descriptor contained in this type given the stats name.
     *
     * @param name the name of the stat to find in the current type
     * @return the descriptor that matches the name or null if the type does not
     * have a stat of the given name
     */
    public StatDescriptor getStat(String name) {
      return (StatDescriptor) descriptorMap.get(name);
    }

    /**
     * Returns a description of this resource type.
     */
    public String getDescription() {
      return this.desc;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
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
      ResourceType other = (ResourceType) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      return true;
    }
  }

  /**
   * Describes some global information about the archive.
   */
  public static class ArchiveInfo {
    private final StatArchiveFile archive;
    private final byte archiveVersion;
    private final long startTimeStamp; // in milliseconds
    private final long systemStartTimeStamp; // in milliseconds
    private final int timeZoneOffset;
    private final String timeZoneName;
    private final String systemDirectory;
    private final long systemId;
    private final String productVersion;
    private final String os;
    private final String machine;

    public ArchiveInfo(StatArchiveFile archive, byte archiveVersion,
        long startTimeStamp, long systemStartTimeStamp,
        int timeZoneOffset, String timeZoneName,
        String systemDirectory, long systemId,
        String productVersion, String os, String machine) {
      this.archive = archive;
      this.archiveVersion = archiveVersion;
      this.startTimeStamp = startTimeStamp;
      this.systemStartTimeStamp = systemStartTimeStamp;
      this.timeZoneOffset = timeZoneOffset;
      this.timeZoneName = timeZoneName;
      this.systemDirectory = systemDirectory;
      this.systemId = systemId;
      this.productVersion = productVersion;
      this.os = os;
      this.machine = machine;
      archive.setTimeZone(getTimeZone());
    }

    /**
     * Returns the difference, measured in milliseconds, between the time the
     * archive file was create and midnight, January 1, 1970 UTC.
     */
    public long getStartTimeMillis() {
      return this.startTimeStamp;
    }

    /**
     * Returns the difference, measured in milliseconds, between the time the
     * archived system was started and midnight, January 1, 1970 UTC.
     */
    public long getSystemStartTimeMillis() {
      return this.systemStartTimeStamp;
    }

    /**
     * Returns a numeric id of the archived system.  It can be used in
     * conjunction with the {@link #getSystemStartTimeMillis} to uniquely
     * identify an archived system.
     */
    public long getSystemId() {
      return this.systemId;
    }

    /**
     * Returns a string describing the operating system the archive was written
     * on.
     */
    public String getOs() {
      return this.os;
    }

    /**
     * Returns a string describing the machine the archive was written on.
     */
    public String getMachine() {
      return this.machine;
    }

    /**
     * Returns  the time zone used when the archive was created. This can be
     * used to print timestamps in the same time zone that was in effect when
     * the archive was created.
     */
    public TimeZone getTimeZone() {
      TimeZone result = TimeZone.getTimeZone(this.timeZoneName);
      if (result.getRawOffset() != this.timeZoneOffset) {
        result = new SimpleTimeZone(this.timeZoneOffset, this.timeZoneName);
      }
      return result;
    }

    /**
     * Returns a string containing the version of the product that wrote this
     * archive.
     */
    public String getProductVersion() {
      return this.productVersion;
    }

    /**
     * Returns a numeric code that represents the format version used to encode
     * the archive as a stream of bytes.
     */
    public int getArchiveFormatVersion() {
      return this.archiveVersion;
    }

    /**
     * Returns a string describing the system that this archive recorded.
     */
    public String getSystem() {
      return this.systemDirectory;
    }

    /**
     * Returns a string representation of this object.
     */
    @Override
    public String toString() {
      StringWriter sw = new StringWriter();
      this.dump(new PrintWriter(sw));
      return sw.toString();
    }

    protected void dump(PrintWriter stream) {
      stream.println("archiveVersion=" + archiveVersion);
      if (archive != null) {
        stream.println("startDate=" + archive.formatTimeMillis(startTimeStamp));
      }
      // stream.println("startTimeStamp=" + startTimeStamp +" tz=" + timeZoneName + " tzOffset=" + timeZoneOffset);
      // stream.println("timeZone=" + getTimeZone().getDisplayName());
      stream.println("systemDirectory=" + systemDirectory);
      stream.println(
          "systemStartDate=" + archive.formatTimeMillis(systemStartTimeStamp));
      stream.println("systemId=" + systemId);
      stream.println("productVersion=" + productVersion);
      stream.println("osInfo=" + os);
      stream.println("machineInfo=" + machine);
    }
  }

  /**
   * Defines a single instance of a resource type.
   */
  public static class ResourceInst {
    private final boolean loaded;
    private final StatArchiveFile archive;
    //    private final int uniqueId;
    private final ResourceType type;
    private final String name;
    private final long id;
    private boolean active = true;
    private final SimpleValue[] values;
    private int firstTSidx = -1;
    private int lastTSidx = -1;

    /**
     * Returns the approximate amount of memory used to implement this object.
     */
    protected int getMemoryUsed() {
      int result = 0;
      if (values != null) {
        for (SimpleValue value : values) {
          result += value.getMemoryUsed();
        }
      }
      return result;
    }

    /**
     * Returns a string representation of this object.
     */
    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      result.append(name)
          .append(", ")
          .append(id)
          .append(", ")
          .append(type.getName())
          .append(": \"")
          .append(archive.formatTimeMillis(getFirstTimeMillis()))
          .append('\"');
      if (!active) {
        result.append(" inactive");
      }
      result.append(" samples=").append(getSampleCount());
      return result.toString();
    }

    /**
     * Returns the number of times this resource instance has been sampled.
     */
    public int getSampleCount() {
      if (active) {
        return archive.getTimeStamps().getSize() - firstTSidx;
      } else {
        return (lastTSidx + 1) - firstTSidx;
      }
    }

    public StatArchiveFile getArchive() {
      return this.archive;
    }

    protected void dump(PrintWriter stream) {
      stream.println(name + ":"
          + " file=" + getArchive().getArchiveFileName()
          + " id=" + id
          + (active ? "" : " deleted")
          + " start=" + archive.formatTimeMillis(getFirstTimeMillis()));
      for (SimpleValue value : values) {
        value.dump(stream);
      }
    }

    protected ResourceInst(StatArchiveFile archive, String name,
        long id, ResourceType type, boolean loaded) {
      this.loaded = loaded;
      this.archive = archive;
      this.name = name;
      this.id = id;
      assert (type != null);
      this.type = type;
      if (loaded) {
        StatDescriptor[] stats = type.getStats();
        this.values = new SimpleValue[stats.length];
        for (int i = 0; i < stats.length; i++) {
          if (archive.loadStat(stats[i], this)) {
            this.values[i] = new SimpleValue(this, stats[i]);
          } else {
            this.values[i] = null;
          }
        }
      } else {
        this.values = null;
      }
    }

    void matchSpec(StatSpec spec, List<AbstractValue> matchedValues) {
      if (spec.typeMatches(this.type.getName())) {
        if (spec.instanceMatches(this.getName(), this.getId())) {
          for (SimpleValue value : values) {
            if (value != null) {
              if (spec.statMatches(value.getDescriptor().getName())) {
                matchedValues.add(value);
              }
            }
          }
        }
      }
    }

    protected void initialValue(int statOffset, long v) {
      if (this.values != null && this.values[statOffset] != null) {
        this.values[statOffset].initialValue(v);
      }
    }

    /**
     * Returns true if sample was added.
     */
    protected boolean addValueSample(int statOffset, long statDeltaBits) {
      if (this.values != null && this.values[statOffset] != null) {
        this.values[statOffset].prepareNextBits(statDeltaBits);
        return true;
      } else {
        return false;
      }
    }

    public boolean isLoaded() {
      return this.loaded;
    }

    /**
     * Frees up any resources no longer needed after the archive file is closed.
     * Returns true if this guy is no longer needed.
     */
    protected boolean close() {
      if (isLoaded()) {
        for (SimpleValue value : values) {
          if (value != null) {
            value.shrink();
          }
        }
        return false;
      } else {
        return true;
      }
    }

    protected int getFirstTimeStampIdx() {
      return this.firstTSidx;
    }

    protected long[] getAllRawTimeStamps() {
      return archive.getTimeStamps().getRawTimeStamps();
    }

    protected long getTimeBase() {
      return archive.getTimeStamps().getBase();
    }

    /**
     * Returns an array of doubles containing the timestamps at which this
     * instances samples where taken. Each of these timestamps is the
     * difference, measured in milliseconds, between the sample time and
     * midnight, January 1, 1970 UTC. Although these values are double they can
     * safely be converted to <code>long</code> with no loss of information.
     */
    public double[] getSnapshotTimesMillis() {
      return archive.getTimeStamps().getTimeValuesSinceIdx(firstTSidx);
    }

    /**
     * Returns an array of statistic value descriptors. Each element of the
     * array describes the corresponding statistic this instance supports. The
     * <code>StatValue</code> instances can be used to obtain the actual sampled
     * values of the instances statistics.
     */
    public StatValue[] getStatValues() {
      return this.values;
    }

    /**
     * Gets the value of the stat in the current instance given the stat name.
     *
     * @param name the name of the stat to find in the current instance
     * @return the value that matches the name or null if the instance does not
     * have a stat of the given name
     */
    public StatValue getStatValue(String name) {
      StatValue result = null;
      StatDescriptor desc = getType().getStat(name);
      if (desc != null) {
        result = values[desc.getOffset()];
      }
      return result;
    }

    /**
     * Returns the name of this instance.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the id of this instance.
     */
    public long getId() {
      return this.id;
    }

    /**
     * Returns the difference, measured in milliseconds, between the time of the
     * instance's first sample and midnight, January 1, 1970 UTC.
     */
    public long getFirstTimeMillis() {
      return archive.getTimeStamps().getMilliTimeStamp(firstTSidx);
    }

    /**
     * Returns resource type of this instance.
     */
    public ResourceType getType() {
      return this.type;
    }

    protected void makeInactive() {
      this.active = false;
      lastTSidx = archive.getTimeStamps().getSize() - 1;
      close(); // this frees up unused memory now that no more samples
    }

    /**
     * Returns true if archive might still have future samples for this
     * instance.
     */
    public boolean isActive() {
      return this.active;
    }

    protected void addTimeStamp() {
      if (this.loaded) {
        if (firstTSidx == -1) {
          firstTSidx = archive.getTimeStamps().getSize() - 1;
        }
        for (SimpleValue value : values) {
          if (value != null) {
            value.addSample();
          }
        }
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (id ^ (id >>> 32));
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
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
      ResourceInst other = (ResourceInst) obj;
      if (id != other.id)
        return false;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (type == null) {
        if (other.type != null)
          return false;
      } else if (!type.equals(other.type))
        return false;
      return true;
    }
  }

  public static interface StatSpec extends ValueFilter {
    /**
     * Causes all stats that matches this spec, in all archive files, to be
     * combined into a single global stat value.
     */
    public static final int GLOBAL = 2;
    /**
     * Causes all stats that matches this spec, in each archive file, to be
     * combined into a single stat value for each file.
     */
    public static final int FILE = 1;
    /**
     * No combination is done.
     */
    public final int NONE = 0;

    /**
     * Returns one of the following values: {@link #GLOBAL}, {@link #FILE},
     * {@link #NONE}.
     */
    public int getCombineType();
  }

  /**
   * Specifies what data from a statistic archive will be of interest to the
   * reader. This is used when loading a statistic archive file to reduce the
   * memory footprint. Only statistic data that matches all four will be
   * selected for loading.
   */
  public static interface ValueFilter {
    /**
     * Returns true if the specified archive file matches this spec. Any
     * archives whose name does not match this spec will not be selected for
     * loading by this spec.
     */
    public boolean archiveMatches(File archive);

    /**
     * Returns true if the specified type name matches this spec. Any types
     * whose name does not match this spec will not be selected for loading by
     * this spec.
     */
    public boolean typeMatches(String typeName);

    /**
     * Returns true if the specified statistic name matches this spec. Any stats
     * whose name does not match this spec will not be selected for loading by
     * this spec.
     */
    public boolean statMatches(String statName);

    /**
     * Returns true if the specified instance matches this spec. Any instance
     * whose text id and numeric id do not match this spec will not be selected
     * for loading by this spec.
     */
    public boolean instanceMatches(String textId, long numericId);
  }
}
