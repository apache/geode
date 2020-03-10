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
package org.apache.geode.internal.statistics;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * StatArchiveWriter provides APIs to write statistic snapshots to an archive file.
 *
 */
public class StatArchiveWriter implements StatArchiveFormat, SampleHandler {

  private static final Logger logger = LogService.getLogger();

  @MakeNotStatic
  private static volatile String traceStatisticsName = null;
  @MakeNotStatic
  private static volatile String traceStatisticsTypeName = null;
  @MakeNotStatic
  private static volatile int traceResourceInstId = -1;

  private final boolean trace =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "stats.debug.traceStatArchiveWriter");

  private final Set<ResourceInstance> sampleWrittenForResources = new HashSet<ResourceInstance>();
  private final Set<ResourceInstance> addedResources = new HashSet<ResourceInstance>();
  private final StatArchiveDescriptor archiveDescriptor;
  private long initialDate;
  private final OutputStream outStream;
  private final MyDataOutputStream dataOut;
  private final OutputStream traceOutStream;
  private final PrintStream traceDataOut;
  private long previousMillisTimeStamp;
  private int sampleCount;

  /**
   * Opens a StatArchiveWriter that will archive to the specified file.
   *
   * @throws GemFireIOException if <code>archiveName</code> can not be written to
   */
  public StatArchiveWriter(StatArchiveDescriptor archiveDescriptor) {
    this.archiveDescriptor = archiveDescriptor;

    if (archiveDescriptor.getArchiveName().endsWith(".gz")) {
      try {
        this.outStream =
            new GZIPOutputStream(new FileOutputStream(archiveDescriptor.getArchiveName()), 32768);
      } catch (IOException ex) {
        throw new GemFireIOException(String.format("Could not open %s",
            archiveDescriptor.getArchiveName()), ex);
      }
    } else {
      try {
        this.outStream = new BufferedOutputStream(
            new FileOutputStream(archiveDescriptor.getArchiveName()), 32768);
      } catch (IOException ex) {
        throw new GemFireIOException(String.format("Could not open %s",
            archiveDescriptor.getArchiveName()), ex);
      }
    }

    this.dataOut = new MyDataOutputStream(this.outStream);

    if (this.trace) {
      String traceFileName = archiveDescriptor.getArchiveName() + ".trace";
      try {
        this.traceOutStream = new BufferedOutputStream(new FileOutputStream(traceFileName), 32768);
      } catch (IOException ex) {
        throw new GemFireIOException("Could not open " + traceFileName, ex);
      }
      this.traceDataOut = new PrintStream(this.traceOutStream);
    } else {
      this.traceOutStream = null;
      this.traceDataOut = null;
    }
  }

  public String getArchiveName() {
    return this.archiveDescriptor.getArchiveName();
  }

  public void initialize(long nanosTimeStamp) {
    this.previousMillisTimeStamp = initPreviousMillisTimeStamp(nanosTimeStamp);
    this.initialDate = initInitialDate();
    writeHeader(this.initialDate, this.archiveDescriptor);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("archiveName=").append(this.archiveDescriptor.getArchiveName());
    sb.append("productDescription=").append(this.archiveDescriptor.getProductDescription());
    sb.append("systemDirectoryPath=").append(this.archiveDescriptor.getSystemDirectoryPath());
    sb.append("systemId=").append(this.archiveDescriptor.getSystemId());
    sb.append("systemStartTime=").append(this.archiveDescriptor.getSystemStartTime());
    sb.append("previousMillisTimeStamp=").append(this.previousMillisTimeStamp);
    sb.append("initialDate=").append(this.initialDate);
    return sb.toString();
  }

  /**
   * Closes the statArchiver by flushing its data to disk a closing its output stream.
   *
   * @throws GemFireIOException if the archive file could not be closed.
   */
  public void close() {
    try {
      this.dataOut.flush();
      if (this.trace) {
        this.traceDataOut.flush();
      }
    } catch (IOException ignore) {
    }
    try {
      outStream.close();
      if (this.trace) {
        this.traceOutStream.close();
      }
    } catch (IOException ex) {
      throw new GemFireIOException(
          "Could not close statArchiver file",
          ex);
    }
    if (getSampleCount() == 0) {
      // If we are closing an empty file go ahead and delete it.
      // This prevents the fix for 46917 from leaving a bunch of
      // empty gfs files around.
      deleteFileIfPossible(new File(getArchiveName()));
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification = "Best effort attempt to delete a GFS file without any samples.")
  private static void deleteFileIfPossible(File file) {
    file.delete();
  }

  /**
   * Returns the number of bytes written so far to this archive. This does not take compression into
   * account.
   */
  public long bytesWritten() {
    return this.dataOut.getBytesWritten();
  }

  protected long initPreviousMillisTimeStamp(long nanosTimeStamp) {
    return NanoTimer.nanosToMillis(nanosTimeStamp);
  }

  protected long initInitialDate() {
    return System.currentTimeMillis();
  }

  protected TimeZone getTimeZone() {
    return Calendar.getInstance().getTimeZone();
  }

  protected String getOSInfo() {
    return System.getProperty("os.name") + " " + System.getProperty("os.version");
  }

  protected String getMachineInfo() {
    String machineInfo = System.getProperty("os.arch");
    try {
      String hostName = LocalHostUtil.getLocalHostName();
      machineInfo += " " + hostName;
    } catch (UnknownHostException ignore) {
    }
    return machineInfo;
  }

  private void writeHeader(long initialDate, StatArchiveDescriptor archiveDescriptor) {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#writeHeader initialDate={} archiveDescriptor={}", initialDate,
          archiveDescriptor);
    }
    try {
      this.dataOut.writeByte(HEADER_TOKEN);
      this.dataOut.writeByte(ARCHIVE_VERSION);
      this.dataOut.writeLong(initialDate);
      this.dataOut.writeLong(archiveDescriptor.getSystemId());
      this.dataOut.writeLong(archiveDescriptor.getSystemStartTime());
      TimeZone timeZone = getTimeZone();
      this.dataOut.writeInt(timeZone.getRawOffset());
      this.dataOut.writeUTF(timeZone.getID());
      this.dataOut.writeUTF(archiveDescriptor.getSystemDirectoryPath());
      this.dataOut.writeUTF(archiveDescriptor.getProductDescription());
      this.dataOut.writeUTF(getOSInfo());
      this.dataOut.writeUTF(getMachineInfo());

      if (this.trace) {
        this.traceDataOut.println("writeHeader traceStatisticsName: " + traceStatisticsName);
        this.traceDataOut
            .println("writeHeader traceStatisticsTypeName: " + traceStatisticsTypeName);
        this.traceDataOut.println("writeHeader#writeByte HEADER_TOKEN: " + HEADER_TOKEN);
        this.traceDataOut.println("writeHeader#writeByte ARCHIVE_VERSION: " + ARCHIVE_VERSION);
        this.traceDataOut.println("writeHeader#writeLong initialDate: " + initialDate);
        this.traceDataOut.println("writeHeader#writeLong archiveDescriptor.getSystemId(): "
            + archiveDescriptor.getSystemId());
        this.traceDataOut.println("writeHeader#writeLong archiveDescriptor.getSystemStartTime(): "
            + archiveDescriptor.getSystemStartTime());
        this.traceDataOut
            .println("writeHeader#writeInt timeZone.getRawOffset(): " + timeZone.getRawOffset());
        this.traceDataOut.println("writeHeader#writeUTF timeZone.getID(): " + timeZone.getID());
        this.traceDataOut
            .println("writeHeader#writeUTF archiveDescriptor.getSystemDirectoryPath(): "
                + archiveDescriptor.getSystemDirectoryPath());
        this.traceDataOut.println("writeHeader#writeUTF archiveDescriptor.getProductDescription(): "
            + archiveDescriptor.getProductDescription());
        this.traceDataOut.println("writeHeader#writeUTF getOSInfo(): " + getOSInfo());
        this.traceDataOut.println("writeHeader#writeUTF getMachineInfo(): " + getMachineInfo());
      }
    } catch (IOException ex) {
      throw new GemFireIOException(
          "Failed writing header to statistic archive",
          ex);
    }
  }

  @Override
  public void allocatedResourceType(ResourceType resourceType) {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#allocatedResourceType resourceType={}", resourceType);
    }
    if (resourceType.getStatisticDescriptors().length >= ILLEGAL_STAT_OFFSET) {
      throw new InternalGemFireException(
          String.format("Could not archive type %s because it had more than %s statistics.",
              new Object[] {resourceType.getStatisticsType().getName(),
                  Integer.valueOf(ILLEGAL_STAT_OFFSET - 1)}));
    }
    // write the type to the archive
    try {
      this.dataOut.writeByte(RESOURCE_TYPE_TOKEN);
      this.dataOut.writeInt(resourceType.getId());
      this.dataOut.writeUTF(resourceType.getStatisticsType().getName());
      this.dataOut.writeUTF(resourceType.getStatisticsType().getDescription());
      StatisticDescriptor[] stats = resourceType.getStatisticDescriptors();
      this.dataOut.writeShort(stats.length);
      if (this.trace && (traceStatisticsTypeName == null
          || traceStatisticsTypeName.equals(resourceType.getStatisticsType().getName()))) {
        this.traceDataOut
            .println("allocatedResourceType#writeByte RESOURCE_TYPE_TOKEN: " + RESOURCE_TYPE_TOKEN);
        this.traceDataOut.println(
            "allocatedResourceType#writeInt resourceType.getId(): " + resourceType.getId());
        this.traceDataOut
            .println("allocatedResourceType#writeUTF resourceType.getStatisticsType().getName(): "
                + resourceType.getStatisticsType().getName());
        this.traceDataOut.println(
            "allocatedResourceType#writeUTF resourceType.getStatisticsType().getDescription(): "
                + resourceType.getStatisticsType().getDescription());
        this.traceDataOut.println("allocatedResourceType#writeShort stats.length: " + stats.length);
      }
      for (int i = 0; i < stats.length; i++) {
        this.dataOut.writeUTF(stats[i].getName());
        this.dataOut.writeByte(((StatisticDescriptorImpl) stats[i]).getTypeCode());
        this.dataOut.writeBoolean(stats[i].isCounter());
        this.dataOut.writeBoolean(stats[i].isLargerBetter());
        this.dataOut.writeUTF(stats[i].getUnit());
        this.dataOut.writeUTF(stats[i].getDescription());
        if (this.trace && (traceStatisticsTypeName == null
            || traceStatisticsTypeName.equals(resourceType.getStatisticsType().getName()))) {
          this.traceDataOut
              .println("allocatedResourceType#writeUTF stats[i].getName(): " + stats[i].getName());
          this.traceDataOut.println(
              "allocatedResourceType#writeByte ((StatisticDescriptorImpl)stats[i]).getTypeCode(): "
                  + ((StatisticDescriptorImpl) stats[i]).getTypeCode());
          this.traceDataOut.println(
              "allocatedResourceType#writeBoolean stats[i].isCounter(): " + stats[i].isCounter());
          this.traceDataOut.println("allocatedResourceType#writeBoolean stats[i].isLargerBetter(): "
              + stats[i].isLargerBetter());
          this.traceDataOut
              .println("allocatedResourceType#writeUTF stats[i].getUnit(): " + stats[i].getUnit());
          this.traceDataOut.println("allocatedResourceType#writeUTF stats[i].getDescription(): "
              + stats[i].getDescription());
        }
      }
    } catch (IOException ex) {
      throw new GemFireIOException(
          "Failed writing new resource type to statistic archive",
          ex);
    }
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "This is only for debugging and there is never more than one instance being traced because there is only one stat sampler.")
  public void allocatedResourceInstance(ResourceInstance statResource) {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#allocatedResourceInstance statResource={}", statResource);
    }
    if (statResource.getResourceType().getStatisticDescriptors().length >= ILLEGAL_STAT_OFFSET) {
      throw new InternalGemFireException(
          String.format("Could not archive type %s because it had more than %s statistics.",

              new Object[] {statResource.getResourceType().getStatisticsType().getName(),
                  Integer.valueOf(ILLEGAL_STAT_OFFSET - 1)}));
    }
    if (statResource.getStatistics().isClosed()) {
      return;
    }
    this.addedResources.add(statResource);
    try {
      this.dataOut.writeByte(RESOURCE_INSTANCE_CREATE_TOKEN);
      this.dataOut.writeInt(statResource.getId());
      this.dataOut.writeUTF(statResource.getStatistics().getTextId());
      this.dataOut.writeLong(statResource.getStatistics().getNumericId());
      this.dataOut.writeInt(statResource.getResourceType().getId());
      if (this.trace
          && (traceStatisticsName == null
              || traceStatisticsName.equals(statResource.getStatistics().getTextId()))
          && (traceStatisticsTypeName == null || traceStatisticsTypeName
              .equals(statResource.getResourceType().getStatisticsType().getName()))) {
        traceResourceInstId = statResource.getId();
        this.traceDataOut.println("writeHeader traceResourceInstId: " + traceResourceInstId);
        this.traceDataOut
            .println("allocatedResourceInstance#writeByte RESOURCE_INSTANCE_CREATE_TOKEN: "
                + RESOURCE_INSTANCE_CREATE_TOKEN);
        this.traceDataOut.println(
            "allocatedResourceInstance#writeInt statResource.getId(): " + statResource.getId());
        this.traceDataOut
            .println("allocatedResourceInstance#writeUTF statResource.getStatistics().getTextId(): "
                + statResource.getStatistics().getTextId());
        this.traceDataOut.println(
            "allocatedResourceInstance#writeLong statResource.getStatistics().getNumericId(): "
                + statResource.getStatistics().getNumericId());
        this.traceDataOut
            .println("allocatedResourceInstance#writeInt statResource.getResourceType().getId(): "
                + statResource.getResourceType().getId());
      }
    } catch (IOException ex) {
      throw new GemFireIOException(
          "Failed writing new resource instance to statistic archive",
          ex);
    }
  }

  @Override
  public void destroyedResourceInstance(ResourceInstance resourceInstance) {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#destroyedResourceInstance resourceInstance={}", resourceInstance);
    }
    if (!this.addedResources.contains(resourceInstance)) { // Fix for bug #45377
      return;
    }

    this.sampleWrittenForResources.remove(resourceInstance);
    this.addedResources.remove(resourceInstance);

    try {
      this.dataOut.writeByte(RESOURCE_INSTANCE_DELETE_TOKEN);
      this.dataOut.writeInt(resourceInstance.getId());
      if (this.trace
          && (traceStatisticsName == null
              || traceStatisticsName.equals(resourceInstance.getStatistics().getTextId()))
          && (traceStatisticsTypeName == null || traceStatisticsTypeName
              .equals(resourceInstance.getResourceType().getStatisticsType().getName()))) {
        this.traceDataOut
            .println("destroyedResourceInstance#writeByte RESOURCE_INSTANCE_DELETE_TOKEN: "
                + RESOURCE_INSTANCE_DELETE_TOKEN);
        this.traceDataOut.println("destroyedResourceInstance#writeInt resourceInstance.getId(): "
            + resourceInstance.getId());
      }
    } catch (IOException ex) {
      throw new GemFireIOException(
          "Failed writing delete resource instance to statistic archive",
          ex);
    }
  }

  static long calcDelta(long previousMillis, long currentMillis) {
    long delta = currentMillis - previousMillis;
    if (delta <= 0) {
      throw new IllegalArgumentException(
          "Sample timestamp must be greater than previous timestamp (millisTimeStamp is "
              + currentMillis + ", previousMillis is " + previousMillis + " and delta is " + delta
              + ").");
    }
    return delta;
  }

  private void writeTimeStamp(long nanosTimeStamp) throws IOException {
    final long millisTimeStamp = NanoTimer.nanosToMillis(nanosTimeStamp);
    final long delta = calcDelta(this.previousMillisTimeStamp, millisTimeStamp);
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#writeTimeStamp millisTimeStamp={}, delta={}", millisTimeStamp,
          (int) delta);
    }
    if (delta > MAX_SHORT_TIMESTAMP) {
      if (delta > Integer.MAX_VALUE) {
        throw new InternalGemFireException(
            String.format("timeStamp delta %s was greater than %s",

                new Object[] {Long.valueOf(delta), Integer.valueOf(Integer.MAX_VALUE)}));
      }
      this.dataOut.writeShort(INT_TIMESTAMP_TOKEN);
      this.dataOut.writeInt((int) delta);
      if (this.trace) {
        this.traceDataOut
            .println("writeTimeStamp#writeShort INT_TIMESTAMP_TOKEN: " + INT_TIMESTAMP_TOKEN);
        this.traceDataOut.println("writeTimeStamp#writeInt (int)delta: " + (int) delta);
      }
    } else {
      this.dataOut.writeShort((int) delta);
      if (this.trace) {
        this.traceDataOut.println("writeTimeStamp#writeShort (int)delta: " + (int) delta);
      }
    }
    this.previousMillisTimeStamp = millisTimeStamp;
  }

  /**
   * Writes the resource instance id to the <code>dataOut</code> stream.
   */
  private void writeResourceInst(int instId) throws IOException {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "StatArchiveWriter#writeResourceInst instId={}",
          instId);
    }
    if (instId > MAX_BYTE_RESOURCE_INST_ID) {
      if (instId > MAX_SHORT_RESOURCE_INST_ID) {
        this.dataOut.writeByte(INT_RESOURCE_INST_ID_TOKEN);
        this.dataOut.writeInt(instId);
        if (this.trace && (traceResourceInstId == -1 || traceResourceInstId == instId)) {
          this.traceDataOut.println("writeResourceInst#writeByte INT_RESOURCE_INST_ID_TOKEN: "
              + INT_RESOURCE_INST_ID_TOKEN);
          this.traceDataOut.println("writeResourceInst#writeInt instId: " + instId);
        }
      } else {
        this.dataOut.writeByte(SHORT_RESOURCE_INST_ID_TOKEN);
        this.dataOut.writeShort(instId);
        if (this.trace && (traceResourceInstId == -1 || traceResourceInstId == instId)) {
          this.traceDataOut.println("writeResourceInst#writeByte SHORT_RESOURCE_INST_ID_TOKEN: "
              + SHORT_RESOURCE_INST_ID_TOKEN);
          this.traceDataOut.println("writeResourceInst#writeShort instId: " + instId);
        }
      }
    } else {
      this.dataOut.writeByte(instId);
      if (this.trace && (traceResourceInstId == -1 || traceResourceInstId == instId)) {
        if (instId == ILLEGAL_RESOURCE_INST_ID) {
          this.traceDataOut.println(
              "writeResourceInst#writeByte ILLEGAL_RESOURCE_INST_ID: " + ILLEGAL_RESOURCE_INST_ID);
        } else {
          this.traceDataOut.println("writeResourceInst#writeByte instId: " + instId);
        }
      }
    }
  }

  @Override
  public void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances) {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#sampled nanosTimeStamp={}, resourceInstances={}", nanosTimeStamp,
          resourceInstances);
    }
    try {
      this.dataOut.writeByte(SAMPLE_TOKEN);
      if (this.trace) {
        this.traceDataOut.println("sampled#writeByte SAMPLE_TOKEN: " + SAMPLE_TOKEN);
      }
      writeTimeStamp(nanosTimeStamp);
      for (ResourceInstance ri : resourceInstances) {
        writeSample(ri);
      }
      writeResourceInst(ILLEGAL_RESOURCE_INST_ID);
      this.dataOut.flush();
      if (this.trace) {
        this.traceDataOut.flush();
      }
    } catch (IOException ex) {
      throw new GemFireIOException(
          "Failed writing sample to statistic archive",
          ex);
    }
    this.sampleCount++; // only inc after sample done w/o an exception thrown
  }

  public int getSampleCount() {
    return this.sampleCount;
  }

  private void writeSample(ResourceInstance ri) throws IOException {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "StatArchiveWriter#writeSample ri={}", ri);
    }
    if (this.trace
        && (traceStatisticsName == null
            || traceStatisticsName.equals(ri.getStatistics().getTextId()))
        && (traceStatisticsTypeName == null || traceStatisticsTypeName
            .equals(ri.getResourceType().getStatisticsType().getName()))) {
      this.traceDataOut.println("writeSample#writeSample for ri=" + ri);
    }
    if (ri.getStatistics().isClosed()) {
      return;
    }
    StatisticDescriptor[] stats = ri.getResourceType().getStatisticDescriptors();
    if (stats.length > 254) {
      throw new Error("StatisticsType " + ri.getResourceType().getStatisticsType().getName()
          + " has too many stats: " + stats.length);
    }
    boolean wroteInstId = false;
    boolean checkForChange = true;

    if (!this.sampleWrittenForResources.contains(ri)) {
      // first time for this instance so all values need to be written
      checkForChange = false;
      this.sampleWrittenForResources.add(ri);
    }

    long[] previousStatValues = ri.getPreviousStatValues();
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "StatArchiveWriter#writeSample checkForChange={}, previousStatValues={}, stats.length={}",
          checkForChange, Arrays.toString(previousStatValues), stats.length);
    }
    if (previousStatValues == null) {
      previousStatValues = new long[stats.length];
      ri.setPreviousStatValues(previousStatValues);
    }

    int statsWritten = 0;
    try {
      for (int i = 0; i < stats.length; i++) {
        long value = ri.getLatestStatValues()[i];
        if (!checkForChange || value != previousStatValues[i]) {
          long delta = checkForChange ? value - previousStatValues[i] : value;
          if (!wroteInstId) {
            wroteInstId = true;
            writeResourceInst(ri.getId());
          }
          this.dataOut.writeByte(i);
          if (this.trace
              && (traceStatisticsName == null
                  || traceStatisticsName.equals(ri.getStatistics().getTextId()))
              && (traceStatisticsTypeName == null || traceStatisticsTypeName
                  .equals(ri.getResourceType().getStatisticsType().getName()))) {
            this.traceDataOut.println("writeSample#writeByte i: " + i);
          }
          if (isDebugEnabled_STATISTICS) {
            logger.trace(LogMarker.STATISTICS_VERBOSE,
                "StatArchiveWriter#writeStatValue stats[{}]={}, delta={}", i, stats[i], delta);
          }
          writeStatValue(stats[i], delta, this.dataOut);
          if (this.trace
              && (traceStatisticsName == null
                  || traceStatisticsName.equals(ri.getStatistics().getTextId()))
              && (traceStatisticsTypeName == null || traceStatisticsTypeName
                  .equals(ri.getResourceType().getStatisticsType().getName()))) {
            byte typeCode = ((StatisticDescriptorImpl) stats[i]).getTypeCode();
            switch (typeCode) {
              case BYTE_CODE:
                this.traceDataOut.println(
                    "writeStatValue#writeByte " + typeCodeToString(typeCode) + " delta: " + delta);
                break;
              case SHORT_CODE:
                this.traceDataOut.println(
                    "writeStatValue#writeShort" + typeCodeToString(typeCode) + " delta: " + delta);
                break;
              case INT_CODE:
              case FLOAT_CODE:
              case LONG_CODE:
              case DOUBLE_CODE:
                this.traceDataOut.println("writeStatValue#writeCompactValue "
                    + typeCodeToString(typeCode) + " delta: " + delta);
                break;
              default:
            }
          }
        }
      }
    } catch (IllegalStateException closedEx) {
      // resource was closed causing getStatValue to throw this exception
    }

    if (wroteInstId) {
      this.dataOut.writeByte(ILLEGAL_STAT_OFFSET);
      if (this.trace
          && (traceStatisticsName == null
              || traceStatisticsName.equals(ri.getStatistics().getTextId()))
          && (traceStatisticsTypeName == null || traceStatisticsTypeName
              .equals(ri.getResourceType().getStatisticsType().getName()))) {
        this.traceDataOut
            .println("writeSample#writeByte ILLEGAL_STAT_OFFSET: " + ILLEGAL_STAT_OFFSET);
      }
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "StatArchiveWriter#writeSample statsWritten={}",
          statsWritten);
    }
  }

  public static void writeCompactValue(long v, DataOutput dataOut) throws IOException {
    if (v <= MAX_1BYTE_COMPACT_VALUE && v >= MIN_1BYTE_COMPACT_VALUE) {
      dataOut.writeByte((int) v);
    } else if (v <= MAX_2BYTE_COMPACT_VALUE && v >= MIN_2BYTE_COMPACT_VALUE) {
      dataOut.writeByte(COMPACT_VALUE_2_TOKEN);
      dataOut.writeShort((int) v);
    } else {
      byte[] buffer = new byte[8];
      int idx = 0;
      long originalValue = v;
      if (v < 0) {
        while (v != -1 && v != 0) {
          buffer[idx++] = (byte) (v & 0xFF);
          v >>= 8;
        }
        // On windows v goes to zero somtimes; seems like a bug
        if (v == 0) {
          // when this happens we end up with a bunch of -1 bytes
          // so strip off the high order ones
          while (buffer[idx - 1] == -1) {
            idx--;
          }
        }
        if ((buffer[idx - 1] & 0x80) == 0) {
          /*
           * If the most significant byte does not have its high order bit set then add a -1 byte so
           * we know this is a negative number
           */
          buffer[idx++] = -1;
        }
      } else {
        while (v != 0) {
          buffer[idx++] = (byte) (v & 0xFF);
          v >>= 8;
        }
        if ((buffer[idx - 1] & 0x80) != 0) {
          /*
           * If the most significant byte has its high order bit set then add a zero byte so we know
           * this is a positive number
           */
          buffer[idx++] = 0;
        }
      }
      if (idx <= 2) {
        throw new InternalGemFireException(
            String.format("Expected idx to be greater than 2. It was %s for the value %s",

                new Object[] {Integer.valueOf(idx), Long.valueOf(originalValue)}));
      }
      int token = COMPACT_VALUE_2_TOKEN + (idx - 2);
      dataOut.writeByte(token);
      for (int i = idx - 1; i >= 0; i--) {
        dataOut.writeByte(buffer[i]);
      }
    }
  }

  public static long readCompactValue(DataInput dataIn) throws IOException {
    long v = dataIn.readByte();
    boolean dump = false;
    if (dump) {
      System.out.print("compactValue(byte1)=" + v);
    }
    if (v < MIN_1BYTE_COMPACT_VALUE) {
      if (v == COMPACT_VALUE_2_TOKEN) {
        v = dataIn.readShort();
        if (dump) {
          System.out.print("compactValue(short)=" + v);
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

  protected static void writeStatValue(StatisticDescriptor f, long v, DataOutput dataOut)
      throws IOException {
    byte typeCode = ((StatisticDescriptorImpl) f).getTypeCode();
    writeStatValue(typeCode, v, dataOut);
  }

  public static void writeStatValue(byte typeCode, long v, DataOutput dataOut) throws IOException {
    switch (typeCode) {
      case BYTE_CODE:
        dataOut.writeByte((int) v);
        break;
      case SHORT_CODE:
        dataOut.writeShort((int) v);
        break;
      case INT_CODE:
      case FLOAT_CODE:
      case LONG_CODE:
      case DOUBLE_CODE:
        writeCompactValue(v, dataOut);
        break;
      default:
        throw new InternalGemFireException(String.format("Unexpected type code %s",
            Byte.valueOf(typeCode)));
    }
  }

  protected static void setTraceFilter(String traceStatisticsName, String traceStatisticsTypeName) {
    StatArchiveWriter.traceStatisticsName = traceStatisticsName;
    StatArchiveWriter.traceStatisticsTypeName = traceStatisticsTypeName;
    StatArchiveWriter.traceResourceInstId = -1;
  }

  protected static void clearTraceFilter() {
    StatArchiveWriter.traceStatisticsName = null;
    StatArchiveWriter.traceStatisticsTypeName = null;
    StatArchiveWriter.traceResourceInstId = -1;
  }

  private static String typeCodeToString(byte typeCode) {
    switch (typeCode) {
      case BYTE_CODE:
        return "BYTE_CODE";
      case SHORT_CODE:
        return "SHORT_CODE";
      case INT_CODE:
        return "INT_CODE";
      case FLOAT_CODE:
        return "FLOAT_CODE";
      case LONG_CODE:
        return "LONG_CODE";
      case DOUBLE_CODE:
        return "DOUBLE_CODE";
      default:
        return "unknown typeCode " + typeCode;
    }
  }

  private static class MyDataOutputStream implements DataOutput {
    private long bytesWritten = 0;
    private final DataOutputStream dataOut;

    public MyDataOutputStream(OutputStream out) {
      this.dataOut = new DataOutputStream(out);
    }

    public long getBytesWritten() {
      return this.bytesWritten;
    }

    public void flush() throws IOException {
      this.dataOut.flush();
    }

    @SuppressWarnings("unused")
    public void close() throws IOException {
      this.dataOut.close();
    }

    @Override
    public void write(int b) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void write(byte[] b) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void writeBytes(String v) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void writeChar(int v) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void writeChars(String v) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void writeDouble(double v) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void writeFloat(float v) throws IOException {
      throw new RuntimeException(
          "method unimplemented");
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
      this.dataOut.writeBoolean(v);
      this.bytesWritten += 1;
    }

    @Override
    public void writeByte(int v) throws IOException {
      this.dataOut.writeByte(v);
      this.bytesWritten += 1;
    }

    @Override
    public void writeShort(int v) throws IOException {
      this.dataOut.writeShort(v);
      this.bytesWritten += 2;
    }

    @Override
    public void writeInt(int v) throws IOException {
      this.dataOut.writeInt(v);
      this.bytesWritten += 4;
    }

    @Override
    public void writeLong(long v) throws IOException {
      this.dataOut.writeLong(v);
      this.bytesWritten += 8;
    }

    @Override
    public void writeUTF(String v) throws IOException {
      this.dataOut.writeUTF(v);
      this.bytesWritten += v.length() + 2; // this is the minimum. The max is v.size()*3 +2
    }
  }
}
