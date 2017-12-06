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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.DateFormatter;

/**
 * StatArchiveReader provides APIs to read statistic snapshots from an archive file.
 */
public class StatArchiveReader implements StatArchiveFormat {

  protected static final NumberFormat nf = NumberFormat.getNumberInstance();
  static {
    nf.setMaximumFractionDigits(2);
    nf.setGroupingUsed(false);
  }

  private final StatArchiveFile[] archives;
  private boolean dump;
  private boolean closed = false;

  /**
   * Creates a StatArchiveReader that will read the named archive file.
   *
   * @param autoClose if its <code>true</code> then the reader will close input files as soon as it
   *        finds their end.
   * @throws IOException if <code>archiveName</code> could not be opened read, or closed.
   */
  public StatArchiveReader(File[] archiveNames, ValueFilter[] filters, boolean autoClose)
      throws IOException {
    this.archives = new StatArchiveFile[archiveNames.length];
    this.dump = Boolean.getBoolean("StatArchiveReader.dumpall");
    for (int i = 0; i < archiveNames.length; i++) {
      this.archives[i] = new StatArchiveFile(this, archiveNames[i], dump, filters);
    }

    update(false, autoClose);

    if (this.dump || Boolean.getBoolean("StatArchiveReader.dump")) {
      this.dump(new PrintWriter(System.out));
    }
  }

  /**
   * Creates a StatArchiveReader that will read the named archive file.
   *
   * @throws IOException if <code>archiveName</code> could not be opened read, or closed.
   */
  public StatArchiveReader(String archiveName) throws IOException {
    this(new File[] {new File(archiveName)}, null, false);
  }

  /**
   * Returns an array of stat values that match the specified spec. If nothing matches then an empty
   * array is returned.
   */
  public StatValue[] matchSpec(StatSpec spec) {
    if (spec.getCombineType() == StatSpec.GLOBAL) {
      StatValue[] allValues = matchSpec(new RawStatSpec(spec));
      if (allValues.length == 0) {
        return allValues;
      } else {
        ComboValue cv = new ComboValue(allValues);
        // need to save this in reader's combo value list
        return new StatValue[] {cv};
      }
    } else {
      List l = new ArrayList();
      StatArchiveReader.StatArchiveFile[] archives = getArchives();
      for (int i = 0; i < archives.length; i++) {
        StatArchiveFile f = archives[i];
        if (spec.archiveMatches(f.getFile())) {
          f.matchSpec(spec, l);
        }
      }
      StatValue[] result = new StatValue[l.size()];
      return (StatValue[]) l.toArray(result);
    }
  }

  /**
   * Checks to see if any archives have changed since the StatArchiverReader instance was created or
   * last updated. If an archive has additional samples then those are read the resource instances
   * maintained by the reader are updated.
   * <p>
   * Once closed a reader can no longer be updated.
   *
   * @return true if update read some new data.
   * @throws IOException if an archive could not be opened read, or closed.
   */
  public boolean update() throws IOException {
    return update(true, false);
  }

  private boolean update(boolean doReset, boolean autoClose) throws IOException {
    if (this.closed) {
      return false;
    }
    boolean result = false;
    StatArchiveReader.StatArchiveFile[] archives = getArchives();
    for (int i = 0; i < archives.length; i++) {
      StatArchiveFile f = archives[i];
      if (f.update(doReset)) {
        result = true;
      }
      if (autoClose) {
        f.close();
      }
    }
    return result;
  }

  /**
   * Returns an unmodifiable list of all the {@link ResourceInst} this reader contains.
   */
  public List getResourceInstList() {
    return new ResourceInstList();
  }

  public StatArchiveFile[] getArchives() {
    return this.archives;
  }

  /**
   * Closes all archives.
   */
  public void close() throws IOException {
    if (!this.closed) {
      StatArchiveReader.StatArchiveFile[] archives = getArchives();
      for (int i = 0; i < archives.length; i++) {
        StatArchiveFile f = archives[i];
        f.close();
      }
      this.closed = true;
    }
  }

  private int getMemoryUsed() {
    int result = 0;
    StatArchiveReader.StatArchiveFile[] archives = getArchives();
    for (int i = 0; i < archives.length; i++) {
      StatArchiveFile f = archives[i];
      result += f.getMemoryUsed();
    }
    return result;
  }

  private void dump(PrintWriter stream) {
    StatArchiveReader.StatArchiveFile[] archives = getArchives();
    for (int i = 0; i < archives.length; i++) {
      StatArchiveFile f = archives[i];
      f.dump(stream);
    }
  }

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
        throw new InternalGemFireException(LocalizedStrings.StatArchiveReader_UNEXPECTED_TYPECODE_0
            .toLocalizedString(Integer.valueOf(type)));
    }
  }

  /**
   * Simple utility to read and dump statistic archive.
   */
  public static void main(String args[]) throws IOException {
    String archiveName = null;
    if (args.length > 1) {
      System.err.println("Usage: [archiveName]");
      ExitCode.FATAL.doSystemExit();
    } else if (args.length == 1) {
      archiveName = args[0];
    } else {
      archiveName = "statArchive.gfs";
    }
    StatArchiveReader reader = new StatArchiveReader(archiveName);
    System.out.println("DEBUG: memory used = " + reader.getMemoryUsed());
    reader.close();
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

  private class ResourceInstList extends AbstractList {
    protected ResourceInstList() {
      // nothing needed.
    }

    @Override
    public Object get(int idx) {
      int archiveIdx = 0;
      StatArchiveReader.StatArchiveFile[] archives = getArchives();
      for (int i = 0; i < archives.length; i++) {
        StatArchiveFile f = archives[i];
        if (idx < (archiveIdx + f.resourceInstSize)) {
          return f.resourceInstTable[idx - archiveIdx];
        }
        archiveIdx += f.resourceInstSize;
      }
      return null;
    }

    @Override
    public int size() {
      int result = 0;
      StatArchiveReader.StatArchiveFile[] archives = getArchives();
      for (int i = 0; i < archives.length; i++) {
        result += archives[i].resourceInstSize;
      }
      return result;
    }
  }

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
      stream.println(
          "  " + name + ": type=" + typeCode + " offset=" + offset + (isCounter ? " counter" : "")
              + " units=" + units + " largerBetter=" + largerBetter + " desc=" + desc);
    }

    protected StatDescriptor(String name, int offset, boolean isCounter, boolean largerBetter,
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
     * Returns the type code of this statistic. It will be one of the following values:
     * <ul>
     * <li>{@link #BOOLEAN_CODE}
     * <li>{@link #WCHAR_CODE}
     * <li>{@link #CHAR_CODE}
     * <li>{@link #BYTE_CODE}
     * <li>{@link #SHORT_CODE}
     * <li>{@link #INT_CODE}
     * <li>{@link #LONG_CODE}
     * <li>{@link #FLOAT_CODE}
     * <li>{@link #DOUBLE_CODE}
     * </ul>
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

  public interface StatValue {
    /**
     * {@link StatArchiveReader.StatValue} filter that causes the statistic values to be unfiltered.
     * This causes the raw values written to the archive to be used.
     * <p>
     * This is the default filter for non-counter statistics. To determine if a statistic is not a
     * counter use {@link StatArchiveReader.StatDescriptor#isCounter}.
     */
    int FILTER_NONE = 0;
    /**
     * {@link StatArchiveReader.StatValue} filter that causes the statistic values to be filtered to
     * reflect how often they change per second. Since the difference between two samples is used to
     * calculate the value this causes the {@link StatArchiveReader.StatValue} to have one less
     * sample than {@link #FILTER_NONE}. The instance time stamp that does not have a per second
     * value is the instance's first time stamp
     * {@link StatArchiveReader.ResourceInst#getFirstTimeMillis}.
     * <p>
     * This is the default filter for counter statistics. To determine if a statistic is a counter
     * use {@link StatArchiveReader.StatDescriptor#isCounter}.
     */
    int FILTER_PERSEC = 1;
    /**
     * {@link StatArchiveReader.StatValue} filter that causes the statistic values to be filtered to
     * reflect how much they changed between sample periods. Since the difference between two
     * samples is used to calculate the value this causes the {@link StatArchiveReader.StatValue} to
     * have one less sample than {@link #FILTER_NONE}. The instance time stamp that does not have a
     * per second value is the instance's first time stamp
     * {@link StatArchiveReader.ResourceInst#getFirstTimeMillis}.
     */
    int FILTER_PERSAMPLE = 2;

    /**
     * Creates and returns a trimmed version of this stat value. Any samples taken before
     * <code>startTime</code> and after <code>endTime</code> are discarded from the resulting value.
     * Set a time parameter to <code>-1</code> to not trim that side.
     */
    StatValue createTrimmed(long startTime, long endTime);

    /**
     * Returns true if value has data that has been trimmed off it by a start timestamp.
     */
    boolean isTrimmedLeft();

    /**
     * Gets the {@link StatArchiveReader.ResourceType type} of the resources that this value belongs
     * to.
     */
    ResourceType getType();

    /**
     * Gets the {@link StatArchiveReader.ResourceInst resources} that this value belongs to.
     */
    ResourceInst[] getResources();

    /**
     * Returns an array of timestamps for each unfiltered snapshot in this value. Each returned time
     * stamp is the number of millis since midnight, Jan 1, 1970 UTC.
     */
    long[] getRawAbsoluteTimeStamps();

    /**
     * Returns an array of timestamps for each unfiltered snapshot in this value. Each returned time
     * stamp is the number of millis since midnight, Jan 1, 1970 UTC. The resolution is seconds.
     */
    long[] getRawAbsoluteTimeStampsWithSecondRes();

    /**
     * Returns an array of doubles containing the unfiltered value of this statistic for each point
     * in time that it was sampled.
     */
    double[] getRawSnapshots();

    /**
     * Returns an array of doubles containing the filtered value of this statistic for each point in
     * time that it was sampled.
     */
    double[] getSnapshots();

    /**
     * Returns the number of samples taken of this statistic's value.
     */
    int getSnapshotsSize();

    /**
     * Returns the smallest of all the samples taken of this statistic's value.
     */
    double getSnapshotsMinimum();

    /**
     * Returns the largest of all the samples taken of this statistic's value.
     */
    double getSnapshotsMaximum();

    /**
     * Returns the average of all the samples taken of this statistic's value.
     */
    double getSnapshotsAverage();

    /**
     * Returns the standard deviation of all the samples taken of this statistic's value.
     */
    double getSnapshotsStandardDeviation();

    /**
     * Returns the most recent value of all the samples taken of this statistic's value.
     */
    double getSnapshotsMostRecent();

    /**
     * Returns true if sample whose value was different from previous values has been added to this
     * StatValue since the last time this method was called.
     */
    boolean hasValueChanged();

    /**
     * Returns the current filter used to calculate this statistic's values. It will be one of these
     * values:
     * <ul>
     * <li>{@link #FILTER_NONE}
     * <li>{@link #FILTER_PERSAMPLE}
     * <li>{@link #FILTER_PERSEC}
     * </ul>
     */
    int getFilter();

    /**
     * Sets the current filter used to calculate this statistic's values. The default filter is
     * {@link #FILTER_NONE} unless the statistic is a counter,
     * {@link StatArchiveReader.StatDescriptor#isCounter}, in which case its {@link #FILTER_PERSEC}.
     *
     * @param filter It must be one of these values:
     *        <ul>
     *        <li>{@link #FILTER_NONE}
     *        <li>{@link #FILTER_PERSAMPLE}
     *        <li>{@link #FILTER_PERSEC}
     *        </ul>
     * @throws IllegalArgumentException if <code>filter</code> is not a valid filter constant.
     */
    void setFilter(int filter);

    /**
     * Returns a description of this statistic.
     */
    StatDescriptor getDescriptor();
  }

  protected abstract static class AbstractValue implements StatValue {
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
        if (filter != FILTER_NONE && filter != FILTER_PERSEC && filter != FILTER_PERSAMPLE) {
          throw new IllegalArgumentException(
              LocalizedStrings.StatArchiveReader_FILTER_VALUE_0_MUST_BE_1_2_OR_3.toLocalizedString(
                  new Object[] {Integer.valueOf(filter), Integer.valueOf(FILTER_NONE),
                      Integer.valueOf(FILTER_PERSEC), Integer.valueOf(FILTER_PERSAMPLE)}));
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
      StringBuffer result = new StringBuffer();
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
      result.append(": samples=").append(getSnapshotsSize());
      if (startTime != -1) {
        result.append(" startTime=\"").append(new Date(startTime)).append("\"");
      }
      if (endTime != -1) {
        result.append(" endTime=\"").append(new Date(endTime)).append("\"");
      }
      result.append(" min=").append(nf.format(min));
      result.append(" max=").append(nf.format(max));
      result.append(" average=").append(nf.format(avg));
      result.append(" stddev=").append(nf.format(stddev));
      result.append(" last=") // for bug 42532
          .append(nf.format(mostRecent));
      return result.toString();
    }
  }

  /**
   * A ComboValue is a value that is the logical combination of a set of other stat values.
   * <p>
   * For now ComboValue has a simple implementation that does not suppport updates.
   */
  private static class ComboValue extends AbstractValue {
    private final ResourceType type;
    private final StatValue[] values;

    /**
     * Creates a ComboValue by adding all the specified values together.
     */
    ComboValue(List valueList) {
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
          /*
           * I'm not sure why this would happen. If it really can happen then this code should
           * change the filter since a client has no way to select values based on the filter.
           */
          throw new IllegalArgumentException(
              LocalizedStrings.StatArchiveReader_CANT_COMBINE_VALUES_WITH_DIFFERENT_FILTERS
                  .toLocalizedString());
        }
        if (!typeName.equals(this.values[i].getType().getName())) {
          throw new IllegalArgumentException(
              LocalizedStrings.StatArchiveReader_CANT_COMBINE_VALUES_WITH_DIFFERENT_TYPES
                  .toLocalizedString());
        }
        if (!statName.equals(this.values[i].getDescriptor().getName())) {
          throw new IllegalArgumentException(
              LocalizedStrings.StatArchiveReader_CANT_COMBINE_DIFFERENT_STATS.toLocalizedString());
        }
        if (this.values[i].getDescriptor().isCounter()) {
          // its a counter which is not the default
          if (!this.values[i].getDescriptor().isLargerBetter()) {
            // this guy has non-defaults for both use him
            bestTypeIdx = i;
          } else if (this.values[bestTypeIdx].getDescriptor()
              .isCounter() == this.values[bestTypeIdx].getDescriptor().isLargerBetter()) {
            // as long as we haven't already found a guy with defaults
            // make this guy the best type
            bestTypeIdx = i;
          }
        } else {
          // its a gauge, see if it has a non-default largerBetter
          if (this.values[i].getDescriptor().isLargerBetter()) {
            // as long as we haven't already found a guy with defaults
            if (this.values[bestTypeIdx].getDescriptor().isCounter() == this.values[bestTypeIdx]
                .getDescriptor().isLargerBetter()) {
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
      Set set = new HashSet();
      for (int i = 0; i < values.length; i++) {
        set.addAll(Arrays.asList(values[i].getResources()));
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
     * Return true if v is closer to prev. Return false if v is closer to next. Return true if v is
     * the same distance from both.
     */
    public static boolean closer(long v, long prev, long next) {
      return Math.abs(v - prev) <= Math.abs(v - next);
    }


    /**
     * Return true if the current ts must be inserted instead of being mapped to the tsAtInsertPoint
     */
    private static boolean mustInsert(int nextIdx, long[] valueTimeStamps, long tsAtInsertPoint) {
      return (nextIdx < valueTimeStamps.length) && (valueTimeStamps[nextIdx] <= tsAtInsertPoint);
    }

    public long[] getRawAbsoluteTimeStampsWithSecondRes() {
      return getRawAbsoluteTimeStamps();
    }

    public long[] getRawAbsoluteTimeStamps() {
      if (values.length == 0) {
        return new long[0];
      }
      long[] valueTimeStamps = values[0].getRawAbsoluteTimeStamps();
      int tsCount = valueTimeStamps.length + 1;
      long[] ourTimeStamps = new long[(tsCount * 2) + 1];
      System.arraycopy(valueTimeStamps, 0, ourTimeStamps, 0, valueTimeStamps.length);
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
            // System.out.println("DEBUG: skipping " + ourIdx + " because it was not closeEnough");
            ourIdx++;
            tsAtInsertPoint = ourTimeStamps[ourIdx];
          }
          if (closeEnough(tsToInsert, tsAtInsertPoint, timeDelta)
              && !mustInsert(j + 1, valueTimeStamps, tsAtInsertPoint)) {
            // It was already in our list so just go to the next one
            j++;
            ourIdx++; // never put the next timestamp at this index
            while (!closer(tsToInsert, ourTimeStamps[ourIdx - 1], ourTimeStamps[ourIdx])
                && !mustInsert(j, valueTimeStamps, ourTimeStamps[ourIdx])) {
              ourIdx++; // it is closer to the next one so skip forward on more
            }
          } else {
            // its not in our list so add it
            int endRunIdx = j + 1;
            while (endRunIdx < valueTimeStamps.length
                && valueTimeStamps[endRunIdx] < tsAtInsertPoint
                && !closeEnough(valueTimeStamps[endRunIdx], tsAtInsertPoint, timeDelta)) {
              endRunIdx++;
            }
            int numToCopy = endRunIdx - j;
            if (tsCount + numToCopy > ourTimeStamps.length) {
              // grow our timestamp array
              long[] tmp = new long[(tsCount + numToCopy) * 2];
              System.arraycopy(ourTimeStamps, 0, tmp, 0, tsCount);
              ourTimeStamps = tmp;
            }
            // make room for insert
            System.arraycopy(ourTimeStamps, ourIdx, ourTimeStamps, ourIdx + numToCopy,
                tsCount - ourIdx);
            // insert the elements
            if (numToCopy == 1) {
              ourTimeStamps[ourIdx] = valueTimeStamps[j];
            } else {
              System.arraycopy(valueTimeStamps, j, ourTimeStamps, ourIdx, numToCopy);
            }
            ourIdx += numToCopy;
            tsCount += numToCopy;
            // skip over all inserted elements
            j += numToCopy;
          }
        }
      }
      tsCount--;
      {
        int startIdx = 0;
        int endIdx = tsCount - 1;
        if (startTime != -1) {
          Assert.assertTrue(ourTimeStamps[startIdx] >= startTime);
        }
        if (endTime != -1) {
          Assert.assertTrue(endIdx == startIdx - 1 || ourTimeStamps[endIdx] < endTime);
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
     * Returns true if the timeStamp at curIdx is the one that ts is the closest to. We know that
     * timeStamps[curIdx-1], if it exists, was not the closest.
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
      for (int i = 0; i < this.values.length; i++) {
        if (this.values[i].isTrimmedLeft()) {
          return true;
        }
      }
      return false;
    }

    private double[] getRawSnapshots(long[] ourTimeStamps) {
      double[] result = new double[ourTimeStamps.length];
      if (result.length > 0) {
        for (int i = 0; i < values.length; i++) {
          long[] valueTimeStamps = values[i].getRawAbsoluteTimeStamps();
          double[] valueSnapshots = values[i].getRawSnapshots();
          double currentValue = 0.0;
          int curIdx = 0;
          if (values[i].isTrimmedLeft() && valueSnapshots.length > 0) {
            currentValue = valueSnapshots[0];
          }
          for (int j = 0; j < valueSnapshots.length; j++) {
            while (!isClosest(valueTimeStamps[j], ourTimeStamps, curIdx)) {
              if (descriptor.isCounter()) {
                result[curIdx] += currentValue;
              }

              curIdx++;
            }
            if (curIdx >= result.length) {
              // Add this to workaround bug 30288
              int samplesSkipped = valueSnapshots.length - j;
              StringBuffer msg = new StringBuffer(100);
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
      return new ResourceInst[] {this.resource};
    }

    public boolean isTrimmedLeft() {
      return getStartIdx() != 0;
    }

    private int getStartIdx() {
      int startIdx = 0;
      if (startTime != -1) {
        long startTimeStamp = startTime - resource.getTimeBase();
        long[] timestamps = resource.getAllRawTimeStamps();
        for (int i = resource.getFirstTimeStampIdx(); i < resource.getFirstTimeStampIdx()
            + series.getSize(); i++) {
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
        for (int i = resource.getFirstTimeStampIdx() + startIdx; i < resource.getFirstTimeStampIdx()
            + series.getSize(); i++) {
          if (timestamps[i] >= endTimeStamp) {
            break;
          }
          endIdx++;
        }
        Assert.assertTrue(endIdx == startIdx - 1 || timestamps[endIdx] < endTimeStamp);
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
        double[] values = series.getValuesEx(descriptor.getTypeCode(), startIdx, resultSize);
        for (int i = 0; i < result.length; i++) {
          double valueDelta = values[i + 1] - values[i];
          if (filter == FILTER_PERSEC) {
            double timeDelta = (timestamps[tsIdx + i + 1] - timestamps[tsIdx + i]); // millis
            valueDelta /= (timeDelta / 1000); // per second
          }
          result[i] = valueDelta;
        }
      } else {
        result = series.getValuesEx(descriptor.getTypeCode(), startIdx, resultSize);
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
      stream.print("[size=" + getSnapshotsSize() + " min=" + nf.format(min) + " max="
          + nf.format(max) + " avg=" + nf.format(avg) + " stddev=" + nf.format(stddev) + "]");
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

  private abstract static class BitInterval {
    /** Returns number of items added to values */
    abstract int fill(double[] values, int valueOffset, int typeCode, int skipCount);

    abstract void dump(PrintWriter stream);

    abstract boolean attemptAdd(long addBits, long addInterval, int addCount);

    int getMemoryUsed() {
      return 0;
    }

    protected int count;

    public int getSampleCount() {
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
            return new BitNonZeroIntIntInterval((int) bits, (int) interval, count);
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

  private abstract static class BitNonZeroInterval extends BitInterval {
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

  private abstract static class BitZeroInterval extends BitInterval {
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
        values[valueOffset + i] = bitsToDouble(typeCode, bitArray[skipCount + i]);
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

    public double[] getValues(int typeCode) {
      return getValuesEx(typeCode, 0, getSize());
    }

    /**
     * Gets the first "resultSize" values of this series skipping over the first "samplesToSkip"
     * ones. The first value in a series is at index 0. The maximum result size can be obtained by
     * calling "getSize()".
     */
    public double[] getValuesEx(int typeCode, int samplesToSkip, int resultSize) {
      double[] result = new double[resultSize];
      int firstInterval = 0;
      int idx = 0;
      while (samplesToSkip > 0 && firstInterval <= intervalIdx
          && intervals[firstInterval].getSampleCount() <= samplesToSkip) {
        samplesToSkip -= intervals[firstInterval].getSampleCount();
        firstInterval++;
      }
      for (int i = firstInterval; i <= intervalIdx; i++) {
        idx += intervals[i].fill(result, idx, typeCode, samplesToSkip);
        samplesToSkip = 0;
      }
      if (currentCount != 0) {
        idx += BitInterval.create(currentStartBits, currentInterval, currentCount).fill(result, idx,
            typeCode, samplesToSkip);
      }
      // assert
      if (idx != resultSize) {
        throw new InternalGemFireException(
            LocalizedStrings.StatArchiveReader_GETVALUESEX_DIDNT_FILL_THE_LAST_0_ENTRIES_OF_ITS_RESULT
                .toLocalizedString(Integer.valueOf(resultSize - idx)));
      }
      return result;
    }

    void dump(PrintWriter stream) {
      stream.print("[size=" + count + " intervals=" + (intervalIdx + 1) + " memused="
          + getMemoryUsed() + " ");
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
        BitInterval.create(currentStartBits, currentInterval, currentCount).dump(stream);
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
          intervals[0] = BitInterval.create(currentStartBits, currentInterval, currentCount);
        } else {
          if (!intervals[intervalIdx].attemptAdd(currentStartBits, currentInterval, currentCount)) {
            // wouldn't fit in current bit interval so add a new one
            intervalIdx++;
            if (intervalIdx >= intervals.length) {
              BitInterval[] tmp = new BitInterval[intervals.length * 2];
              System.arraycopy(intervals, 0, tmp, 0, intervals.length);
              intervals = tmp;
            }
            intervals[intervalIdx] =
                BitInterval.create(currentStartBits, currentInterval, currentCount);
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
    private static final int GROW_SIZE = 256;
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
     * Provides direct access to underlying data. Do not modify contents and use getSize() to keep
     * from reading past end of array.
     */
    long[] getRawTimeStamps() {
      return this.timeStamps;
    }

    long getMilliTimeStamp(int idx) {
      return this.base + this.timeStamps[idx];
    }

    /**
     * Returns an array of time stamp values the first of which has the specified index. Each
     * returned time stamp is the number of millis since midnight, Jan 1, 1970 UTC.
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
   * Defines a statistic resource type. Each resource instance must be of a single type. The type
   * defines what statistics each instance of it will support. The type also has a description of
   * itself.
   */
  public static class ResourceType {
    private boolean loaded;
    private final String name;
    private String desc;
    private final StatDescriptor[] stats;
    private Map descriptorMap;

    public void dump(PrintWriter stream) {
      if (loaded) {
        stream.println(name + ": " + desc);
        for (int i = 0; i < stats.length; i++) {
          stats[i].dump(stream);
        }
      }
    }

    protected ResourceType(int id, String name, int statCount) {
      this.loaded = false;
      this.name = name;
      this.desc = null;
      this.stats = new StatDescriptor[statCount];
      this.descriptorMap = null;
    }

    protected ResourceType(int id, String name, String desc, int statCount) {
      this.loaded = true;
      this.name = name;
      this.desc = desc;
      this.stats = new StatDescriptor[statCount];
      this.descriptorMap = new HashMap();
    }

    public boolean isLoaded() {
      return this.loaded;
    }

    /**
     * Frees up any resources no longer needed after the archive file is closed. Returns true if
     * this guy is no longer needed.
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
      for (int i = 0; i < this.stats.length; i++) {
        this.stats[i].unload();
      }
      this.descriptorMap.clear();
      this.descriptorMap = null;
    }

    protected void addStatDescriptor(StatArchiveFile archive, int offset, String name,
        boolean isCounter, boolean largerBetter, byte typeCode, String units, String desc) {
      StatDescriptor descriptor =
          new StatDescriptor(name, offset, isCounter, largerBetter, typeCode, units, desc);
      this.stats[offset] = descriptor;
      if (archive.loadStatDescriptor(descriptor, this)) {
        descriptorMap.put(name, descriptor);
      }
    }

    /**
     * Returns the name of this resource type.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns an array of descriptors for each statistic this resource type supports.
     */
    public StatDescriptor[] getStats() {
      return this.stats;
    }

    /**
     * Gets a stat descriptor contained in this type given the stats name.
     *
     * @param name the name of the stat to find in the current type
     * @return the descriptor that matches the name or null if the type does not have a stat of the
     *         given name
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

    public ArchiveInfo(StatArchiveFile archive, byte archiveVersion, long startTimeStamp,
        long systemStartTimeStamp, int timeZoneOffset, String timeZoneName, String systemDirectory,
        long systemId, String productVersion, String os, String machine) {
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
     * Returns the difference, measured in milliseconds, between the time the archive file was
     * create and midnight, January 1, 1970 UTC.
     */
    public long getStartTimeMillis() {
      return this.startTimeStamp;
    }

    /**
     * Returns the difference, measured in milliseconds, between the time the archived system was
     * started and midnight, January 1, 1970 UTC.
     */
    public long getSystemStartTimeMillis() {
      return this.systemStartTimeStamp;
    }

    /**
     * Returns a numeric id of the archived system. It can be used in conjunction with the
     * {@link #getSystemStartTimeMillis} to uniquely identify an archived system.
     */
    public long getSystemId() {
      return this.systemId;
    }

    /**
     * Returns a string describing the operating system the archive was written on.
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
     * Returns the time zone used when the archive was created. This can be used to print timestamps
     * in the same time zone that was in effect when the archive was created.
     */
    public TimeZone getTimeZone() {
      TimeZone result = TimeZone.getTimeZone(this.timeZoneName);
      if (result.getRawOffset() != this.timeZoneOffset) {
        result = new SimpleTimeZone(this.timeZoneOffset, this.timeZoneName);
      }
      return result;
    }

    /**
     * Returns a string containing the version of the product that wrote this archive.
     */
    public String getProductVersion() {
      return this.productVersion;
    }

    /**
     * Returns a numeric code that represents the format version used to encode the archive as a
     * stream of bytes.
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
     * Return the name of the file this archive was stored in or an empty string if the archive was
     * not stored in a file.
     */
    public String getArchiveFileName() {
      if (this.archive != null) {
        return this.archive.getFile().getPath();
      } else {
        return "";
      }
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
      if (archive != null) {
        stream.println("archive=" + archive.getFile());
      }
      stream.println("archiveVersion=" + archiveVersion);
      if (archive != null) {
        stream.println("startDate=" + archive.formatTimeMillis(startTimeStamp));
      }
      // stream.println("startTimeStamp=" + startTimeStamp +" tz=" + timeZoneName + " tzOffset=" +
      // timeZoneOffset);
      // stream.println("timeZone=" + getTimeZone().getDisplayName());
      stream.println("systemDirectory=" + systemDirectory);
      if (archive != null) {
        stream.println("systemStartDate=" + archive.formatTimeMillis(systemStartTimeStamp));
      }
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
        for (int i = 0; i < values.length; i++) {
          result += this.values[i].getMemoryUsed();
        }
      }
      return result;
    }

    public StatArchiveReader getReader() {
      return archive.getReader();
    }

    /**
     * Returns a string representation of this object.
     */
    @Override
    public String toString() {
      StringBuffer result = new StringBuffer();
      result.append(name).append(", ").append(id).append(", ").append(type.getName()).append(": \"")
          .append(archive.formatTimeMillis(getFirstTimeMillis())).append('\"');
      if (!active) {
        result.append(" inactive");
      }
      result.append(" samples=" + getSampleCount());
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
      stream.println(
          name + ":" + " file=" + getArchive().getFile() + " id=" + id + (active ? "" : " deleted")
              + " start=" + archive.formatTimeMillis(getFirstTimeMillis()));
      for (int i = 0; i < values.length; i++) {
        values[i].dump(stream);
      }
    }

    protected ResourceInst(StatArchiveFile archive, int uniqueId, String name, long id,
        ResourceType type, boolean loaded) {
      this.loaded = loaded;
      this.archive = archive;
      this.name = name;
      this.id = id;
      Assert.assertTrue(type != null);
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

    void matchSpec(StatSpec spec, List matchedValues) {
      if (spec.typeMatches(this.type.getName())) {
        if (spec.instanceMatches(this.getName(), this.getId())) {
          for (int statIdx = 0; statIdx < values.length; statIdx++) {
            if (values[statIdx] != null) {
              if (spec.statMatches(values[statIdx].getDescriptor().getName())) {
                matchedValues.add(values[statIdx]);
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
     * Frees up any resources no longer needed after the archive file is closed. Returns true if
     * this guy is no longer needed.
     */
    protected boolean close() {
      if (isLoaded()) {
        for (int i = 0; i < values.length; i++) {
          if (values[i] != null) {
            values[i].shrink();
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
     * Returns an array of doubles containing the timestamps at which this instances samples where
     * taken. Each of these timestamps is the difference, measured in milliseconds, between the
     * sample time and midnight, January 1, 1970 UTC. Although these values are double they can
     * safely be converted to <code>long</code> with no loss of information.
     */
    public double[] getSnapshotTimesMillis() {
      return archive.getTimeStamps().getTimeValuesSinceIdx(firstTSidx);
    }

    /**
     * Returns an array of statistic value descriptors. Each element of the array describes the
     * corresponding statistic this instance supports. The <code>StatValue</code> instances can be
     * used to obtain the actual sampled values of the instances statistics.
     */
    public StatValue[] getStatValues() {
      return this.values;
    }

    /**
     * Gets the value of the stat in the current instance given the stat name.
     *
     * @param name the name of the stat to find in the current instance
     * @return the value that matches the name or null if the instance does not have a stat of the
     *         given name
     *
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
     * Returns the difference, measured in milliseconds, between the time of the instance's first
     * sample and midnight, January 1, 1970 UTC.
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
     * Returns true if archive might still have future samples for this instance.
     */
    public boolean isActive() {
      return this.active;
    }

    protected void addTimeStamp() {
      if (this.loaded) {
        if (firstTSidx == -1) {
          firstTSidx = archive.getTimeStamps().getSize() - 1;
        }
        for (int i = 0; i < values.length; i++) {
          if (values[i] != null) {
            values[i].addSample();
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
      if (this.firstTSidx != other.firstTSidx) {
        return false;
      }
      return true;
    }
  }

  public interface StatSpec extends ValueFilter {
    /**
     * Causes all stats that matches this spec, in all archive files, to be combined into a single
     * global stat value.
     */
    int GLOBAL = 2;
    /**
     * Causes all stats that matches this spec, in each archive file, to be combined into a single
     * stat value for each file.
     */
    int FILE = 1;
    /**
     * No combination is done.
     */
    int NONE = 0;

    /**
     * Returns one of the following values: {@link #GLOBAL}, {@link #FILE}, {@link #NONE}.
     */
    int getCombineType();
  }

  /**
   * Specifies what data from a statistic archive will be of interest to the reader. This is used
   * when loading a statistic archive file to reduce the memory footprint. Only statistic data that
   * matches all four will be selected for loading.
   */
  public interface ValueFilter {
    /**
     * Returns true if the specified archive file matches this spec. Any archives whose name does
     * not match this spec will not be selected for loading by this spec.
     */
    boolean archiveMatches(File archive);

    /**
     * Returns true if the specified type name matches this spec. Any types whose name does not
     * match this spec will not be selected for loading by this spec.
     */
    boolean typeMatches(String typeName);

    /**
     * Returns true if the specified statistic name matches this spec. Any stats whose name does not
     * match this spec will not be selected for loading by this spec.
     */
    boolean statMatches(String statName);

    /**
     * Returns true if the specified instance matches this spec. Any instance whose text id and
     * numeric id do not match this spec will not be selected for loading by this spec.
     */
    boolean instanceMatches(String textId, long numericId);
  }

  public static class StatArchiveFile {
    private final StatArchiveReader reader;
    private InputStream is;
    private DataInputStream dataIn;
    private ValueFilter[] filters;
    private final File archiveName;
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
    private final DateFormat timeFormatter = new SimpleDateFormat(DateFormatter.FORMAT_STRING);
    private static final int BUFFER_SIZE = 1024 * 1024;
    private final ArrayList fileComboValues = new ArrayList();


    public StatArchiveFile(StatArchiveReader reader, File archiveName, boolean dump,
        ValueFilter[] filters) throws IOException {
      this.reader = reader;
      this.archiveName = archiveName;
      this.dump = dump;
      this.compressed = archiveName.getPath().endsWith(".gz");
      this.is = new FileInputStream(this.archiveName);
      if (this.compressed) {
        this.dataIn = new DataInputStream(
            new BufferedInputStream(new GZIPInputStream(this.is, BUFFER_SIZE), BUFFER_SIZE));
      } else {
        this.dataIn = new DataInputStream(new BufferedInputStream(this.is, BUFFER_SIZE));
      }
      this.updateOK = this.dataIn.markSupported();
      this.filters = createFilters(filters);
    }

    private ValueFilter[] createFilters(ValueFilter[] allFilters) {
      if (allFilters == null) {
        return new ValueFilter[0];
      }
      ArrayList l = new ArrayList();
      for (int i = 0; i < allFilters.length; i++) {
        if (allFilters[i].archiveMatches(this.getFile())) {
          l.add(allFilters[i]);
        }
      }
      if (l.size() == allFilters.length) {
        return allFilters;
      } else {
        ValueFilter[] result = new ValueFilter[l.size()];
        return (ValueFilter[]) l.toArray(result);
      }
    }

    StatArchiveReader getReader() {
      return this.reader;
    }

    void matchSpec(StatSpec spec, List matchedValues) {
      if (spec.getCombineType() == StatSpec.FILE) {
        // search for previous ComboValue
        Iterator it = this.fileComboValues.iterator();
        while (it.hasNext()) {
          ComboValue v = (ComboValue) it.next();
          if (!spec.statMatches(v.getDescriptor().getName())) {
            continue;
          }
          if (!spec.typeMatches(v.getType().getName())) {
            continue;
          }
          ResourceInst[] resources = v.getResources();
          for (int i = 0; i < resources.length; i++) {
            if (!spec.instanceMatches(resources[i].getName(), resources[i].getId())) {
              continue;
            }
            // note: we already know the archive file matches
          }
          matchedValues.add(v);
          return;
        }
        ArrayList l = new ArrayList();
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
     * Formats an archive timestamp in way consistent with GemFire log dates. It will also be
     * formatted to reflect the time zone the archive was created in.
     *
     * @param ts The difference, measured in milliseconds, between the time marked by this time
     *        stamp and midnight, January 1, 1970 UTC.
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
     * Checks to see if the archive has changed since the StatArchiverReader instance was created or
     * last updated. If the archive has additional samples then those are read the resource
     * instances maintained by the reader are updated.
     * <p>
     * Once closed a reader can no longer be updated.
     *
     * @return true if update read some new data.
     * @throws IOException if <code>archiveName</code> could not be opened read, or closed.
     */
    public boolean update(boolean doReset) throws IOException {
      if (this.closed) {
        return false;
      }
      if (!this.updateOK) {
        throw new InternalGemFireException(
            LocalizedStrings.StatArchiveReader_UPDATE_OF_THIS_TYPE_OF_FILE_IS_NOT_SUPPORTED
                .toLocalizedString());
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
      stream.print("archive=" + archiveName);
      if (info != null) {
        info.dump(stream);
      }
      for (int i = 0; i < resourceTypeTable.length; i++) {
        if (resourceTypeTable[i] != null) {
          resourceTypeTable[i].dump(stream);
        }
      }
      stream.print("time=");
      timeSeries.dump(stream);
      for (int i = 0; i < resourceInstTable.length; i++) {
        if (resourceInstTable[i] != null) {
          resourceInstTable[i].dump(stream);
        }
      }
    }

    public File getFile() {
      return this.archiveName;
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
          for (int i = 0; i < this.resourceTypeTable.length; i++) {
            if (this.resourceTypeTable[i] != null) {
              newTypeTable[typeCount] = this.resourceTypeTable[i];
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
          for (int i = 0; i < this.resourceInstTable.length; i++) {
            if (this.resourceInstTable[i] != null) {
              newInstTable[instCount] = this.resourceInstTable[i];
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
     * Returns global information about the read archive. Returns null if no information is
     * available.
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
        throw new GemFireIOException(
            LocalizedStrings.StatArchiveReader_ARCHIVE_VERSION_0_IS_NO_LONGER_SUPPORTED
                .toLocalizedString(Byte.valueOf(archiveVersion)),
            null);
      }
      if (archiveVersion > ARCHIVE_VERSION) {
        throw new GemFireIOException(
            LocalizedStrings.StatArchiveReader_UNSUPPORTED_ARCHIVE_VERSION_0_THE_SUPPORTED_VERSION_IS_1
                .toLocalizedString(
                    new Object[] {Byte.valueOf(archiveVersion), Byte.valueOf(ARCHIVE_VERSION)}),
            null);
      }
      this.archiveVersion = archiveVersion;
      this.info = new ArchiveInfo(this, archiveVersion, startTimeStamp, systemStartTimeStamp,
          timeZoneOffset, timeZoneName, systemDirectory, systemId, productVersion, os, machine);
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
        for (int i = 0; i < filters.length; i++) {
          if (filters[i].typeMatches(typeName)) {
            return true;
          }
        }
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
        for (int i = 0; i < filters.length; i++) {
          if (filters[i].statMatches(stat.getName()) && filters[i].typeMatches(type.getName())) {
            return true;
          }
        }
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
        for (int i = 0; i < filters.length; i++) {
          if (filters[i].typeMatches(type.getName())) {
            if (filters[i].instanceMatches(textId, numericId)) {
              StatDescriptor[] stats = type.getStats();
              for (int j = 0; j < stats.length; j++) {
                if (stats[j].isLoaded()) {
                  if (filters[i].statMatches(stats[j].getName())) {
                    return true;
                  }
                }
              }
            }
          }
        }
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
        for (int i = 0; i < filters.length; i++) {
          if (filters[i].statMatches(stat.getName()) && filters[i].typeMatches(type.getName())
              && filters[i].instanceMatches(textId, numericId)) {
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
        System.arraycopy(resourceTypeTable, 0, tmp, 0, resourceTypeTable.length);
        resourceTypeTable = tmp;
      }
      Assert.assertTrue(resourceTypeTable[resourceTypeId] == null);

      ResourceType rt;
      if (loadType(resourceTypeName)) {
        rt = new ResourceType(resourceTypeId, resourceTypeName, resourceTypeDesc, statCount);
        if (dump) {
          System.out.println("ResourceType id=" + resourceTypeId + " name=" + resourceTypeName
              + " statCount=" + statCount + " desc=" + resourceTypeDesc);
        }
      } else {
        rt = new ResourceType(resourceTypeId, resourceTypeName, statCount);
        if (dump) {
          System.out.println(
              "Not loading ResourceType id=" + resourceTypeId + " name=" + resourceTypeName);
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
        rt.addStatDescriptor(this, i, statName, isCounter, largerBetter, typeCode, units, desc);
        if (dump) {
          System.out.println("  " + i + "=" + statName + " isCtr=" + isCounter + " largerBetter="
              + largerBetter + " typeCode=" + typeCode + " units=" + units + " desc=" + desc);
        }
      }
    }

    private void readResourceInstanceCreateToken(boolean initialize) throws IOException {
      int resourceInstId = dataIn.readInt();
      String name = dataIn.readUTF();
      long id = dataIn.readLong();
      int resourceTypeId = dataIn.readInt();
      while (resourceInstId >= resourceInstTable.length) {
        ResourceInst[] tmp = new ResourceInst[resourceInstTable.length + 128];
        System.arraycopy(resourceInstTable, 0, tmp, 0, resourceInstTable.length);
        resourceInstTable = tmp;
      }
      Assert.assertTrue(resourceInstTable[resourceInstId] == null);
      if ((resourceInstId + 1) > this.resourceInstSize) {
        this.resourceInstSize = resourceInstId + 1;
      }
      ResourceType type = resourceTypeTable[resourceTypeId];
      if (type == null) {
        throw new IllegalStateException("ResourceType is missing for resourceTypeId "
            + resourceTypeId + ", resourceName " + name);
      }
      boolean loadInstance = loadInstance(name, id, resourceTypeTable[resourceTypeId]);
      resourceInstTable[resourceInstId] = new ResourceInst(this, resourceInstId, name, id,
          resourceTypeTable[resourceTypeId], loadInstance);
      if (dump) {
        System.out.println(
            (loadInstance ? "Loaded" : "Did not load") + " resource instance " + resourceInstId);
        System.out.println("  name=" + name + " id=" + id + " typeId=" + resourceTypeId);
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
              throw new IOException(LocalizedStrings.StatArchiveReader_UNEXPECTED_TYPECODE_VALUE_0
                  .toLocalizedString(Byte.valueOf(stats[i].getTypeCode())));
          }
          resourceInstTable[resourceInstId].initialValue(i, v);
        }
      }
    }

    private void readResourceInstanceDeleteToken() throws IOException {
      int resourceInstId = dataIn.readInt();
      Assert.assertTrue(resourceInstTable[resourceInstId] != null);
      resourceInstTable[resourceInstId].makeInactive();
      if (dump) {
        System.out.println("Delete resource instance " + resourceInstId);
      }
    }

    private int readResourceInstId() throws IOException {
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
      return StatArchiveWriter.readCompactValue(this.dataIn);
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
              throw new IOException(LocalizedStrings.StatArchiveReader_UNEXPECTED_TYPECODE_VALUE_0
                  .toLocalizedString(Byte.valueOf(stats[statOffset].getTypeCode())));
          }
          if (resourceInstTable[resourceInstId].addValueSample(statOffset, statDeltaBits)) {
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
      for (int i = 0; i < resourceInstTable.length; i++) {
        ResourceInst inst = resourceInstTable[i];
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
            throw new IOException(LocalizedStrings.StatArchiveReader_UNEXPECTED_TOKEN_BYTE_VALUE_0
                .toLocalizedString(Byte.valueOf(token)));
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
  }
}
