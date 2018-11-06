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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.io.RollingFileHandler;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * Captures sample of statistics. The SampleCollector contains maps of StatisticsTypes to
 * ResourceTypes and Statistics instances to ResourceInstances, each of which contains the actual
 * stat values of the sample. The {@link #sample(long)} operation determines what stats have
 * changed, adds in new types and instances, and updates the sampled stat values.
 * <p/>
 * SampleHandlers are registered with the SampleCollector. The handlers are notified of any changes
 * to statistics.
 * <p/>
 * Extracted from StatArchiveWriter.
 *
 * @since GemFire 7.0
 */
public class SampleCollector {

  private static final Logger logger = LogService.getLogger();

  /**
   * Singleton instance of SampleCollector set during initialization. This field simply points to
   * the latest initialized instance.
   */
  private static SampleCollector instance;

  /** The stat sampler using this collector to capture samples of stats */
  private final StatisticsSampler sampler;

  /** The handlers that are registered for notification of stat samples */
  private final SampleHandlers sampleHandlers = new SampleHandlers();

  /**
   * Map of StatisticsType to ResourceType. Contains all currently known statistics types.
   */
  private final Map<StatisticsType, ResourceType> resourceTypeMap =
      new HashMap<StatisticsType, ResourceType>();

  /**
   * Map of Statistics to ResourceInstance. Contains all currently known statistics resources.
   */
  private final Map<Statistics, ResourceInstance> resourceInstMap =
      new HashMap<Statistics, ResourceInstance>();

  /** Incremented to use as unique identifier to construct new ResourceType */
  private int resourceTypeId = 0;

  /** Incremented to use as unique identifier to construct new ResourceInstance */
  private int resourceInstId = 0;

  /** The number of statistics resources that existed during the latest sample */
  private int statResourcesModCount;

  /** The StatArchiveHandler which is created during initialization */
  private StatArchiveHandler statArchiveHandler;

  /** The StatArchiveHandler which is created on demand */
  private StatMonitorHandler statMonitorHandler;

  /**
   * Constructs a new instance.
   *
   * @param sampler the stat sampler using this collector
   */
  public SampleCollector(StatisticsSampler sampler) {
    this.sampler = sampler;
    if (sampler.getStatisticsModCount() == 0) {
      this.statResourcesModCount = -1;
    } else {
      this.statResourcesModCount = 0;
    }
  }

  /**
   * Returns the {@link StatMonitorHandler}. If one does not currently exist it will be created.
   *
   * @return the StatMonitorHandler for adding monitors
   *
   * @throws IllegalStateException if no SampleCollector has been created and initialized yet
   */
  static StatMonitorHandler getStatMonitorHandler() {
    // sync SampleCollector.class and then instance.sampleHandlers
    synchronized (SampleCollector.class) {
      if (instance == null) {
        throw new IllegalStateException("Statistics sampler is not available");
      }
      synchronized (instance.sampleHandlers) {
        StatMonitorHandler handler = instance.statMonitorHandler;
        if (handler == null) {
          handler = new StatMonitorHandler();
          instance.addSampleHandler(handler);
          instance.statMonitorHandler = handler;
        }
        return handler;
      }
    }
  }

  /**
   * Initializes this collector by creating a {@link StatArchiveHandler} and registering it as a
   * handler.
   *
   * @param config defines the configuration for the StatArchiveHandler
   * @param nanosTimeStamp the nanos time stamp to initialize stat archiver with
   * @param rollingFileHandler provides file rolling behavior
   */
  @SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "There is never more than one SampleCollector instance.")
  public void initialize(final StatArchiveHandlerConfig config, final long nanosTimeStamp,
      final RollingFileHandler rollingFileHandler) {
    synchronized (SampleCollector.class) {
      instance = this;
      synchronized (this.sampleHandlers) {
        StatArchiveHandler newStatArchiveHandler =
            new StatArchiveHandler(config, this, rollingFileHandler);
        this.statArchiveHandler = newStatArchiveHandler;
        addSampleHandler(newStatArchiveHandler);
        newStatArchiveHandler.initialize(nanosTimeStamp);
      }
    }
  }

  public boolean isInitialized() {
    synchronized (SampleCollector.class) {
      synchronized (this.sampleHandlers) {
        return instance == this;
      }
    }
  }

  /**
   * Adds a {@link SampleHandler} to this collector.
   *
   * @param handler the SampleHandler to add
   */
  public void addSampleHandler(SampleHandler handler) {
    this.sampleHandlers.addSampleHandler(handler);
  }

  /**
   * Removes a {@link SampleHandler} from this collector.
   *
   * @param handler the SampleHandler to remove
   */
  public void removeSampleHandler(SampleHandler handler) {
    this.sampleHandlers.removeSampleHandler(handler);
  }

  /** Returns true if the specified SampleHandler is registered */
  public boolean containsSampleHandler(SampleHandler handler) {
    return this.sampleHandlers.contains(handler);
  }

  /**
   * Collect a sample of all statistics. Adds new statistics resources, removes destroyed statistics
   * resources, collects the latest stat values, and notifies SamplerHandlers of the sample.
   * <p/>
   * The timeStamp is an arbitrary nanoseconds time stamp only used for archiving to stats files.
   * The initial time in system milliseconds and the first NanoTimer timeStamp are both written to
   * the archive file. Thereafter only the NanoTimer timeStamp is written to the archive file for
   * each sample. The delta in nanos can be determined by comparing any nano timeStamp to the first
   * nano timeStamp written to the archive file. Adding this delta to the recorded initial time in
   * milliseconds provides the actual (non-arbitrary) time for each sample.
   *
   * @param nanosTimeStamp an arbitrary time stamp in nanoseconds for this sample
   */
  public void sample(long nanosTimeStamp) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#sample nanosTimeStamp={}",
          nanosTimeStamp);
    }
    List<MarkableSampleHandler> handlers = this.sampleHandlers.currentHandlers();
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#sample handlers={}", handlers);
    }
    sampleResources(handlers);

    List<ResourceInstance> updatedResources = new ArrayList<ResourceInstance>();
    for (ResourceInstance ri : this.resourceInstMap.values()) {
      StatisticDescriptor[] stats = ri.getResourceType().getStatisticDescriptors();
      if (ri.getStatistics().isClosed()) {
        continue;
      }

      // size updatedStats to same size as stats in case all stats changed
      int[] updatedStats = new int[stats.length];
      long[] statValues = ri.getPreviousStatValues();

      if (statValues == null) {
        statValues = new long[stats.length];
        for (int i = 0; i < stats.length; i++) {
          statValues[i] = ri.getRawStatValue(stats[i]);
          updatedStats[i] = i;
        }

      } else {
        statValues = Arrays.copyOf(statValues, statValues.length);
        int updatedStatsIdx = 0;
        for (int i = 0; i < stats.length; i++) {
          long value = ri.getRawStatValue(stats[i]);
          if (value != statValues[i]) {
            statValues[i] = value;
            updatedStats[updatedStatsIdx] = i;
            updatedStatsIdx++;
          }
        }
        // resize updatedStats to only contain the the stats which were updated
        updatedStats = Arrays.copyOf(updatedStats, updatedStatsIdx);
      }

      ri.setUpdatedStats(updatedStats);
      ri.setLatestStatValues(statValues);
      updatedResources.add(ri);
    }

    try {
      notifyAllHandlersOfSample(handlers, updatedResources, nanosTimeStamp);
    } catch (IllegalArgumentException e) {
      logger.warn(LogMarker.STATISTICS_MARKER,
          "Use of java.lang.System.nanoTime() resulted in a non-positive timestamp delta. Skipping notification of statistics sample.",
          e);
    }

    for (ResourceInstance ri : updatedResources) {
      ri.setPreviousStatValues(ri.getLatestStatValues());
    }
  }

  public void close() {
    // sync SampleCollector.class and then this.sampleHandlers
    synchronized (SampleCollector.class) {
      synchronized (this.sampleHandlers) {
        if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
          logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#close");
        }
        try {
          StatArchiveHandler handler = this.statArchiveHandler;
          if (handler != null) {
            handler.close();
          }
        } catch (GemFireException ignore) {
          logger.warn(LogMarker.STATISTICS_MARKER, "Statistic archiver shutdown failed because: {}",
              ignore.getMessage());
        }
        StatMonitorHandler handler = this.statMonitorHandler;
        if (handler != null) {
          handler.close();
        }
      }
      if (instance == this) {
        instance = null;
      }
    }
  }

  public void changeArchive(File newFile, long nanosTimeStamp) {
    synchronized (this.sampleHandlers) {
      if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
        logger.trace(LogMarker.STATISTICS_VERBOSE,
            "SampleCollector#changeArchive newFile={}, nanosTimeStamp={}", newFile, nanosTimeStamp);
      }
      StatArchiveHandler handler = this.statArchiveHandler;
      if (handler != null) {
        handler.changeArchiveFile(newFile, nanosTimeStamp);
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("sampler=").append(this.sampler);
    sb.append(", statResourcesModCount=").append(this.statResourcesModCount);
    sb.append(", resourceTypeId=").append(this.resourceTypeId);
    sb.append(", resourceInstId=").append(this.resourceInstId);
    sb.append(", resourceTypeMap.size()=").append(this.resourceTypeMap.size());
    sb.append(", resourceInstMap.size()=").append(this.resourceInstMap.size());
    sb.append("}");
    return sb.toString();
  }

  /** For testing only */
  StatArchiveHandler getStatArchiveHandler() {
    synchronized (this.sampleHandlers) {
      return this.statArchiveHandler;
    }
  }

  protected List<MarkableSampleHandler> currentHandlersForTesting() {
    return this.sampleHandlers.currentHandlers();
  }

  /**
   * Detect and archive any new resource additions or deletions
   */
  private void sampleResources(List<MarkableSampleHandler> handlers) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#sampleResources handlers={}",
          handlers);
    }
    int newModCount = this.sampler.getStatisticsModCount();
    // TODO: what if one is deleted and one is added
    if (this.statResourcesModCount != newModCount) {
      this.statResourcesModCount = newModCount;
      int ignoreCount = 0;

      Statistics[] resources = this.sampler.getStatistics();
      for (int i = 0; i < resources.length; i++) {
        Statistics statistics = resources[i];

        // only notify marked/old handlers of new types or resources
        if (!this.resourceInstMap.containsKey(statistics)) {
          try {
            ResourceType type = getResourceType(handlers, statistics);
            ResourceInstance resource = allocateResourceInstance(type, statistics);
            notifyOldHandlersOfResource(handlers, resource);
          } catch (IgnoreResourceException ex) {
            ignoreCount++;
          }
        }
      }

      if (isDebugEnabled_STATISTICS) {
        logger.trace(LogMarker.STATISTICS_VERBOSE,
            "SampleCollector#sampleResources resources.length={}, ignoreCount={}", resources.length,
            ignoreCount);
      }
      List<ResourceInstance> ri = cleanupResources(resources, ignoreCount);
      notifyOldHandlers(handlers, ri);
    }

    // notify unmarked/new handlers but not marked/old handlers
    notifyNewHandlersOfResources(handlers, this.resourceTypeMap.values(),
        this.resourceInstMap.values());
  }

  private ResourceType getResourceType(List<MarkableSampleHandler> handlers, Statistics statistics)
      throws IgnoreResourceException {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#getResourceType statistics={}",
          statistics);
    }
    StatisticsType type = statistics.getType();
    if (type == null) {
      // bug 30707
      if (isDebugEnabled_STATISTICS) {
        logger.trace(LogMarker.STATISTICS_VERBOSE,
            "SampleCollector#getResourceType type={}, throwing IgnoreResourceException", type);
      }
      throw new IgnoreResourceException();
    }
    ResourceType resourceType = null;
    try {
      resourceType = (ResourceType) this.resourceTypeMap.get(type);
    } catch (NullPointerException ex) {
      // bug 30716
      if (isDebugEnabled_STATISTICS) {
        logger.trace(LogMarker.STATISTICS_VERBOSE,
            "SampleCollector#getResourceType resourceTypeMap.get threw NPE, throwing NullPointerException");
      }
      throw new IgnoreResourceException();
    }
    if (resourceType == null) {
      resourceType = allocateResourceType(handlers, type);
      notifyOldHandlersOfResourceType(handlers, resourceType);
    }
    return resourceType;
  }

  private ResourceType allocateResourceType(List<MarkableSampleHandler> handlers,
      StatisticsType type) throws IgnoreResourceException {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#allocateResourceType type={}",
          type);
    }
    ResourceType resourceType = new ResourceType(this.resourceTypeId, type);
    this.resourceTypeMap.put(type, resourceType);
    this.resourceTypeId++;
    return resourceType;
  }

  private ResourceInstance allocateResourceInstance(ResourceType type, Statistics s) {
    if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#allocateResourceInstance type={}, s={}", type, s);
    }
    ResourceInstance resourceInstance = new ResourceInstance(this.resourceInstId, s, type);
    this.resourceInstMap.put(s, resourceInstance);
    this.resourceInstId++;
    return resourceInstance;
  }

  private List<ResourceInstance> cleanupResources(Statistics[] resources, int ignoreCount) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#cleanupResources resources.length={}, ignoreCount={}", resources.length,
          ignoreCount);
    }
    int resourcesToDelete = this.resourceInstMap.size() - (resources.length - ignoreCount);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#cleanupResources resourcesToDelete={}", resourcesToDelete);
    }
    if (resourcesToDelete == 0) {
      return Collections.<ResourceInstance>emptyList();
    }

    // some resource instances need to be removed
    List<ResourceInstance> resourcesRemoved = new ArrayList<ResourceInstance>();
    List<Statistics> resourceList = Arrays.asList(resources);
    Iterator<Map.Entry<Statistics, ResourceInstance>> it =
        this.resourceInstMap.entrySet().iterator();

    while (it.hasNext() && resourcesToDelete > 0) {
      Map.Entry<Statistics, ResourceInstance> e = it.next();
      Statistics key = (Statistics) e.getKey();
      if (!resourceList.contains(key)) {
        key.close();
        ResourceInstance inst = (ResourceInstance) e.getValue();
        resourcesRemoved.add(inst);
        resourcesToDelete--;
        it.remove();
      }
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#cleanupResources resourcesRemoved={}", resourcesRemoved);
    }
    return resourcesRemoved;
  }

  private void notifyAllHandlersOfSample(List<MarkableSampleHandler> handlers,
      List<ResourceInstance> updatedResources, long nanosTimeStamp) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyAllHandlersOfSample timeStamp={}", nanosTimeStamp);
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyAllHandlersOfSample updatedResources.size()={}, handlers={}",
          updatedResources.size(), handlers);
    }
    for (MarkableSampleHandler handler : handlers) {
      handler.sampled(nanosTimeStamp, Collections.unmodifiableList(updatedResources));
    }
  }

  private void notifyNewHandlersOfResources(List<MarkableSampleHandler> handlers,
      Collection<ResourceType> types, Collection<ResourceInstance> resources) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyNewHandlersOfResources ri.size()={}", resources.size());
    }
    int count = 0;
    for (MarkableSampleHandler handler : handlers) {
      if (!handler.isMarked()) {
        List<ResourceType> allocatedResourceTypes = new ArrayList<ResourceType>();
        for (ResourceInstance resourceInstance : resources) {
          ResourceType resourceType = resourceInstance.getResourceType();
          if (!allocatedResourceTypes.contains(resourceType)) {
            // allocatedResourceType...
            handler.allocatedResourceType(resourceType);
            allocatedResourceTypes.add(resourceType);
          }
          // allocatedResourceInstance...
          handler.allocatedResourceInstance(resourceInstance);
        }
        for (ResourceType resourceType : types) {
          if (!allocatedResourceTypes.contains(resourceType)) {
            handler.allocatedResourceType(resourceType);
          }
        }
        handler.mark();
        count++;
      }
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyNewHandlersOfResources notified {} new handlers", count);
    }
  }

  private void notifyOldHandlersOfResource(List<MarkableSampleHandler> handlers,
      ResourceInstance resource) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyOldHandlersOfResource resource={}", resource);
    }
    int count = 0;
    for (MarkableSampleHandler handler : handlers) {
      if (handler.isMarked()) {
        handler.allocatedResourceInstance(resource);
        count++;
      }
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyOldHandlersOfResource notified {} old handlers", count);
    }
  }

  private void notifyOldHandlersOfResourceType(List<MarkableSampleHandler> handlers,
      ResourceType type) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyOldHandlersOfResourceType type={}", type);
    }
    int count = 0;
    for (MarkableSampleHandler handler : handlers) {
      if (handler.isMarked()) {
        handler.allocatedResourceType(type);
        count++;
      }
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyOldHandlersOfResourceType notified {} old handlers", count);
    }
  }

  private void notifyOldHandlers(List<MarkableSampleHandler> handlers, List<ResourceInstance> ri) {
    final boolean isDebugEnabled_STATISTICS = logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE);
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE, "SampleCollector#notifyOldHandlers ri={}", ri);
    }
    int count = 0;
    for (ResourceInstance resource : ri) {
      for (MarkableSampleHandler handler : handlers) {
        if (handler.isMarked()) {
          handler.destroyedResourceInstance(resource);
          count++;
        }
      }
    }
    if (isDebugEnabled_STATISTICS) {
      logger.trace(LogMarker.STATISTICS_VERBOSE,
          "SampleCollector#notifyOldHandlers notified {} old handlers", count);
    }
  }

  /** For testing only */
  StatMonitorHandler getStatMonitorHandlerSnapshot() {
    synchronized (this.sampleHandlers) {
      return this.statMonitorHandler;
    }
  }

  /**
   * @since GemFire 7.0
   */
  public class MarkableSampleHandler implements SampleHandler {

    private final SampleHandler sampleHandler;
    private boolean mark = false;

    public MarkableSampleHandler(SampleHandler sampleHandler) {
      if (sampleHandler == null) {
        throw new NullPointerException("SampleHandler is null");
      }
      this.sampleHandler = sampleHandler;
    }

    public boolean isMarked() {
      if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
        logger.trace(LogMarker.STATISTICS_VERBOSE,
            "MarkableSampleHandler#isMarked returning {} for {}", this.mark, this);
      }
      return this.mark;
    }

    public void mark() {
      if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
        logger.trace(LogMarker.STATISTICS_VERBOSE, "MarkableSampleHandler#mark marking {}", this);
      }
      this.mark = true;
    }

    public SampleHandler getSampleHandler() {
      return this.sampleHandler;
    }

    @Override
    public void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances) {
      this.sampleHandler.sampled(nanosTimeStamp, resourceInstances);
    }

    @Override
    public void allocatedResourceType(ResourceType resourceType) {
      this.sampleHandler.allocatedResourceType(resourceType);
    }

    @Override
    public void allocatedResourceInstance(ResourceInstance resourceInstance) {
      this.sampleHandler.allocatedResourceInstance(resourceInstance);
    }

    @Override
    public void destroyedResourceInstance(ResourceInstance resourceInstance) {
      this.sampleHandler.destroyedResourceInstance(resourceInstance);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + this.sampleHandler.hashCode();
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
      MarkableSampleHandler other = (MarkableSampleHandler) obj;
      return this.sampleHandler == other.sampleHandler;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getName());
      sb.append("@").append(System.identityHashCode(this)).append("{");
      sb.append("mark=").append(this.mark);
      sb.append(", sampleHandler=").append(this.sampleHandler);
      return sb.toString();
    }
  }

  /**
   * @since GemFire 7.0
   */
  public class SampleHandlers implements Iterable<MarkableSampleHandler> {

    private volatile List<MarkableSampleHandler> currentHandlers =
        Collections.<MarkableSampleHandler>emptyList();

    public SampleHandlers() {}

    /** For test usage only. */
    public MarkableSampleHandler getMarkableSampleHandler(SampleHandler handler) {
      if (contains(handler)) {
        for (MarkableSampleHandler markableSamplerHandler : currentHandlers()) {
          if (markableSamplerHandler.sampleHandler == handler) {
            return markableSamplerHandler;
          }
        }
      }
      return null;
    }

    public boolean contains(SampleHandler handler) {
      MarkableSampleHandler markableHandler = new MarkableSampleHandler(handler);
      return this.currentHandlers.contains(markableHandler);
    }

    public List<MarkableSampleHandler> currentHandlers() {
      // optimized for read (no copy and unmodifiable)
      return this.currentHandlers;
    }

    @Override
    public Iterator<MarkableSampleHandler> iterator() {
      // optimized for read (no copy and unmodifiable)
      return this.currentHandlers.iterator();
    }

    public Iterator<MarkableSampleHandler> markedIterator() {
      return new MarkableIterator(true);
    }

    public Iterator<MarkableSampleHandler> unmarkedIterator() {
      return new MarkableIterator(false);
    }

    public boolean addSampleHandler(SampleHandler handler) {
      synchronized (this) {
        boolean added = false;
        MarkableSampleHandler markableHandler = new MarkableSampleHandler(handler);
        List<MarkableSampleHandler> oldHandlers = this.currentHandlers;
        if (!oldHandlers.contains(markableHandler)) {
          if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
            logger.trace(LogMarker.STATISTICS_VERBOSE,
                "SampleHandlers#addSampleHandler adding markableHandler to {}", this);
          }
          List<MarkableSampleHandler> newHandlers =
              new ArrayList<MarkableSampleHandler>(oldHandlers);
          added = newHandlers.add(markableHandler);
          this.currentHandlers = Collections.unmodifiableList(newHandlers);
        }
        return added;
      }
    }

    public boolean removeSampleHandler(SampleHandler handler) {
      synchronized (this) {
        boolean removed = false;
        MarkableSampleHandler markableHandler = new MarkableSampleHandler(handler);
        List<MarkableSampleHandler> oldHandlers = this.currentHandlers;
        if (oldHandlers.contains(markableHandler)) {
          if (logger.isTraceEnabled(LogMarker.STATISTICS_VERBOSE)) {
            logger.trace(LogMarker.STATISTICS_VERBOSE,
                "SampleHandlers#removeSampleHandler removing markableHandler from {}", this);
          }
          List<MarkableSampleHandler> newHandlers =
              new ArrayList<MarkableSampleHandler>(oldHandlers);
          removed = newHandlers.remove(markableHandler);
          this.currentHandlers = Collections.unmodifiableList(newHandlers);
        }
        return removed;
      }
    }

    protected int countMarkableSampleHandlers(SampleHandler handler) {
      int count = 0;
      List<MarkableSampleHandler> list = currentHandlers();
      for (MarkableSampleHandler wrapper : list) {
        if (wrapper.getSampleHandler() == handler) {
          count++;
        }
      }
      return count;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getName());
      sb.append("@").append(System.identityHashCode(this)).append("{");
      sb.append("currentHandlers=").append(this.currentHandlers);
      return sb.toString();
    }

    private class MarkableIterator implements Iterator<MarkableSampleHandler> {

      private final Iterator<MarkableSampleHandler> iterator;

      public MarkableIterator(boolean marked) {
        List<MarkableSampleHandler> matchingHandlers = new ArrayList<MarkableSampleHandler>();
        List<MarkableSampleHandler> handlers = SampleHandlers.this.currentHandlers();
        for (MarkableSampleHandler handler : handlers) {
          if (handler.isMarked() == marked) {
            matchingHandlers.add(handler);
          }
        }
        this.iterator = matchingHandlers.iterator();
      }

      @Override
      public boolean hasNext() {
        return this.iterator.hasNext();
      }

      @Override
      public MarkableSampleHandler next() {
        return this.iterator.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove operation is not supported");
      }
    }
  }
}
