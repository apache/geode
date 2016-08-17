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
package com.gemstone.gemfire.internal.cache.wan;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.statistics.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.CachePerfStats;

public class GatewaySenderStats {


  public static final String typeName = "GatewaySenderStatistics";


   /** The <code>StatisticsType</code> of the statistics */
   private static final StatisticsType type;

   ////////////////////  Statistic "Id" Fields  ////////////////////

   /** Name of the events received statistic */
   protected static final String EVENTS_RECEIVED = "eventsReceived";
   /** Name of the events queued statistic */
   protected static final String EVENTS_QUEUED = "eventsQueued";
   /** Name of the events not queued because conflated statistic */
   protected static final String EVENTS_NOT_QUEUED_CONFLATED = "eventsNotQueuedConflated";
   /** Name of the events conflated from the batch statistic */ 
   protected static final String EVENTS_CONFLATED_FROM_BATCHES = "eventsConflatedFromBatches"; 
   /** Name of the event queue time statistic */
   protected static final String EVENT_QUEUE_TIME = "eventQueueTime";
   /** Name of the event queue size statistic */
   protected static final String EVENT_QUEUE_SIZE = "eventQueueSize";
   /** Name of the event temporary queue size statistic */
   protected static final String TMP_EVENT_QUEUE_SIZE = "tempQueueSize";
   /** Name of the events distributed statistic */
   protected static final String EVENTS_DISTRIBUTED = "eventsDistributed";
   /** Name of the events exceeding alert threshold statistic */
   protected static final String EVENTS_EXCEEDING_ALERT_THRESHOLD = "eventsExceedingAlertThreshold";
   /** Name of the batch distribution time statistic */
   protected static final String BATCH_DISTRIBUTION_TIME = "batchDistributionTime";
   /** Name of the batches distributed statistic */
   protected static final String BATCHES_DISTRIBUTED = "batchesDistributed";
   /** Name of the batches redistributed statistic */
   protected static final String BATCHES_REDISTRIBUTED = "batchesRedistributed";
  /** Name of the batches resized statistic */
  protected static final String BATCHES_RESIZED = "batchesResized";
   /** Name of the unprocessed events added by primary statistic */
   protected static final String UNPROCESSED_TOKENS_ADDED_BY_PRIMARY = "unprocessedTokensAddedByPrimary";
   /** Name of the unprocessed events added by secondary statistic */
   protected static final String UNPROCESSED_EVENTS_ADDED_BY_SECONDARY = "unprocessedEventsAddedBySecondary";
   /** Name of the unprocessed events removed by primary statistic */
   protected static final String UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY = "unprocessedEventsRemovedByPrimary";
   /** Name of the unprocessed events removed by secondary statistic */
   protected static final String UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY = "unprocessedTokensRemovedBySecondary";
   protected static final String UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT = "unprocessedEventsRemovedByTimeout";
   protected static final String UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT = "unprocessedTokensRemovedByTimeout";
   /** Name of the unprocessed events map size statistic */
   protected static final String UNPROCESSED_EVENT_MAP_SIZE = "unprocessedEventMapSize";
   protected static final String UNPROCESSED_TOKEN_MAP_SIZE = "unprocessedTokenMapSize";
   
   protected static final String CONFLATION_INDEXES_MAP_SIZE = "conflationIndexesSize";
   
   protected static final String EVENTS_FILTERED = "eventsFiltered";
   protected static final String NOT_QUEUED_EVENTS = "notQueuedEvent";

   protected static final String LOAD_BALANCES_COMPLETED = "loadBalancesCompleted";
   protected static final String LOAD_BALANCES_IN_PROGRESS = "loadBalancesInProgress";
   protected static final String LOAD_BALANCE_TIME = "loadBalanceTime";

   /** Id of the events queued statistic */
   protected static  int eventsReceivedId;
   /** Id of the events queued statistic */
   protected static  int eventsQueuedId;
   /** Id of the events not queued because conflated statistic */
   protected static  int eventsNotQueuedConflatedId;
   /** Id of the event queue time statistic */
   protected static  int eventQueueTimeId;
   /** Id of the event queue size statistic */
   protected static  int eventQueueSizeId;
   /** Id of the temp event queue size statistic */
   protected static  int eventTmpQueueSizeId;
   /** Id of the events distributed statistic */
   protected static  int eventsDistributedId;
   /** Id of the events exceeding alert threshold statistic */
   protected static  int eventsExceedingAlertThresholdId;
   /** Id of the batch distribution time statistic */
   protected static  int batchDistributionTimeId;
   /** Id of the batches distributed statistic */
   protected static  int batchesDistributedId;
   /** Id of the batches redistributed statistic */
   protected static  int batchesRedistributedId;
  /** Id of the batches resized statistic */
  protected static  int batchesResizedId;
   /** Id of the unprocessed events added by primary statistic */
   protected static  int unprocessedTokensAddedByPrimaryId;
   /** Id of the unprocessed events added by secondary statistic */
   protected static  int unprocessedEventsAddedBySecondaryId;
   /** Id of the unprocessed events removed by primary statistic */
   protected static  int unprocessedEventsRemovedByPrimaryId;
   /** Id of the unprocessed events removed by secondary statistic */
   protected static  int unprocessedTokensRemovedBySecondaryId;
   protected static  int unprocessedEventsRemovedByTimeoutId;
   protected static  int unprocessedTokensRemovedByTimeoutId;
   /** Id of the unprocessed events map size statistic */
   protected static  int unprocessedEventMapSizeId;
   protected static  int unprocessedTokenMapSizeId;
   /** Id of the conflation indexes size statistic */
   protected static  int conflationIndexesMapSizeId;
   /** Id of filtered events*/
   protected static  int eventsFilteredId;
   /** Id of not queued events*/
   protected static  int notQueuedEventsId;
   /**Id of events conflated in batch*/
   protected static  int eventsConflatedFromBatchesId; 
   /** Id of load balances completed*/
   protected static int loadBalancesCompletedId;
   /** Id of load balances in progress*/
   protected static int loadBalancesInProgressId;
   /** Id of load balance time*/
   protected static int loadBalanceTimeId;

   /**
    * Static initializer to create and initialize the <code>StatisticsType</code>
    */
   static {

     StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

     type = f.createType(typeName, "Stats for activity in the GatewaySender",
        new StatisticDescriptor[] {
         f.createIntCounter
         (EVENTS_RECEIVED,
          "Number of events received by this Sender.",
          "operations"),
         f.createIntCounter
         (EVENTS_QUEUED,
          "Number of events added to the event queue.",
          "operations"),
         f.createLongCounter
          (EVENT_QUEUE_TIME,
           "Total time spent queueing events.",
           "nanoseconds"),
         f.createIntGauge
          (EVENT_QUEUE_SIZE,
           "Size of the event queue.",
           "operations", false),
           f.createIntGauge
           (TMP_EVENT_QUEUE_SIZE,
            "Size of the temporary events.",
            "operations", false),
         f.createIntCounter
          (EVENTS_NOT_QUEUED_CONFLATED,
           "Number of events received but not added to the event queue because the queue already contains an event with the event's key.",
           "operations"),
         f.createIntCounter 
          (EVENTS_CONFLATED_FROM_BATCHES, 
           "Number of events conflated from batches.", 
           "operations"), 
         f.createIntCounter
          (EVENTS_DISTRIBUTED,
           "Number of events removed from the event queue and sent.",
           "operations"),
         f.createIntCounter
          (EVENTS_EXCEEDING_ALERT_THRESHOLD,
           "Number of events exceeding the alert threshold.",
           "operations", false),
         f.createLongCounter
          (BATCH_DISTRIBUTION_TIME,
           "Total time spent distributing batches of events to other gateway receivers.",
           "nanoseconds"),
         f.createIntCounter
          (BATCHES_DISTRIBUTED,
           "Number of batches of events removed from the event queue and sent.",
           "operations"),
         f.createIntCounter
          (BATCHES_REDISTRIBUTED,
           "Number of batches of events removed from the event queue and resent.",
           "operations", false),
         f.createIntCounter
          (BATCHES_RESIZED,
           "Number of batches that were resized because they were too large",
           "operations", false),
         f.createIntCounter
          (UNPROCESSED_TOKENS_ADDED_BY_PRIMARY,
           "Number of tokens added to the secondary's unprocessed token map by the primary (though a listener).",
           "tokens"),
         f.createIntCounter
          (UNPROCESSED_EVENTS_ADDED_BY_SECONDARY,
           "Number of events added to the secondary's unprocessed event map by the secondary.",
           "events"),
         f.createIntCounter
          (UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY,
           "Number of events removed from the secondary's unprocessed event map by the primary (though a listener).",
           "events"),
         f.createIntCounter
          (UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY,
           "Number of tokens removed from the secondary's unprocessed token map by the secondary.",
           "tokens"),
         f.createIntCounter
          (UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT,
           "Number of events removed from the secondary's unprocessed event map by a timeout.",
           "events"),
         f.createIntCounter
          (UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT,
           "Number of tokens removed from the secondary's unprocessed token map by a timeout.",
           "tokens"),
         f.createIntGauge
          (UNPROCESSED_EVENT_MAP_SIZE,
           "Current number of entries in the secondary's unprocessed event map.",
           "events", false),
         f.createIntGauge
          (UNPROCESSED_TOKEN_MAP_SIZE,
           "Current number of entries in the secondary's unprocessed token map.",
           "tokens", false),
         f.createIntGauge
          (CONFLATION_INDEXES_MAP_SIZE,
           "Current number of entries in the conflation indexes map.",
           "events"),
         f.createIntCounter
           (NOT_QUEUED_EVENTS,
            "Number of events not added to queue.",
            "events"),
          f.createIntCounter
            (EVENTS_FILTERED,
             "Number of events filtered through GatewayEventFilter.",
             "events"),
          f.createIntCounter
            (LOAD_BALANCES_COMPLETED,
             "Number of load balances completed",
             "operations"),
          f.createIntGauge
            (LOAD_BALANCES_IN_PROGRESS,
             "Number of load balances in progress",
             "operations"),
          f.createLongCounter
            (LOAD_BALANCE_TIME,
             "Total time spent load balancing this sender",
             "nanoseconds"),
     });

     // Initialize id fields
     eventsReceivedId = type.nameToId(EVENTS_RECEIVED);
     eventsQueuedId = type.nameToId(EVENTS_QUEUED);
     eventsNotQueuedConflatedId = type.nameToId(EVENTS_NOT_QUEUED_CONFLATED);
     eventQueueTimeId = type.nameToId(EVENT_QUEUE_TIME);
     eventQueueSizeId = type.nameToId(EVENT_QUEUE_SIZE);
     eventTmpQueueSizeId = type.nameToId(TMP_EVENT_QUEUE_SIZE);
     eventsDistributedId = type.nameToId(EVENTS_DISTRIBUTED);
     eventsExceedingAlertThresholdId = type.nameToId(EVENTS_EXCEEDING_ALERT_THRESHOLD);
     batchDistributionTimeId = type.nameToId(BATCH_DISTRIBUTION_TIME);
     batchesDistributedId = type.nameToId(BATCHES_DISTRIBUTED);
     batchesRedistributedId = type.nameToId(BATCHES_REDISTRIBUTED);
     batchesResizedId = type.nameToId(BATCHES_RESIZED);
     unprocessedTokensAddedByPrimaryId = type.nameToId(UNPROCESSED_TOKENS_ADDED_BY_PRIMARY);
     unprocessedEventsAddedBySecondaryId = type.nameToId(UNPROCESSED_EVENTS_ADDED_BY_SECONDARY);
     unprocessedEventsRemovedByPrimaryId = type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY);
     unprocessedTokensRemovedBySecondaryId = type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY);
     unprocessedEventsRemovedByTimeoutId = type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT);
     unprocessedTokensRemovedByTimeoutId = type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT);
     unprocessedEventMapSizeId = type.nameToId(UNPROCESSED_EVENT_MAP_SIZE);
     unprocessedTokenMapSizeId = type.nameToId(UNPROCESSED_TOKEN_MAP_SIZE); 
     conflationIndexesMapSizeId = type.nameToId(CONFLATION_INDEXES_MAP_SIZE); 
     notQueuedEventsId = type.nameToId(NOT_QUEUED_EVENTS);
     eventsFilteredId = type.nameToId(EVENTS_FILTERED);
     eventsConflatedFromBatchesId = type.nameToId(EVENTS_CONFLATED_FROM_BATCHES);
     loadBalancesCompletedId = type.nameToId(LOAD_BALANCES_COMPLETED);
     loadBalancesInProgressId = type.nameToId(LOAD_BALANCES_IN_PROGRESS);
     loadBalanceTimeId = type.nameToId(LOAD_BALANCE_TIME);
   }

   //////////////////////  Instance Fields  //////////////////////

   /** The <code>Statistics</code> instance to which most behavior is delegated */
   private final Statistics stats;

   ///////////////////////  Constructors  ///////////////////////

   /**
    * Constructor.
    *
    * @param f The <code>StatisticsFactory</code> which creates the
    * <code>Statistics</code> instance
    * @param gatewaySenderId The id of the <code>GatewaySender</code> used to
    * generate the name of the <code>Statistics</code>
    */
   public GatewaySenderStats(StatisticsFactory f, String gatewaySenderId) {
     this.stats = f.createAtomicStatistics(type, "gatewaySenderStats-"+gatewaySenderId);
   }
   
   /**
    * Constructor.
    *
    * @param f The <code>StatisticsFactory</code> which creates the
    * <code>Statistics</code> instance
    * @param asyncQueueId The id of the <code>AsyncEventQueue</code> used to
    * generate the name of the <code>Statistics</code>
    * @param    statType   The StatisticsTYpe 
    */
   public GatewaySenderStats(StatisticsFactory f, String asyncQueueId, StatisticsType statType) {
     this.stats = f.createAtomicStatistics(statType, "asyncEventQueueStats-" + asyncQueueId);
   }

   /////////////////////  Instance Methods  /////////////////////

   /**
    * Closes the <code>GatewaySenderStats</code>.
    */
   public void close() {
     this.stats.close();
   }

   /**
    * Returns the current value of the "eventsReceived" stat.
    * @return the current value of the "eventsReceived" stat
    */
   public int getEventsReceived() {
     return this.stats.getInt(eventsReceivedId);
   }
   
   /**
    * Increments the number of events received by 1.
    */
   public void incEventsReceived() {
     this.stats.incInt(eventsReceivedId, 1);
   }
   
   /**
    * Returns the current value of the "eventsQueued" stat.
    * @return the current value of the "eventsQueued" stat
    */
   public int getEventsQueued() {
     return this.stats.getInt(eventsQueuedId);
   }

   /**
    * Returns the current value of the "eventsNotQueuedConflated" stat.
    * @return the current value of the "eventsNotQueuedConflated" stat
    */
   public int getEventsNotQueuedConflated() {
     return this.stats.getInt(eventsNotQueuedConflatedId);
   }

  /**
   * Returns the current value of the "eventsConflatedFromBatches" stat.
   * 
   * @return the current value of the "eventsConflatedFromBatches" stat
   */
  public int getEventsConflatedFromBatches() {
    return this.stats.getInt(eventsConflatedFromBatchesId);
  }
   
   /**
    * Returns the current value of the "eventQueueSize" stat.
    * @return the current value of the "eventQueueSize" stat
    */
   public int getEventQueueSize() {
     return this.stats.getInt(eventQueueSizeId);
   }
   
   /**
    * Returns the current value of the "tempQueueSize" stat.
    * @return the current value of the "tempQueueSize" stat.
    */
   public int getTempEventQueueSize() {
     return this.stats.getInt(eventTmpQueueSizeId);
   }


  /** Returns the internal ID for {@link #getEventQueueSize()} statistic */
  public static String getEventQueueSizeId() {
    return EVENT_QUEUE_SIZE;
  }
  
  /** Returns the internal ID for {@link #getTempEventQueueSize()} statistic */
  public static String getEventTempQueueSizeId() {
    return TMP_EVENT_QUEUE_SIZE;
  }

   /**
    * Returns the current value of the "eventsDistributed" stat.
    * @return the current value of the "eventsDistributed" stat
    */
   public int getEventsDistributed() {
     return this.stats.getInt(eventsDistributedId);
   }

   /**
    * Returns the current value of the "eventsExceedingAlertThreshold" stat.
    * @return the current value of the "eventsExceedingAlertThreshold" stat
    */
   public int getEventsExceedingAlertThreshold() {
     return this.stats.getInt(eventsExceedingAlertThresholdId);
   }

   /**
    * Increments the value of the "eventsExceedingAlertThreshold" stat by 1.
    */
   public void incEventsExceedingAlertThreshold() {
     this.stats.incInt(eventsExceedingAlertThresholdId, 1);
   }

   /**
    * Returns the current value of the "batchDistributionTime" stat.
    * @return the current value of the "batchDistributionTime" stat
    */
   public long getBatchDistributionTime() {
     return this.stats.getLong(batchDistributionTimeId);
   }

   /**
    * Returns the current value of the batchesDistributed" stat.
    * @return the current value of the batchesDistributed" stat
    */
   public int getBatchesDistributed() {
     return this.stats.getInt(batchesDistributedId);
   }

   /**
    * Returns the current value of the batchesRedistributed" stat.
    * @return the current value of the batchesRedistributed" stat
    */
   public int getBatchesRedistributed() {
     return this.stats.getInt(batchesRedistributedId);
   }

   /**
    * Returns the current value of the batchesResized" stat.
    * @return the current value of the batchesResized" stat
    */
   public int getBatchesResized() {
     return this.stats.getInt(batchesResizedId);
   }

   /**
    * Increments the value of the "batchesRedistributed" stat by 1.
    */
   public void incBatchesRedistributed() {
     this.stats.incInt(batchesRedistributedId, 1);
   }

   /**
    * Increments the value of the "batchesRedistributed" stat by 1.
    */
   public void incBatchesResized() {
     this.stats.incInt(batchesResizedId, 1);
   }

   /**
    * Sets the "eventQueueSize" stat.
    * @param size The size of the queue
    */
   public void setQueueSize(int size)
   {
     this.stats.setInt(eventQueueSizeId, size);
   }
   
   /**
    * Sets the "tempQueueSize" stat.
    * @param size The size of the temp queue
    */
   public void setTempQueueSize(int size)
   {
     this.stats.setInt(eventTmpQueueSizeId, size);
   }


   /**
    * Increments the "eventQueueSize" stat by 1.
    */
   public void incQueueSize()
   {
     this.stats.incInt(eventQueueSizeId, 1);
   }

   /**
    * Increments the "tempQueueSize" stat by 1.
    */
   public void incTempQueueSize()
   {
     this.stats.incInt(eventTmpQueueSizeId, 1);
   }
   
   /**
    * Increments the "eventQueueSize" stat by given delta.
    * @param delta an integer by which queue size to be increased
    */
   public void incQueueSize(int delta)
   {
     this.stats.incInt(eventQueueSizeId, delta);
   }
   /**
    * Increments the "tempQueueSize" stat by given delta.
    * @param delta an integer by which temp queue size to be increased
    */
   public void incTempQueueSize(int delta)
   {
     this.stats.incInt(eventTmpQueueSizeId, delta);
   }

   /**
    * Decrements the "eventQueueSize" stat by 1.
    */
   public void decQueueSize()
   {
     this.stats.incInt(eventQueueSizeId, -1);
   }
   /**
    * Decrements the "tempQueueSize" stat by 1.
    */
   public void decTempQueueSize()
   {
     this.stats.incInt(eventTmpQueueSizeId, -1);
   }

   /**
    * Decrements the "eventQueueSize" stat by given delta.
    * @param delta an integer by which queue size to be increased
    */
   public void decQueueSize(int delta)
   {
     this.stats.incInt(eventQueueSizeId, -delta);
   }
   /**
    * Decrements the "tempQueueSize" stat by given delta.
    * @param delta an integer by which temp queue size to be increased
    */
   public void decTempQueueSize(int delta)
   {
     this.stats.incInt(eventTmpQueueSizeId, -delta);
   }
   
   /**
    * Increments the "eventsNotQueuedConflated" stat.
    */
   public void incEventsNotQueuedConflated()
   {
     this.stats.incInt(eventsNotQueuedConflatedId, 1);
   }

  /**
   * Increments the "eventsConflatedFromBatches" stat.
   */
  public void incEventsConflatedFromBatches(int numEvents) {
    this.stats.incInt(eventsConflatedFromBatchesId, numEvents);
  } 
   
   
   /**
    * Returns the current value of the "unprocessedTokensAddedByPrimary" stat.
    * @return the current value of the "unprocessedTokensAddedByPrimary" stat
    */
   public int getUnprocessedTokensAddedByPrimary() {
     return this.stats.getInt(unprocessedTokensAddedByPrimaryId);
   }

   /**
    * Returns the current value of the "unprocessedEventsAddedBySecondary" stat.
    * @return the current value of the "unprocessedEventsAddedBySecondary" stat
    */
   public int getUnprocessedEventsAddedBySecondary() {
     return this.stats.getInt(unprocessedEventsAddedBySecondaryId);
   }

   /**
    * Returns the current value of the "unprocessedEventsRemovedByPrimary" stat.
    * @return the current value of the "unprocessedEventsRemovedByPrimary" stat
    */
   public int getUnprocessedEventsRemovedByPrimary() {
     return this.stats.getInt(unprocessedEventsRemovedByPrimaryId);
   }

   /**
    * Returns the current value of the "unprocessedTokensRemovedBySecondary" stat.
    * @return the current value of the "unprocessedTokensRemovedBySecondary" stat
    */
   public int getUnprocessedTokensRemovedBySecondary() {
     return this.stats.getInt(unprocessedTokensRemovedBySecondaryId);
   }

   /**
    * Returns the current value of the "unprocessedEventMapSize" stat.
    * @return the current value of the "unprocessedEventMapSize" stat
    */
   public int getUnprocessedEventMapSize() {
     return this.stats.getInt(unprocessedEventMapSizeId);
   }
   public int getUnprocessedTokenMapSize() {
     return this.stats.getInt(unprocessedTokenMapSizeId);
   }

   public void incEventsNotQueued() {
     this.stats.incInt(notQueuedEventsId, 1);
   }

   public int getEventsNotQueued() {
     return this.stats.getInt(notQueuedEventsId);
   }
   
   public void incEventsFiltered() {
     this.stats.incInt(eventsFilteredId, 1);
   }

   public int getEventsFiltered() {
     return this.stats.getInt(eventsFilteredId);
   }
   /**
    * Increments the value of the "unprocessedTokensAddedByPrimary" stat by 1.
    */
   public void incUnprocessedTokensAddedByPrimary() {
     this.stats.incInt(unprocessedTokensAddedByPrimaryId, 1);
     incUnprocessedTokenMapSize();
   }

   /**
    * Increments the value of the "unprocessedEventsAddedBySecondary" stat by 1.
    */
   public void incUnprocessedEventsAddedBySecondary() {
     this.stats.incInt(unprocessedEventsAddedBySecondaryId, 1);
     incUnprocessedEventMapSize();
   }

   /**
    * Increments the value of the "unprocessedEventsRemovedByPrimary" stat by 1.
    */
   public void incUnprocessedEventsRemovedByPrimary() {
     this.stats.incInt(unprocessedEventsRemovedByPrimaryId, 1);
     decUnprocessedEventMapSize();
   }

   /**
    * Increments the value of the "unprocessedTokensRemovedBySecondary" stat by 1.
    */
   public void incUnprocessedTokensRemovedBySecondary() {
     this.stats.incInt(unprocessedTokensRemovedBySecondaryId, 1);
     decUnprocessedTokenMapSize();
   }
   public void incUnprocessedEventsRemovedByTimeout(int count) {
     this.stats.incInt(unprocessedEventsRemovedByTimeoutId, count);
     decUnprocessedEventMapSize(count);
   }
   public void incUnprocessedTokensRemovedByTimeout(int count) {
     this.stats.incInt(unprocessedTokensRemovedByTimeoutId, count);
     decUnprocessedTokenMapSize(count);
   }

   /**
    * Sets the "unprocessedEventMapSize" stat.
    */
   public void clearUnprocessedMaps() {
     this.stats.setInt(unprocessedEventMapSizeId, 0);
     this.stats.setInt(unprocessedTokenMapSizeId, 0);
   }
   private void incUnprocessedEventMapSize() {
     this.stats.incInt(unprocessedEventMapSizeId, 1);
   }
   private void decUnprocessedEventMapSize() {
     this.stats.incInt(unprocessedEventMapSizeId, -1);
   }
   private void decUnprocessedEventMapSize(int decCount) {
     this.stats.incInt(unprocessedEventMapSizeId, -decCount);
   }
   private void incUnprocessedTokenMapSize() {
     this.stats.incInt(unprocessedTokenMapSizeId, 1);
   }
   private void decUnprocessedTokenMapSize() {
     this.stats.incInt(unprocessedTokenMapSizeId, -1);
   }
   private void decUnprocessedTokenMapSize(int decCount) {
     this.stats.incInt(unprocessedTokenMapSizeId, -decCount);
   }
    
   /**
    * Increments the value of the "conflationIndexesMapSize" stat by 1
    */
   public void incConflationIndexesMapSize() {
     this.stats.incInt(conflationIndexesMapSizeId, 1);
   }
    
   /**
    * Decrements the value of the "conflationIndexesMapSize" stat by 1
    */
   public void decConflationIndexesMapSize() {
     this.stats.incInt(conflationIndexesMapSizeId, -1);
   }
    
   /**
    * Returns the current time (ns).
    * @return the current time (ns)
    */
   public long startTime()
   {
     return DistributionStats.getStatTime();
   }

   /**
    * Increments the "eventsDistributed" and "batchDistributionTime" stats.
    * @param start The start of the batch (which is decremented from the current
    * time to determine the batch processing time).
    * @param numberOfEvents The number of events to add to the events
    * distributed stat
    */
   public void endBatch(long start, int numberOfEvents)
   {
     long ts = DistributionStats.getStatTime();

     // Increment number of batches distributed
     this.stats.incInt(batchesDistributedId, 1);
     
     // Increment number of events distributed
     this.stats.incInt(eventsDistributedId, numberOfEvents);

     // Increment batch distribution time
     long elapsed = ts-start;
     this.stats.incLong(batchDistributionTimeId, elapsed);
   }

   /**
    * Increments the "eventsQueued" and "eventQueueTime" stats.
    * @param start The start of the put (which is decremented from the current
    * time to determine the queue processing time).
    */
   public void endPut(long start)
   {
     long ts = DistributionStats.getStatTime();

     // Increment number of event queued
     this.stats.incInt(eventsQueuedId, 1);

     // Increment event queue time
     long elapsed = ts-start;
     this.stats.incLong(eventQueueTimeId, elapsed);
   }

   public long startLoadBalance() {
     stats.incInt(loadBalancesInProgressId, 1);
     return CachePerfStats.getStatTime();
   }

   public void endLoadBalance(long start) {
     long delta = CachePerfStats.getStatTime() - start;
     stats.incInt(loadBalancesInProgressId, -1);
     stats.incInt(loadBalancesCompletedId, 1);
     stats.incLong(loadBalanceTimeId, delta);
   }

   public Statistics getStats(){
     return stats;
   }
}
