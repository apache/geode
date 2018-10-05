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
package org.apache.geode.internal.cache.ha;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.logging.LogService;

/**
 * This message is sent to all the nodes in the DistributedSystem. It contains the list of messages
 * that have been dispatched by this node. The messages are received by other nodes and the
 * processing is handed over to an executor
 */
public class QueueRemovalMessage extends PooledDistributionMessage {
  private static final Logger logger = LogService.getLogger();

  /**
   * List of messages (String[] )
   */
  private List messagesList;

  /**
   * Constructor : Set the recipient list to ALL_RECIPIENTS
   */
  public QueueRemovalMessage() {
    this.setRecipient(ALL_RECIPIENTS);
  }

  /**
   * Set the message list
   */
  public void setMessagesList(List messages) {
    this.messagesList = messages;
  }

  /**
   * Extracts the region from the message list and hands over the message removal task to the
   * executor
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    final InternalCache cache = dm.getCache();
    if (cache != null) {
      Iterator iterator = this.messagesList.iterator();
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        while (iterator.hasNext()) {
          final String regionName = (String) iterator.next();
          final int size = (Integer) iterator.next();
          final LocalRegion region = (LocalRegion) cache.getRegion(regionName);
          final HARegionQueue hrq;
          if (region == null || !region.isInitialized()) {
            hrq = null;
          } else {
            HARegionQueue tmp = ((HARegion) region).getOwner();
            if (tmp != null && tmp.isQueueInitialized()) {
              hrq = tmp;
            } else {
              hrq = null;
            }
          }
          // we have to iterate even if the hrq isn't available since there are
          // a bunch of event IDs to go through
          for (int i = 0; i < size; i++) {
            final EventID id = (EventID) iterator.next();
            boolean interrupted = Thread.interrupted();
            if (hrq == null || !hrq.isQueueInitialized()) {
              continue;
            }
            try {
              // Fix for bug 39516: inline removal of events by QRM.
              // dm.getWaitingThreadPool().execute(new Runnable() {
              // public void run()
              // {
              try {
                if (logger.isTraceEnabled()) {
                  logger.trace("QueueRemovalMessage: removing dispatched events on queue {} for {}",
                      regionName, id);
                }
                hrq.removeDispatchedEvents(id);
              } catch (RegionDestroyedException ignore) {
                logger.info(
                    "Queue found destroyed while processing the last disptached sequence ID for a HARegionQueue's DACE. The event ID is {} for HARegion with name={}",
                    new Object[] {id, regionName});
              } catch (CancelException ignore) {
                return; // cache or DS is closing
              } catch (CacheException e) {
                logger.error(String.format(
                    "QueueRemovalMessage::process:Exception in processing the last disptached sequence ID for a HARegionQueue's DACE. The problem is with event ID,%s for HARegion with name=%s",
                    new Object[] {regionName, id}),
                    e);
              } catch (InterruptedException ignore) {
                return; // interrupt occurs during shutdown. this runs in an executor, so just stop
                        // processing
              }
            } catch (RejectedExecutionException ignore) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // if
        } // for
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    } // cache != null
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    /*
     * first write the total list size then in a loop write the region name, number of eventIds and
     * the event ids
     */
    super.toData(out);
    // write the size of the data list
    DataSerializer.writeInteger(this.messagesList.size(), out);
    Iterator iterator = messagesList.iterator();
    String regionName = null;
    Integer numberOfIds = null;
    Object eventId = null;
    int maxVal;
    while (iterator.hasNext()) {
      regionName = (String) iterator.next();
      // write the regionName
      DataSerializer.writeString(regionName, out);
      numberOfIds = (Integer) iterator.next();
      // write the number of event ids
      DataSerializer.writeInteger(numberOfIds, out);
      maxVal = numberOfIds;
      // write the event ids
      for (int i = 0; i < maxVal; i++) {
        eventId = iterator.next();
        DataSerializer.writeObject(eventId, out);
      }
    }
  }

  public int getDSFID() {
    return QUEUE_REMOVAL_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    /*
     * read the total list size, reconstruct the message list in a loop by reading the region name,
     * number of eventIds and the event ids
     */
    super.fromData(in);
    // read the size of the message
    int size = DataSerializer.readInteger(in);
    this.messagesList = new LinkedList();
    int eventIdSizeInt;
    for (int i = 0; i < size; i++) {
      // read the region name
      this.messagesList.add(DataSerializer.readString(in));
      // read the datasize
      Integer eventIdSize = DataSerializer.readInteger(in);
      this.messagesList.add(eventIdSize);
      eventIdSizeInt = eventIdSize;
      // read the total number of events
      for (int j = 0; j < eventIdSizeInt; j++) {
        this.messagesList.add(DataSerializer.readObject(in));
      }
      // increment i by adding the total number of ids read and 1 for
      // the length of the message
      //
      i = i + eventIdSizeInt + 1;
    }
  }

  @Override
  public String toString() {
    return "QueueRemovalMessage" + this.messagesList;
  }
}
