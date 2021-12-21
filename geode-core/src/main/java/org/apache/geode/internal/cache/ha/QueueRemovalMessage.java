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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

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
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

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
  private List<Object> messagesList;

  /**
   * Constructor : Set the recipient list to ALL_RECIPIENTS
   */
  public QueueRemovalMessage() {
    setRecipient(ALL_RECIPIENTS);
  }

  /**
   * Set the message list
   */
  void setMessagesList(List messages) {
    messagesList = uncheckedCast(messages);
  }

  /**
   * Extracts the region from the message list and hands over the message removal task to the
   * executor
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    final InternalCache cache = dm.getCache();
    if (cache != null) {
      Iterator iterator = messagesList.iterator();
      processRegionQueues(cache, iterator);
    }
  }

  void processRegionQueues(InternalCache cache, Iterator iterator) {
    while (iterator.hasNext()) {
      final String regionName = (String) iterator.next();
      final int size = (Integer) iterator.next();
      final LocalRegion region = (LocalRegion) cache.getRegion(regionName);
      final HARegionQueue hrq;
      if (region == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("processing QRM region {} does not exist.", regionName);
        }
        hrq = null;
      } else {
        long maxWaitTimeForInitialization = 30000;
        hrq = ((HARegion) region).getOwnerWithWait(maxWaitTimeForInitialization);
      }
      boolean succeed = processRegionQueue(iterator, regionName, size, hrq);
      if (!succeed) {
        return;
      } else {
        if (hrq != null) {
          hrq.synchronizeQueueWithPrimary(getSender(), cache);
        }
      }
    }
  }

  boolean processRegionQueue(Iterator iterator, String regionName, int size,
      HARegionQueue hrq) {
    // we have to iterate even if the hrq isn't available since there are
    // a bunch of event IDs to go through
    for (int i = 0; i < size; i++) {
      final EventID id = (EventID) iterator.next();
      if (hrq == null || !hrq.isQueueInitialized()) {
        if (logger.isDebugEnabled()) {
          logger.debug("QueueRemovalMessage: hrq is not ready when trying to remove "
              + "dispatched event on queue {} for {}", regionName, id);
        }
        continue;
      }

      if (!removeQueueEvent(regionName, hrq, id)) {
        return false;
      }
    }
    return true;
  }

  boolean removeQueueEvent(String regionName, HARegionQueue hrq, EventID id) {
    // Fix for bug 39516: inline removal of events by QRM.
    // dm.getWaitingThreadPool().execute(new Runnable() {
    // public void run()
    // {
    boolean interrupted = Thread.interrupted();
    try {
      if (logger.isTraceEnabled()) {
        logger.trace("QueueRemovalMessage: removing dispatched events on queue {} for {}",
            regionName, id);
      }
      hrq.removeDispatchedEvents(id);
    } catch (RegionDestroyedException ignore) {
      logger.info(
          "Queue found destroyed while processing the last dispatched sequence ID for a HARegionQueue's DACE. The event ID is {} for HARegion with name={}",
          new Object[] {id, regionName});
    } catch (CancelException ignore) {
      return false;
    } catch (CacheException e) {
      logger.error(String.format(
          "QueueRemovalMessage::process:Exception in processing the last dispatched sequence ID for a HARegionQueue's DACE. The problem is with event ID, %s for HARegion with name=%s",
          regionName, id),
          e);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      return false;
    } catch (RejectedExecutionException ignore) {
      interrupted = true;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return true;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    /*
     * first write the total list size then in a loop write the region name, number of eventIds and
     * the event ids
     */
    super.toData(out, context);
    // write the size of the data list
    DataSerializer.writeInteger(messagesList.size(), out);
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

  @Override
  public int getDSFID() {
    return QUEUE_REMOVAL_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    /*
     * read the total list size, reconstruct the message list in a loop by reading the region name,
     * number of eventIds and the event ids
     */
    super.fromData(in, context);
    // read the size of the message
    int size = DataSerializer.readInteger(in);
    messagesList = new LinkedList<>();
    int eventIdSizeInt;
    for (int i = 0; i < size; i++) {
      // read the region name
      messagesList.add(DataSerializer.readString(in));
      // read the data size
      Integer eventIdSize = DataSerializer.readInteger(in);
      messagesList.add(eventIdSize);
      eventIdSizeInt = eventIdSize;
      // read the total number of events
      for (int j = 0; j < eventIdSizeInt; j++) {
        messagesList.add(DataSerializer.readObject(in));
      }
      // increment i by adding the total number of ids read and 1 for
      // the length of the message
      //
      i = i + eventIdSizeInt + 1;
    }
  }

  @Override
  public String toString() {
    return "QueueRemovalMessage" + messagesList;
  }
}
