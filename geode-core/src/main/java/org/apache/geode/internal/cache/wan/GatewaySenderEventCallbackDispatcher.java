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

package org.apache.geode.internal.cache.wan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.internal.logging.LogService;

/**
 * Class <code>SerialGatewayEventCallbackDispatcher</code> dispatches batches of
 * <code>GatewayEvent</code>s to <code>AsyncEventListener</code> callback implementers. This
 * dispatcher is used in the write-behind case.
 *
 * @since GemFire 7.0
 */
public class GatewaySenderEventCallbackDispatcher implements GatewaySenderEventDispatcher {

  private static final Logger logger = LogService.getLogger();

  /**
   * The <code>SerialGatewayEventProcessor</code> used by this <code>CacheListener</code> to process
   * events.
   */
  protected final AbstractGatewaySenderEventProcessor eventProcessor;

  /**
   * The <code>AsyncEventListener</code>s registered on this
   * <code>SerialGatewayEventCallbackDispatcher</code>.
   */
  private volatile List<AsyncEventListener> eventListeners = Collections.emptyList();

  /**
   * A lock to protect access to the registered <code>AsyncEventListener</code>s.
   */
  private final Object eventLock = new Object();

  public GatewaySenderEventCallbackDispatcher(AbstractGatewaySenderEventProcessor eventProcessor) {
    this.eventProcessor = eventProcessor;
    initializeEventListeners();
  }

  /**
   * Dispatches a batch of messages to all registered <code>AsyncEventListener</code>s.
   *
   * @param events The <code>List</code> of events to send
   * @param removeFromQueueOnException Unused.
   * @return whether the batch of messages was successfully processed
   */
  public boolean dispatchBatch(List events, boolean removeFromQueueOnException, boolean isRetry) {
    GatewaySenderStats statistics = this.eventProcessor.sender.getStatistics();
    boolean success = false;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("About to dispatch batch");
      }
      long start = statistics.startTime();
      // Send the batch to the corresponding GatewaySender
      success = dispatchBatch(events);
      statistics.endBatch(start, events.size());
      if (logger.isDebugEnabled()) {
        logger.debug("Done dispatching the batch");
      }
    } catch (GatewaySenderException e) {
      // Do nothing in this case. The exception has already been logged.
    } catch (CancelException e) {
      this.eventProcessor.setIsStopped(true);
      throw e;
    } catch (Exception e) {
      logger.fatal(
          "Stopping the processor because the following exception occurred while processing a batch:",
          e);
      this.eventProcessor.setIsStopped(true);
    }
    return success;
  }

  /**
   * Registers a <code>AsyncEventListener</code>.
   *
   * @param listener A AsyncEventListener to be registered
   */
  public void registerAsyncEventListener(AsyncEventListener listener) {
    synchronized (eventLock) {
      List<AsyncEventListener> oldListeners = this.eventListeners;
      if (!oldListeners.contains(listener)) {
        List<AsyncEventListener> newListeners = new ArrayList<AsyncEventListener>(oldListeners);
        newListeners.add(listener);
        this.eventListeners = newListeners;
      }
    }
  }

  /**
   * Removes registration of a previously registered <code>AsyncEventListener</code>.
   *
   * @param listener A AsyncEventListener to be unregistered
   */
  public void unregisterGatewayEventListener(AsyncEventListener listener) {
    synchronized (eventLock) {
      List<AsyncEventListener> oldListeners = this.eventListeners;
      if (oldListeners.contains(listener)) {
        List<AsyncEventListener> newListeners = new ArrayList<AsyncEventListener>(oldListeners);
        if (newListeners.remove(listener)) {
          this.eventListeners = newListeners;
        }
      }
    }
  }

  protected void initializeEventListeners() {
    for (AsyncEventListener listener : this.eventProcessor.getSender().getAsyncEventListeners()) {
      registerAsyncEventListener(listener);
    }
  }

  /**
   * Sends a batch of messages to the registered <code>AsyncEventListener</code>s.
   *
   * @param events The <code>List</code> of events to send
   *
   */
  protected boolean dispatchBatch(List events) throws GatewaySenderException {
    if (events.isEmpty()) {
      return true;
    }
    int batchId = this.eventProcessor.getBatchId();
    boolean successAll = true;
    try {
      for (AsyncEventListener listener : this.eventListeners) {
        boolean successOne = listener.processEvents(events);
        if (!successOne) {
          successAll = false;
        }
      }
    } catch (Exception e) {
      final String alias =
          "%s: Exception during processing batch %s";
      final Object[] aliasArgs = new Object[] {this, Integer.valueOf(batchId)};
      String exMsg = String.format(alias, aliasArgs);
      GatewaySenderException ge = new GatewaySenderException(exMsg, e);
      logger.warn(exMsg, ge);
      throw ge;
    }
    return successAll;
  }

  @Override
  public boolean isRemoteDispatcher() {
    return false;
  }

  @Override
  public boolean isConnectedToRemote() {
    return false;
  }

  @Override
  public void stop() {
    // no op

  }

  @Override
  public void shutDownAckReaderConnection() {
    // no op
  }
}
