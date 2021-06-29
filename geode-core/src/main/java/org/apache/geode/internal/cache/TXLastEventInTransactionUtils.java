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

import java.util.Collection;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class TXLastEventInTransactionUtils {

  private static final Logger logger = LogService.getLogger();

  /**
   * @param callbacks list of events belonging to a transaction
   *
   * @return the last event of the transaction.
   *         If the regions to which the events belong do not have senders
   *         that group transactions it returns null.
   *         If the regions to which the
   *         events belong have different sets of senders that group transactions
   *         then it throws a ServiceConfigurationError exception.
   */
  public static EntryEventImpl getLastTransactionEvent(List<EntryEventImpl> callbacks,
      Cache cache)
      throws ServiceConfigurationError {
    if (checkNoSendersGroupTransactionEvents(callbacks, cache)) {
      return null;
    }

    List<Set> senderIdsPerEvent = getGroupingSendersPerEvent(callbacks, cache);
    if (senderIdsPerEvent.stream().distinct().count() > 1) {
      String info = eventsAndSendersPerEventToString(callbacks, senderIdsPerEvent);
      throw new ServiceConfigurationError(
          "Not all events go to the same senders that group transactions. " + info);
    }

    return callbacks.get(callbacks.size() - 1);
  }

  private static String eventsAndSendersPerEventToString(List<EntryEventImpl> callbacks,
      List<Set> senderIdsPerEvent) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < callbacks.size(); i++) {
      buf.append("Event[");
      buf.append(i);
      buf.append("]: ");
      buf.append(callbacks.get(i));
      buf.append(", senders for Event[");
      buf.append(i);
      buf.append("]: ");
      buf.append(senderIdsPerEvent.get(i));
      buf.append("; ");
    }
    return buf.toString();
  }

  private static boolean checkNoSendersGroupTransactionEvents(List<EntryEventImpl> callbacks,
      Cache cache) throws ServiceConfigurationError {
    for (String senderId : getSenderIdsForEvents(callbacks)) {
      GatewaySender sender = cache.getGatewaySender(senderId);
      if (sender != null && sender.mustGroupTransactionEvents()) {
        return false;
      }
    }
    return true;
  }

  private static Set<String> getSenderIdsForEvents(List<EntryEventImpl> callbacks) {
    return callbacks
        .stream()
        .map(event -> event.getRegion().getAllGatewaySenderIds())
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private static List<Set> getGroupingSendersPerEvent(List<EntryEventImpl> callbacks,
      Cache cache) throws ServiceConfigurationError {
    List<Set> senderIdsPerEvent = callbacks
        .stream()
        .map((event) -> event.getRegion().getAllGatewaySenderIds().stream()
            .filter(senderId -> doesSenderGroupTransactionEvents(cache, senderId))
            .collect(
                Collectors.toSet()))
        .collect(Collectors.toList());

    return senderIdsPerEvent;
  }

  private static boolean doesSenderGroupTransactionEvents(Cache cache, String senderId)
      throws ServiceConfigurationError {
    GatewaySender sender = cache.getGatewaySender(senderId);
    if (sender == null) {
      throw new ServiceConfigurationError("No information for sender id: " + senderId);
    }
    return sender.mustGroupTransactionEvents();
  }
}
