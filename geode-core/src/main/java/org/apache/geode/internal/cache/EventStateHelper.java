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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.event.EventSequenceNumberHolder;
import org.apache.geode.internal.cache.ha.HARegionQueue.DispatchedAndCurrentEvents;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

/**
 * A helper class for serializing the event state map.
 *
 * TODO - Store the event state map in DataSerializable object that keeps the map in this compressed
 * format in memory.
 *
 */
public class EventStateHelper {
  /**
   * Post 7.1, if changes are made to this method make sure that it is backwards compatible by
   * creating toDataPreXX methods. Also make sure that the callers to this method are backwards
   * compatible by creating toDataPreXX methods for them even if they are not changed. <br>
   * Callers for this method are: <br>
   * {@link DataSerializableFixedID#toData(DataOutput, org.apache.geode.internal.serialization.SerializationContext)}
   * <br>
   * {@link DataSerializableFixedID#toData(DataOutput, org.apache.geode.internal.serialization.SerializationContext)}
   * <br>
   *
   * @param myId the memberId that is serializing
   */
  @SuppressWarnings("synthetic-access")
  public static void dataSerialize(DataOutput dop, Map eventState, boolean isHARegion,
      InternalDistributedMember myId) throws IOException {
    // For HARegionQueues, the event state map is uses different values
    // than a regular region :(
    Map<EventStateMemberIdentifier, Map<ThreadIdentifier, Object>> groupedThreadIds =
        groupThreadIds(eventState);
    List<EventStateMemberIdentifier> orderedIds = new LinkedList();
    Map<EventStateMemberIdentifier, Integer> seenIds = new HashMap();

    myId.writeEssentialData(dop); // added in 7.0 for version tag processing in fromData

    for (EventStateMemberIdentifier memberId : groupedThreadIds.keySet()) {
      if (!seenIds.containsKey(memberId)) {
        orderedIds.add(memberId);
        seenIds.put(memberId, seenIds.size());
      }
    }

    dop.writeInt(seenIds.size());
    for (EventStateMemberIdentifier memberId : orderedIds) {
      DataSerializer.writeByteArray(memberId.bytes, dop);
    }

    dop.writeInt(groupedThreadIds.size());
    for (Map.Entry<EventStateMemberIdentifier, Map<ThreadIdentifier, Object>> memberIdEntry : groupedThreadIds
        .entrySet()) {
      EventStateMemberIdentifier memberId = memberIdEntry.getKey();
      dop.writeInt(seenIds.get(memberId));
      Map<ThreadIdentifier, Object> threadIdMap = memberIdEntry.getValue();
      dop.writeInt(threadIdMap.size());
      for (Object next : threadIdMap.entrySet()) {
        Map.Entry entry = (Map.Entry) next;
        ThreadIdentifier key = (ThreadIdentifier) entry.getKey();
        dop.writeLong(key.getThreadID());
        if (isHARegion) {
          DispatchedAndCurrentEvents value = (DispatchedAndCurrentEvents) entry.getValue();
          InternalDataSerializer.invokeToData(value, dop);
        } else {
          EventSequenceNumberHolder value = (EventSequenceNumberHolder) entry.getValue();
          InternalDataSerializer.invokeToData(value, dop);
        }
      }
    }

  }

  /**
   * Post 7.1, if changes are made to this method make sure that it is backwards compatible by
   * creating fromDataPreXX methods. Also make sure that the callers to this method are backwards
   * compatible by creating fromDataPreXX methods for them even if they are not changed. <br>
   * Callers for this method are: <br>
   * {@link DataSerializableFixedID#fromData(DataInput, org.apache.geode.internal.serialization.DeserializationContext)}
   * <br>
   * {@link DataSerializableFixedID#fromData(DataInput, org.apache.geode.internal.serialization.DeserializationContext)}
   * <br>
   */
  public static Map deDataSerialize(DataInput dip, boolean isHARegion)
      throws IOException, ClassNotFoundException {

    InternalDistributedMember senderId = InternalDistributedMember.readEssentialData(dip);

    int numIds = dip.readInt();
    Map<Integer, byte[]> numberToMember = new HashMap();
    for (int i = 0; i < numIds; i++) {
      numberToMember.put(i, DataSerializer.readByteArray(dip));
    }

    int size = dip.readInt();
    HashMap eventState = new HashMap(size);
    for (int i = 0; i < size; i++) {
      int idNumber = dip.readInt();
      int subMapSize = dip.readInt();

      for (int j = 0; j < subMapSize; j++) {
        long threadId = dip.readLong();
        ThreadIdentifier key = new ThreadIdentifier(numberToMember.get(idNumber), threadId);
        if (isHARegion) {
          DispatchedAndCurrentEvents value = new DispatchedAndCurrentEvents();
          InternalDataSerializer.invokeFromData(value, dip);
          eventState.put(key, value);
        } else {
          EventSequenceNumberHolder value = new EventSequenceNumberHolder();
          InternalDataSerializer.invokeFromData(value, dip);
          eventState.put(key, value);
          if (value.getVersionTag() != null) {
            value.getVersionTag().replaceNullIDs(senderId);
          }
        }
      }
    }
    return eventState;
  }


  private static Map<EventStateMemberIdentifier, Map<ThreadIdentifier, Object>> groupThreadIds(
      Map eventState) {
    Map<EventStateMemberIdentifier, Map<ThreadIdentifier, Object>> results =
        new HashMap<>();
    for (Object next : eventState.entrySet()) {
      Map.Entry entry = (Map.Entry) next;
      ThreadIdentifier key = (ThreadIdentifier) entry.getKey();
      EventStateMemberIdentifier memberId = new EventStateMemberIdentifier(key.getMembershipID());
      Object value = entry.getValue();
      Map<ThreadIdentifier, Object> subMap = results.get(memberId);
      if (subMap == null) {
        subMap = new HashMap<>();
        results.put(memberId, subMap);
      }
      subMap.put(key, value);
    }

    return results;
  }

  private static class EventStateMemberIdentifier {
    private final byte[] bytes;

    public EventStateMemberIdentifier(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(bytes);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof EventStateMemberIdentifier)) {
        return false;
      }
      EventStateMemberIdentifier other = (EventStateMemberIdentifier) obj;
      return Arrays.equals(bytes, other.bytes);
    }
  }
}
