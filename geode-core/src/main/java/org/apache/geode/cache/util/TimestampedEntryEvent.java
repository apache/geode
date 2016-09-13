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
package org.apache.geode.cache.util;

/**
 * TimestampedEntryEvent is an EntryEvent that has additional information provided
 * to GatewayConflictResolver plugins.  It holds the low 4 bytes of the millisecond
 * clock value from the point of origin of the event and the distributed system ID
 * of the system that caused the change.  It also has this information for the
 * previous change to the entry.
 * @since GemFire 7.0
 */
public interface TimestampedEntryEvent extends org.apache.geode.cache.EntryEvent {
  // note that this interface inherits the following methods, among others:
  // getRegion(), getOperation(), getCallbackArgument()
  // getKey()
  // getOldValue(), getNewValue()
  // getSerializedOldValue(), getSerializedNewValue()

  public int getNewDistributedSystemID();
  public int getOldDistributedSystemID();

  public long getNewTimestamp();
  public long getOldTimestamp();
}
