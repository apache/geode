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
package com.gemstone.gemfire.internal.cache;

/**
 * This class is an event that just holds an EventID.
 * Unlike other EntryEventImpls this class does not need to be released
 * since its values are never off-heap.
 */
public class EventIDHolder extends EntryEventImpl {
  /*
   * This constructor is used to create a bridge event in server-side
   * command classes.  Events created with this are not intended to be
   * used in cache operations.
   * @param id the identity of the client's event
   */
  public EventIDHolder(EventID id) {
    setEventId(id);
    disallowOffHeapValues();
  }
}
