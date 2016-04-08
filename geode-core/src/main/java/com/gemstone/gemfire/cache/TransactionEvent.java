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

package com.gemstone.gemfire.cache;
import java.util.*;

/**
 * <p>An event that describes the culmination of an entire transaction.
 * It either describes the work done by a committed transaction
 * or the work abandoned by an explicit rollback or failed commit.
 * The actual work is represented by an ordered list of {@link EntryEvent}
 * instances.
 * 
 * <p>A <code>TransactionListener</code> receives an instance of this
 * class allowing exploration of the resultant operations.  The
 * resultant operation is the final result of, potentially, a sequence
 * of operations on a key such that earlier operations might be
 * masked.  For example, multiple put operations using the same key
 * will result in only one {@link EntryEvent} for that key.
 * 
 * <p>An instance of TransactionEvent for the same transaction on
 * the originating VM may differ from a recipient VM.  The amount of
 * variation will depend on the variation of the state of Entries on
 * each VM.  One reason for why this might occur is the different
 * Expiration/Eviction settings of the similar Regions on different
 * VMs.
 *
 * <p>The event lists are ordered according to the chronological order of
 * the indiviual operations.
 *
 * <p>The {@link EntryEvent} instances always return <code>null</code>
 * as their {@link CacheEvent#getCallbackArgument callback argument}.
 *
 *
 * @see TransactionListener
 * @see EntryEvent
 * @since 4.0
 */
public interface TransactionEvent {

  /** Gets the <code>TransactionId</code> associated this TransactionEvent.
   * 
   */
  public TransactionId getTransactionId();

  /** Gets all "create" EntryEvents for this transaction;
   * <code>Region.create</code> and/or <code>Region.put</code>
   *
   * @return <code>List</code> of <code>EntryEvents</code> or <code>Collections.EMPTY_LIST</code>
   * @deprecated as of GemFire 5.0, use {@link #getEvents} instead
   */
  @Deprecated
  public List<EntryEvent<?,?>> getCreateEvents();

  /** Gets all "destroy" EntryEvents for this
   * transaction; <code>Region.destroy</code> and
   * <code>Region.localDestroy</code>.
   * 
   * @return <code>List</code> of <code>EntryEvents</code> or <code>Collections.EMPTY_LIST</code>
   * @deprecated as of GemFire 5.0, use {@link #getEvents} instead
   */
  @Deprecated
  public List<EntryEvent<?,?>> getDestroyEvents();

  /** Gets all <code>Region.put</code> EntryEvents for this transaction.
   *
   * @return <code>List</code> of <code>EntryEvents</code> or <code>Collections.EMPTY_LIST</code>
   * @deprecated as of GemFire 5.0, use {@link #getEvents} instead
   */
  @Deprecated
  public List<EntryEvent<?,?>> getPutEvents();

  /** Gets all "invalidate" EntryEvents for this transaction; 
   *  <code>Region.invalidate</code> and
   *  <code>Region.localInvalidate</code>.
   *
   * @return <code>List</code> of <code>EntryEvents</code> or <code>Collections.EMPTY_LIST</code>
   * @deprecated as of GemFire 5.0, use {@link #getEvents} instead
   */
  @Deprecated
  public List<EntryEvent<?,?>> getInvalidateEvents();

  /**
   * Returns an ordered list of every {@link CacheEvent} for this transaction.
   * The event order is consistent with the order in which the operations were
   * performed during the transaction.
   * @return an unmodifiable <code>List</code> of all the {@link CacheEvent} instances;
   * one for each operation performed by this transaction.
   * @since 5.0
   */
  public List<CacheEvent<?,?>> getEvents();
  
  /** Gets the Cache for this transaction event
   *
   * @return <code>Cache</code>
   */
  public Cache getCache();

}
