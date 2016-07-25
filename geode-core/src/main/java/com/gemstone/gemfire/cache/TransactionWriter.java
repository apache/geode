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


/**
 * A callback that is allowed to veto a transaction. Only one TransactionWriter can exist 
 * per cache, and only one TransactionWriter will be fired in the 
 * entire distributed system for each transaction.
 *
 * This writer can be used to update a backend data source before the GemFire cache is updated during commit.
 * If the backend update fails, the implementer can throw a {@link TransactionWriterException} to veto the transaction.
 * @see CacheTransactionManager#setWriter
 * @since GemFire 6.5
 */

public interface TransactionWriter extends CacheCallback {
  
  /** Called before the transaction has finished committing, but after conflict checking. 
   * Provides an opportunity for implementors to cause transaction abort by throwing a
   * TransactionWriterException
   * 
   * @param event the TransactionEvent
   * @see CacheTransactionManager#commit
   * @throws TransactionWriterException in the event that the transaction should be rolled back
   */
  public void beforeCommit(TransactionEvent event) throws TransactionWriterException;

}
