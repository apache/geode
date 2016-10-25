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

package org.apache.geode.internal.cache.locks;

import org.apache.geode.distributed.internal.locks.DLockBatchId;
import org.apache.geode.distributed.internal.locks.LockGrantorId;
import org.apache.geode.distributed.internal.membership.*;

/** Specifies a set of keys to try-lock within the scope of a region */
public interface TXLockId extends DLockBatchId {
  /** Gets the member id of the owner of this lock */
  public InternalDistributedMember getMemberId();
  /** Gets the count that identifies the lock id in this member */
  public int getCount();
  /** Sets the lock grantor id that granted this lock */
  public void setLockGrantorId(LockGrantorId lockGrantorId);
  /** Gets the lock grantor id that granted this lock */
  public LockGrantorId getLockGrantorId();
}

