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
package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * Used to asynchronously disconnect an InternalDistributedSystem
 * when we run out of off-heap memory.
 * If the STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY sys prop
 * is set to true then this listener will not disconnect.
 */
class DisconnectingOutOfOffHeapMemoryListener implements OutOfOffHeapMemoryListener {
  private final Object lock = new Object();
  private InternalDistributedSystem ids;
  
  DisconnectingOutOfOffHeapMemoryListener(InternalDistributedSystem ids) {
    this.ids = ids;
  }
  
  public void close() {
    synchronized (lock) {
      this.ids = null; // set null to prevent memory leak after closure!
    }
  }
  
  @Override
  public void outOfOffHeapMemory(final OutOfOffHeapMemoryException cause) {
    synchronized (lock) {
      if (this.ids == null) {
        return;
      }
      if (Boolean.getBoolean(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY)) {
        return;
      }
      
      final InternalDistributedSystem dsToDisconnect = this.ids;
      this.ids = null; // set null to prevent memory leak after closure!
      
      if (dsToDisconnect.getDistributionManager().getRootCause() == null) {
        dsToDisconnect.getDistributionManager().setRootCause(cause);
      }
        
      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          dsToDisconnect.getLogWriter().info("OffHeapStorage about to invoke disconnect on " + dsToDisconnect);
          dsToDisconnect.disconnect(cause.getMessage(), cause, false);
        }
      };
      
      // invoking disconnect is async because caller may be a DM pool thread which will block until DM shutdown times out

      //LogWriterImpl.LoggingThreadGroup group = LogWriterImpl.createThreadGroup("MemScale Threads", ids.getLogWriterI18n());
      String name = this.getClass().getSimpleName()+"@"+this.hashCode()+" Handle OutOfOffHeapMemoryException Thread";
      //Thread thread = new Thread(group, runnable, name);
      Thread thread = new Thread(runnable, name);
      thread.setDaemon(true);
      thread.start();
    }
  }
}