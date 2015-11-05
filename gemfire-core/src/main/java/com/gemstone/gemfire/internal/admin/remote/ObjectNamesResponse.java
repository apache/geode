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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link ObjectNamesResponse}.
 */
public final class ObjectNamesResponse extends AdminResponse implements Cancellable {
  // instance variables
  private HashSet objectNames;
  private transient boolean cancelled;

  /**
   * Returns a <code>ObjectNamesResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static ObjectNamesResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    ObjectNamesResponse m = new ObjectNamesResponse();
    m.setRecipient(recipient);
    return m;
  }

  public void buildNames(Region r) {
    if (cancelled) { return; }

    Set nameSet = r.keys();
    Iterator it = nameSet.iterator();
    objectNames = new HashSet();
    synchronized(r) {
      while(it.hasNext()) {
        if (cancelled) { break; }
        Object name = it.next();
        if (name instanceof String || name instanceof Number) {
          objectNames.add(name);
        } else {
          objectNames.add(new RemoteObjectName(name));
        }
      }
    }
  }

  public synchronized void cancel() {
    cancelled = true;
  }

  // instance methods
  public Set getNameSet() {
    return new HashSet(this.objectNames);
  }
  
  public int getDSFID() {
    return OBJECT_NAMES_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.objectNames, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.objectNames = (HashSet)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "ObjectNamesResponse from " + this.getRecipient();
  }
}
