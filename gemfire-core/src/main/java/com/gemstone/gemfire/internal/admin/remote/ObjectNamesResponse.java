/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
