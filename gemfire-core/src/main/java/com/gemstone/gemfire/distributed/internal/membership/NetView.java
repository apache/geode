/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;


/**
 * The NetView class represents a membership view.  Note that 
 * this class is not synchronized, so take that under advisement
 * if you decide to modify a view with add() or remove().
 * 
 * @since 5.5 
 */
public class NetView implements DataSerializableFixedID {
  private static final long serialVersionUID = -8888347937416039434L;
  private int viewId;
  private List<InternalDistributedMember> members;
  private List<InternalDistributedMember> shutdownMembers;
  private List<InternalDistributedMember> crashedMembers;
  private InternalDistributedMember creator;
  private Set<InternalDistributedMember> hashedMembers;
  
  public NetView() {
    viewId = 0;
    members = new ArrayList<InternalDistributedMember>(4);
    this.hashedMembers = new HashSet<InternalDistributedMember>(members);
    shutdownMembers = Collections.EMPTY_LIST;
    crashedMembers = Collections.EMPTY_LIST;
    creator = null;
  }

  public NetView(InternalDistributedMember creator) {
    viewId = 0;
    members = new ArrayList<InternalDistributedMember>(4);
    members.add(creator);
    this.hashedMembers = new HashSet<InternalDistributedMember>(members);
    shutdownMembers = Collections.EMPTY_LIST;
    crashedMembers = Collections.EMPTY_LIST;
    this.creator = creator;
  }

  // legacy method for JGMM
  public NetView(int size, long viewId) {
    this.viewId = (int)viewId;
    members = new ArrayList<InternalDistributedMember>(size);
    this.hashedMembers = new HashSet<InternalDistributedMember>(members);
    shutdownMembers = Collections.EMPTY_LIST;
    crashedMembers = Collections.EMPTY_LIST;
    creator = null;
  }

  public NetView(NetView other, int viewId) {
    this.creator = other.creator;
    this.viewId = viewId;
    this.members = new ArrayList<InternalDistributedMember>(other.members);
    this.hashedMembers = new HashSet<InternalDistributedMember>(other.members);
    this.shutdownMembers = new ArrayList<InternalDistributedMember>(other.shutdownMembers);
    this.crashedMembers = new ArrayList<InternalDistributedMember>(other.crashedMembers);
  }
  public NetView(InternalDistributedMember creator,
      int viewId,
      List<InternalDistributedMember> mbrs,
      List<InternalDistributedMember> shutdowns,
      List<InternalDistributedMember> crashes) {
    this.creator = creator;
    this.viewId = viewId;
    this.members = mbrs;
    this.hashedMembers = new HashSet<InternalDistributedMember>(mbrs);
    this.shutdownMembers = shutdowns;
    this.crashedMembers = crashes;
  }
  
  public int getViewId() {
    return this.viewId;
  }
  
  public InternalDistributedMember getCreator() {
    return this.creator;
  }
  
  public void setCreator(InternalDistributedMember creator) {
    this.creator = creator;
  }

  public List<InternalDistributedMember> getMembers() {
    return Collections.unmodifiableList(this.members);
  }
  
  /**
   * return members that are i this view but not the given old view
   */
  public List<InternalDistributedMember> getNewMembers(NetView olderView) {
    List<InternalDistributedMember> result = new ArrayList<InternalDistributedMember>(members);
    result.removeAll(olderView.getMembers());
    return result;
  }

  /**
   * return members added in this view
   */
  public List<InternalDistributedMember> getNewMembers() {
    List<InternalDistributedMember> result = new ArrayList<InternalDistributedMember>(5);
    for (InternalDistributedMember mbr: this.members) {
      if (mbr.getVmViewId() == this.viewId) {
        result.add(mbr);
      }
    }
    return result;
  }
  
  public Object get(int i) {
    return this.members.get(i);
  }
  
  public void add(InternalDistributedMember mbr) {
    this.hashedMembers.add(mbr);
    this.members.add(mbr);
  }
  
  public boolean remove(InternalDistributedMember mbr) {
    this.hashedMembers.remove(mbr);
    return this.members.remove(mbr);
  }
  
  public boolean contains(InternalDistributedMember mbr) {
    return this.hashedMembers.contains(mbr);
  }
  
  public int size() {
    return this.members.size();
  }
  
  public InternalDistributedMember getLeadMember() {
    for (InternalDistributedMember mbr: this.members) {
      if (mbr.getVmKind() == DistributionManager.NORMAL_DM_TYPE) {
        return mbr;
      }
    }
    return null;
  }
  
  public InternalDistributedMember getCoordinator() {
    synchronized(members) {
      for (InternalDistributedMember addr: members) {
        if (addr.getNetMember().preferredForCoordinator()) {
          return addr;
        }
      }
      if (members.size() > 0) {
        return members.get(0);
      }
    }
    return null;
  }

  public List<InternalDistributedMember> getShutdownMembers() {
    return this.shutdownMembers;
  }
  
  public List<InternalDistributedMember> getCrashedMembers() {
    return this.crashedMembers;
  }
  
  /** check to see if the given address is next in line to be coordinator */
  public boolean shouldBeCoordinator(InternalDistributedMember who) {
    Iterator<InternalDistributedMember> it = this.members.iterator();
    InternalDistributedMember firstNonPreferred = null;
    while (it.hasNext()) {
      InternalDistributedMember mbr = it.next();
      if (mbr.getNetMember().preferredForCoordinator()) {
        return mbr.equals(who);
      } else if (firstNonPreferred == null) {
        firstNonPreferred = mbr;
      }
    }
    return (firstNonPreferred == null  ||  firstNonPreferred.equals(who));
  }
  
  public String toString() {
    InternalDistributedMember lead = getLeadMember();
    
    StringBuilder sb = new StringBuilder(200);
    sb.append("View[").append(creator).append('|').append(viewId).append("] members: [");
    boolean first = true;
    for (InternalDistributedMember mbr: this.members) {
      if (!first) sb.append(", ");
      sb.append(mbr);
      if (mbr == lead) {
        sb.append("{lead}");
      }
      first = false;
    }
    if ( !this.shutdownMembers.isEmpty() ) {
      sb.append("]  shutdown: [");
      first = true;
      for (InternalDistributedMember mbr: this.shutdownMembers) {
        if (!first) sb.append(", ");
        sb.append(mbr);
        first = false;
      }
    }
    if ( !this.crashedMembers.isEmpty() ) {
      sb.append("]  crashed: [");
      first = true;
      for (InternalDistributedMember mbr: this.crashedMembers) {
        if (!first) sb.append(", ");
        sb.append(mbr);
        first = false;
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public synchronized boolean equals(Object arg0) {
    if (arg0 == this) {
      return true;
    }
    if ( !(arg0 instanceof NetView) ) {
      return false;
    }
    return this.members.equals(((NetView)arg0).getMembers());
  }
  @Override
  public synchronized int hashCode() {
    return this.members.hashCode();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(creator, out);
    out.writeInt(viewId);
    writeAsArrayList(members, out);
    writeAsArrayList(shutdownMembers, out);
    writeAsArrayList(crashedMembers, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    creator = DataSerializer.readObject(in);
    viewId = in.readInt();
    members = DataSerializer.readArrayList(in);
    this.hashedMembers = new HashSet<InternalDistributedMember>(members);
    shutdownMembers = DataSerializer.readArrayList(in);
    crashedMembers = DataSerializer.readArrayList(in);
  }
  
  /** this will deserialize as an ArrayList */
  private void writeAsArrayList(List list, DataOutput out) throws IOException {
    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      for (int i=0; i < size; i++) {
        DataSerializer.writeObject(list.get(i), out);
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return NETVIEW;
  }
}

