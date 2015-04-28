/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.Node;

/**
 * Versioned ArrayList which maintains the version everytime the list gets
 * modified. This is thread-safe in terms of add and remove operations and also
 * list is an unmodifiable list to avoid ConcurrentModificationException.
 * 
 * @see java.util.ConcurrentModificationException
 * 
 * @author rreja
 */
public class VersionedArrayList implements DataSerializable, Versionable, Iterable<Node>
{
  private static final long serialVersionUID = -1455442285961593385L;

  /** Version of the list. */
  private long version = -1;

  /** ArrayList */
  private List<Node> list;
  
//  private List vhist = new ArrayList();  // DEBUG
  
  /**
   * Constructor for DataSerializable.
   */
  public VersionedArrayList() {
    this.list = new ArrayList<Node>();
  }

  /**
   * Constructor.
   * 
   * @param size
   */
  public VersionedArrayList(int size) {
    this.list = new ArrayList<Node>(size);
  }

  public VersionedArrayList(List<? extends Node> list) {
    this.list = Collections.unmodifiableList(list);
//    incrementVersion("i->" + list);
    incrementVersion();
  }

  /**
   * Adds obj to the list. Addition is done by making a copy of the existing
   * list and then adding the obj to the new list and assigning the old list to
   * the new unmodifiable list. This is to ensure that the iterator of the list
   * doesn't get ConcurrentModificationException.
   * 
   * @see java.util.ConcurrentModificationException
   * @param obj
   */
  public synchronized void add(Node obj)
  {

    ArrayList newList = new ArrayList<Node>(this.list);
    newList.add(obj);
    this.list = Collections.unmodifiableList(newList);
//    incrementVersion("a->" + obj);
    incrementVersion();
  }

  /**
   * Removes obj from the list. Removal is done by making a copy of the existing
   * list and then removing the obj from the new list.  If the object was removed, 
   * the list is assigning to the new unmodifiable list. 
   * This is to ensure that the iterator of the list doesn't get ConcurrentModificationException.
   * @return true if the element was removed and the version was changed, otherwise version and list are left
   * unmodified.
   * 
   * @see java.util.ConcurrentModificationException
   * @param obj the object to remove from the list
   */
  public synchronized boolean remove(Node obj)
  {
    ArrayList<Node> newList = new ArrayList<Node>(this.list);
    boolean ret = newList.remove(obj);
    if (ret) {
      this.list = Collections.unmodifiableList(newList);
//      incrementVersion("r->" + obj);
      incrementVersion();
    } 
    return ret;
  }

  /**
   * Returns the iterator.
   * 
   * @return a list Iterator
   */
  public synchronized Iterator<Node> iterator()
  {
    return this.list.iterator();
  }

  /**
   * Returns the size of the list.
   * 
   * @return int
   */
  public synchronized int size()
  {
    return this.list.size();
  }

  /**
   * Returns true if obj is present in the list otherwise false.
   * 
   * @param obj
   * @return true if obj is present in the list
   */
  public boolean contains(Node obj)
  {
    final List<Node> l; 
    synchronized(this) {
      l = this.list;
    }
    return l.contains(obj);
  }

  

  /**
   * Returns Object at index i.
   * 
   * @param i
   * @return Object
   */
  public Object get(int i)
  {
    final List<Node> l; 
    synchronized(this) {
      l = this.list;
    }
    return l.get(i);
  }

  /**
   * Returns the index of Object if present, else -1.
   * 
   * @param obj
   * @return int
   */
  public int indexOf(Object obj)
  {
    final List<Node> l;
    synchronized(this) {
      l = this.list;
    }
    return l.indexOf(obj);
  }

  /**
   * Returns a copy of the arraylist contained.
   * 
   * @return ArrayList
   */
  public Set<Node> getListCopy()
  {
    final List<Node> l;
    synchronized(this) {
      l = this.list;
    }
    return new HashSet<Node>(l);
  }

  /**
   * Prints the version and elements of the list.
   * 
   * @return String with version and elements of the list.
   */
  @Override
  public String toString()
  {
    final List<Node> l;
//    final List vh;
    synchronized(this) {
      l = this.list;
//      vh = this.vhist;
    }
    StringBuffer sb = new StringBuffer();
    sb.append("ArrayList version = " + getVersion() + " Elements = { ");
    for (int i = 0; i < l.size(); i++) {
      sb.append(l.get(i).toString() + ", ");
    }
//    sb.append("vhist:\n " + vh);
    sb.append("}");
    return sb.toString();
  }

 
  public void toData(DataOutput out) throws IOException
  {
    long v = -1;
    final List<Node> l;
//    final List vh;
    synchronized (this) {
      v = this.version;
      l = this.list;
//      vh = this.vhist;
    }

    out.writeLong(v);
    final int s = l.size();
    out.writeInt(s);
    for (int k = 0; k < s; k++) {
      InternalDataSerializer.invokeToData((l.get(k)), out);
    }

//    final int sh = vh.size();
//    out.writeInt(sh);
//    for (int k = 0; k < sh; k++) {
//      out.writeUTF((String) vh.get(k));
//    }
  }


  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    final ArrayList<Node> l = new ArrayList<Node>();
    final long v = in.readLong();
    final int size = in.readInt();
    for (int k = 0; k < size; k++) {      
      l.add(new Node(in));
    }
    
//    final ArrayList vh = new ArrayList();
//    final int vhsize = in.readInt();
//    for (int k = 0; k < vhsize; k++) {      
//      vh.add(in.readUTF());
//    }
    
    synchronized (this) {
      this.version = v;
      this.list = Collections.unmodifiableList(l);
//      this.vhist = Collections.unmodifiableList(vh);
    }
  }

  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.util.Versionable#getVersion()
   */
  public synchronized Comparable getVersion()
  {
    return Long.valueOf(this.version);
  }
  
  public boolean isNewerThan(Versionable other) {
    if (other instanceof VersionedArrayList) {
      final long v = ((Long) other.getVersion()).longValue();
      synchronized(this) {
        return this.version > v;
      }
    } else {
      final Comparable o = other.getVersion();
      return getVersion().compareTo(o) > 0;
    } 
  }
  
  public boolean isOlderThan(Versionable other) {
    if (other instanceof VersionedArrayList) {
      final long v;
      synchronized(other) {
        v = ((VersionedArrayList) other).version;        
      }
      synchronized(this) {
        return this.version < v;
      }
    } else {
      final Comparable o = other.getVersion();
      return getVersion().compareTo(o) < 0;
    } 
  }
  
  public boolean isSame(Versionable other)
  {
    if (other instanceof VersionedArrayList) {
      final long v;
      synchronized(other) {
        v = ((VersionedArrayList) other).version;        
      }
      synchronized(this) {
        return this.version == v;
      }
    } else {
      final Comparable o = other.getVersion();
      return getVersion().compareTo(o) == 0;
    } 
  }

//private synchronized void incrementVersion(String op)
  private synchronized void incrementVersion()
  {
// DEBUG
//    {
//      StringWriter s = new StringWriter();
//      final String pre = 
//        "o=" + op + 
//        ":t=" + System.currentTimeMillis() + 
//        ":m=" + InternalDistributedSystem.getAnyInstance().getDistributedMember().toString() + ": ";
//      s.write(pre);
//      PrintWriter p = new PrintWriter(s);
//      new Exception().fillInStackTrace().printStackTrace(p);
//      
//      ArrayList newList = new ArrayList(this.vhist);
//      newList.add(pre); // Capture what code added this version
//      this.vhist = Collections.unmodifiableList(newList);
//    }
    
    ++this.version;
  }
}
