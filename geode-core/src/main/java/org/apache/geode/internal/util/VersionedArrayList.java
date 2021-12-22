/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.Node;

/**
 * Versioned ArrayList which maintains the version everytime the list gets modified. This is
 * thread-safe in terms of add and remove operations and also list is an unmodifiable list to avoid
 * ConcurrentModificationException.
 *
 * @see java.util.ConcurrentModificationException
 *
 */
public class VersionedArrayList implements DataSerializable, Versionable, Iterable<Node> {
  private static final long serialVersionUID = -1455442285961593385L;

  /** Version of the list. */
  private long version = -1;

  /** ArrayList */
  private List<Node> list;

  // private List vhist = new ArrayList(); // DEBUG

  /**
   * Constructor for DataSerializable.
   */
  public VersionedArrayList() {
    list = new ArrayList<>();
  }

  /**
   * Constructor.
   *
   */
  public VersionedArrayList(int size) {
    list = new ArrayList<>(size);
  }

  public VersionedArrayList(List<? extends Node> list) {
    this.list = Collections.unmodifiableList(list);
    // incrementVersion("i->" + list);
    incrementVersion();
  }

  /**
   * Adds obj to the list. Addition is done by making a copy of the existing list and then adding
   * the obj to the new list and assigning the old list to the new unmodifiable list. This is to
   * ensure that the iterator of the list doesn't get ConcurrentModificationException.
   *
   * @see java.util.ConcurrentModificationException
   */
  public synchronized void add(Node obj) {

    ArrayList newList = new ArrayList<>(list);
    newList.add(obj);
    list = Collections.unmodifiableList(newList);
    // incrementVersion("a->" + obj);
    incrementVersion();
  }

  /**
   * Removes obj from the list. Removal is done by making a copy of the existing list and then
   * removing the obj from the new list. If the object was removed, the list is assigning to the new
   * unmodifiable list. This is to ensure that the iterator of the list doesn't get
   * ConcurrentModificationException.
   *
   * @return true if the element was removed and the version was changed, otherwise version and list
   *         are left unmodified.
   *
   * @see java.util.ConcurrentModificationException
   * @param obj the object to remove from the list
   */
  public synchronized boolean remove(Node obj) {
    ArrayList<Node> newList = new ArrayList<>(list);
    boolean ret = newList.remove(obj);
    if (ret) {
      list = Collections.unmodifiableList(newList);
      // incrementVersion("r->" + obj);
      incrementVersion();
    }
    return ret;
  }

  /**
   * Returns the iterator.
   *
   * @return a list Iterator
   */
  @Override
  public synchronized Iterator<Node> iterator() {
    return list.iterator();
  }

  /**
   * Returns the size of the list.
   *
   */
  public synchronized int size() {
    return list.size();
  }

  /**
   * Returns true if obj is present in the list otherwise false.
   *
   * @return true if obj is present in the list
   */
  public boolean contains(Node obj) {
    final List<Node> l;
    synchronized (this) {
      l = list;
    }
    return l.contains(obj);
  }



  /**
   * Returns Object at index i.
   *
   */
  public Object get(int i) {
    final List<Node> l;
    synchronized (this) {
      l = list;
    }
    return l.get(i);
  }

  /**
   * Returns the index of Object if present, else -1.
   *
   */
  public int indexOf(Object obj) {
    final List<Node> l;
    synchronized (this) {
      l = list;
    }
    return l.indexOf(obj);
  }

  /**
   * Returns a copy of the arraylist contained.
   *
   */
  public Set<Node> getListCopy() {
    final List<Node> l;
    synchronized (this) {
      l = list;
    }
    return new HashSet<>(l);
  }

  /**
   * Prints the version and elements of the list.
   *
   * @return String with version and elements of the list.
   */
  @Override
  public String toString() {
    final List<Node> l;
    // final List vh;
    synchronized (this) {
      l = list;
      // vh = this.vhist;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("ArrayList version = " + getVersion() + " Elements = { ");
    for (Node node : l) {
      sb.append(node.toString() + ", ");
    }
    // sb.append("vhist:\n " + vh);
    sb.append("}");
    return sb.toString();
  }


  @Override
  public void toData(DataOutput out) throws IOException {
    long v = -1;
    final List<Node> l;
    // final List vh;
    synchronized (this) {
      v = version;
      l = list;
      // vh = this.vhist;
    }

    out.writeLong(v);
    final int s = l.size();
    out.writeInt(s);
    for (Node node : l) {
      InternalDataSerializer.invokeToData(node, out);
    }

    // final int sh = vh.size();
    // out.writeInt(sh);
    // for (int k = 0; k < sh; k++) {
    // out.writeUTF((String) vh.get(k));
    // }
  }


  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    final ArrayList<Node> l = new ArrayList<>();
    final long v = in.readLong();
    final int size = in.readInt();
    for (int k = 0; k < size; k++) {
      l.add(new Node(in));
    }

    // final ArrayList vh = new ArrayList();
    // final int vhsize = in.readInt();
    // for (int k = 0; k < vhsize; k++) {
    // vh.add(in.readUTF());
    // }

    synchronized (this) {
      version = v;
      list = Collections.unmodifiableList(l);
      // this.vhist = Collections.unmodifiableList(vh);
    }
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.util.Versionable#getVersion()
   */
  @Override
  public synchronized Comparable getVersion() {
    return Long.valueOf(version);
  }

  @Override
  public boolean isNewerThan(Versionable other) {
    if (other instanceof VersionedArrayList) {
      final long v = ((Long) other.getVersion()).longValue();
      synchronized (this) {
        return version > v;
      }
    } else {
      final Comparable o = other.getVersion();
      return getVersion().compareTo(o) > 0;
    }
  }

  @Override
  public boolean isOlderThan(Versionable other) {
    if (other instanceof VersionedArrayList) {
      final long v;
      synchronized (other) {
        v = ((VersionedArrayList) other).version;
      }
      synchronized (this) {
        return version < v;
      }
    } else {
      final Comparable o = other.getVersion();
      return getVersion().compareTo(o) < 0;
    }
  }

  @Override
  public boolean isSame(Versionable other) {
    if (other instanceof VersionedArrayList) {
      final long v;
      synchronized (other) {
        v = ((VersionedArrayList) other).version;
      }
      synchronized (this) {
        return version == v;
      }
    } else {
      final Comparable o = other.getVersion();
      return getVersion().compareTo(o) == 0;
    }
  }

  // private synchronized void incrementVersion(String op)
  private synchronized void incrementVersion() {
    // DEBUG
    // {
    // StringWriter s = new StringWriter();
    // final String pre =
    // "o=" + op +
    // ":t=" + System.currentTimeMillis() +
    // ":m=" + InternalDistributedSystem.getAnyInstance().getDistributedMember().toString() + ": ";
    // s.write(pre);
    // PrintWriter p = new PrintWriter(s);
    // new Exception().fillInStackTrace().printStackTrace(p);
    //
    // ArrayList newList = new ArrayList(this.vhist);
    // newList.add(pre); // Capture what code added this version
    // this.vhist = Collections.unmodifiableList(newList);
    // }

    ++version;
  }
}
