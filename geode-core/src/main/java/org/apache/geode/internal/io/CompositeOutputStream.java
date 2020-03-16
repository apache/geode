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
package org.apache.geode.internal.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Delegates all operations to a collection of OutputStreams.
 *
 * @since GemFire 7.0
 */
public class CompositeOutputStream extends OutputStream implements Iterable<OutputStream> {

  protected final Object lock = new Object();

  private volatile Set<OutputStream> streams = Collections.<OutputStream>emptySet();

  /**
   * Constructs a new instance of CompositeOutputStream with zero or more OutputStreams.
   *
   * @param out zero or more OutputStreams to add to the new instance of CompositeOutputStream
   */
  public CompositeOutputStream(OutputStream... out) {
    final Set<OutputStream> newSet = new HashSet<OutputStream>();
    for (OutputStream stream : out) {
      newSet.add(stream);
    }
    this.streams = newSet;
  }

  /**
   * @return <tt>true</tt> if this CompositeOutputStream did not already contain the specified
   *         OutputStream
   */
  public boolean addOutputStream(OutputStream out) {
    synchronized (this.lock) {
      final Set<OutputStream> oldSet = this.streams;
      if (oldSet.contains(out)) {
        return false;
      } else {
        final Set<OutputStream> newSet = new HashSet<OutputStream>(oldSet);
        final boolean added = newSet.add(out);
        this.streams = newSet;
        return added;
      }
    }
  }

  /**
   * @return <tt>true</tt> if this CompositeOutputStream contained the specified OutputStream
   */
  public boolean removeOutputStream(OutputStream out) {
    synchronized (this.lock) {
      final Set<OutputStream> oldSet = this.streams;
      if (!oldSet.contains(out)) {
        return false;
      } else if (oldSet.size() == 1) {
        this.streams = Collections.<OutputStream>emptySet();
        return true;
      } else {
        final Set<OutputStream> newSet = new HashSet<OutputStream>(oldSet);
        final boolean removed = newSet.remove(out);
        this.streams = newSet;
        return removed;
      }
    }
  }

  /**
   * Returns <tt>true</tt> if this CompositeOutputStream contains no OutputStreams.
   *
   * @return <tt>true</tt> if this CompositeOutputStream contains no OutputStreams
   */
  public boolean isEmpty() {
    return this.streams.isEmpty();
  }

  /**
   * Returns the number of OutputStreams in this CompositeOutputStream (its cardinality).
   *
   * @return the number of OutputStreams in this CompositeOutputStream (its cardinality)
   */
  public int size() {
    return this.streams.size();
  }

  @Override
  public Iterator<OutputStream> iterator() {
    return this.streams.iterator();
  }

  /**
   * Writes the specified <code>byte</code> to this output stream.
   * <p>
   * The <code>write</code> method of <code>FilterOutputStream</code> calls the <code>write</code>
   * method of its underlying output stream, that is, it performs <tt>out.write(b)</tt>.
   * <p>
   * Implements the abstract <tt>write</tt> method of <tt>OutputStream</tt>.
   *
   * @param b the <code>byte</code>.
   * @exception IOException if an I/O error occurs.
   */
  @Override
  public void write(int b) throws IOException {
    Set<OutputStream> outputStreams = this.streams;
    for (OutputStream out : outputStreams) {
      out.write(b);
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out to the
   * stream.
   * <p>
   * The <code>flush</code> method of <code>FilterOutputStream</code> calls the <code>flush</code>
   * method of its underlying output stream.
   *
   * @exception IOException if an I/O error occurs.
   * @see OutputStream#flush()
   */
  @Override
  public void flush() throws IOException {
    Set<OutputStream> outputStreams = this.streams;
    for (OutputStream out : outputStreams) {
      out.flush();
    }
  }

  /**
   * Closes this output stream and releases any system resources associated with the stream.
   * <p>
   * The <code>close</code> method of <code>FilterOutputStream</code> calls its <code>flush</code>
   * method, and then calls the <code>close</code> method of its underlying output stream.
   *
   * @exception IOException if an I/O error occurs.
   * @see OutputStream#flush()
   */
  @Override
  public void close() throws IOException {
    Set<OutputStream> outputStreams = this.streams;
    for (OutputStream out : outputStreams) {
      try {
        out.flush();
      } catch (IOException ignored) {
      }
      out.close();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("size=").append(this.streams.size());
    return sb.append("}").toString();
  }
}
