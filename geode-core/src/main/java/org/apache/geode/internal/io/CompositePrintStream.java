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

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;

/**
 * @since GemFire 7.0
 */
public class CompositePrintStream extends PrintStream {

  private final CompositeOutputStream compositeOutputStream;

  public CompositePrintStream(CompositeOutputStream compositeOutputStream) {
    super(compositeOutputStream, true);
    this.compositeOutputStream = compositeOutputStream;
  }

  public CompositePrintStream(OutputStream... out) {
    this(new CompositeOutputStream(out));
  }

  public CompositeOutputStream getCompositePrintStream() {
    return compositeOutputStream;
  }

  /**
   * @return <tt>true</tt> if this CompositePrintStream did not already contain the specified
   *         OutputStream
   */
  public boolean addOutputStream(OutputStream out) {
    return compositeOutputStream.addOutputStream(out);
  }

  /**
   * @return <tt>true</tt> if this CompositePrintStream contained the specified OutputStream
   */
  public boolean removeOutputStream(OutputStream out) {
    return compositeOutputStream.removeOutputStream(out);
  }

  /**
   * Returns <tt>true</tt> if this CompositePrintStream contains no OutputStreams.
   *
   * @return <tt>true</tt> if this CompositePrintStream contains no OutputStreams
   */
  public boolean isEmpty() {
    return compositeOutputStream.isEmpty();
  }

  /**
   * Returns the number of OutputStreams in this CompositePrintStream (its cardinality).
   *
   * @return the number of OutputStreams in this CompositePrintStream (its cardinality)
   */
  public int size() {
    return compositeOutputStream.size();
  }

  public Iterator<OutputStream> iterator() {
    return compositeOutputStream.iterator();
  }
}
