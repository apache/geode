/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.io;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;

/**
 * @author Kirk Lund
 * @since 7.0
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
    return this.compositeOutputStream;
  }

  /**
   * @return <tt>true</tt> if this CompositePrintStream did not already contain the specified OutputStream
   */
  public boolean addOutputStream(OutputStream out) {
    return this.compositeOutputStream.addOutputStream(out);
  }
  
  /**
   * @return <tt>true</tt> if this CompositePrintStream contained the specified OutputStream
   */
  public boolean removeOutputStream(OutputStream out) {
    return this.compositeOutputStream.removeOutputStream(out);
  }
  
  /**
   * Returns <tt>true</tt> if this CompositePrintStream contains no OutputStreams.
   *
   * @return <tt>true</tt> if this CompositePrintStream contains no OutputStreams
   */
  public boolean isEmpty() {
    return this.compositeOutputStream.isEmpty();
  }
  
  /**
   * Returns the number of OutputStreams in this CompositePrintStream (its cardinality).
   *
   * @return the number of OutputStreams in this CompositePrintStream (its cardinality)
   */
  public int size() {
    return this.compositeOutputStream.size();
  }
  
  public Iterator<OutputStream> iterator() {
    return this.compositeOutputStream.iterator();
  }
}
