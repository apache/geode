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

package org.apache.geode.internal.shared;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.security.PrivilegedAction;

/**
 * A {@link PrintWriter} that collects its output in a string builder, which can then be used to
 * construct a string. This completely avoids any locking etc.
 * 
 */
public class StringPrintWriter extends PrintWriter {

  private final StringBuilder sb;

  private final static Writer dummyLock = new StringWriter();

  private final String lineSep;

  /**
   * Create a new string writer using the default initial string-buffer size.
   */
  public StringPrintWriter() {
    this(new StringBuilder(), null);
  }

  /**
   * Create a new string writer using the specified string-builder.
   * 
   * @param sb the {@link StringBuilder} to use as the internal buffer
   */
  public StringPrintWriter(StringBuilder sb) {
    this(sb, null);
  }

  /**
   * Create a new string writer using the specified string-builder and line separator.
   * 
   * @param sb the {@link StringBuilder} to use as the internal buffer
   * @param lineSep the line separator to use, or null to use the default from system
   *        "line.separator" property
   */
  public StringPrintWriter(StringBuilder sb, String lineSep) {
    super(dummyLock, false);
    this.sb = sb;
    this.lineSep = lineSep != null ? lineSep
        : java.security.AccessController.doPrivileged(new PrivilegedAction<String>() {
          public String run() {
            return System.getProperty("line.separator");
          }
        });
  }

  @Override
  public void write(int c) {
    this.sb.append((char) c);
  }

  @Override
  public void write(char[] cbuf) {
    this.sb.append(cbuf);
  }

  @Override
  public void write(char[] cbuf, int off, int len) {
    if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    this.sb.append(cbuf, off, len);
  }

  @Override
  public void write(String str) {
    this.sb.append(str);
  }

  @Override
  public void write(String str, int off, int len) {
    this.sb.append(str.substring(off, off + len));
  }

  @Override
  public void print(boolean b) {
    this.sb.append(b);
  }

  @Override
  public void print(int i) {
    this.sb.append(i);
  }

  @Override
  public void print(long l) {
    this.sb.append(l);
  }

  @Override
  public void print(float f) {
    this.sb.append(f);
  }

  @Override
  public void print(double d) {
    this.sb.append(d);
  }

  @Override
  public void print(char[] s) {
    this.sb.append(s);
  }

  @Override
  public void print(String s) {
    this.sb.append(s);
  }

  @Override
  public void print(Object obj) {
    this.sb.append(obj);
  }

  @Override
  public void println() {
    this.sb.append(lineSep);
  }

  @Override
  public void println(boolean b) {
    this.sb.append(b).append(lineSep);
  }

  @Override
  public void println(int i) {
    this.sb.append(i).append(lineSep);
  }

  @Override
  public void println(long l) {
    this.sb.append(l).append(lineSep);
  }

  @Override
  public void println(float f) {
    this.sb.append(f).append(lineSep);
  }

  @Override
  public void println(double d) {
    this.sb.append(d).append(lineSep);
  }

  @Override
  public void println(char[] s) {
    this.sb.append(s).append(lineSep);
  }

  @Override
  public void println(String s) {
    this.sb.append(s).append(lineSep);
  }

  @Override
  public void println(Object obj) {
    this.sb.append(obj).append(lineSep);
  }

  @Override
  public StringPrintWriter append(char c) {
    write(c);
    return this;
  }

  @Override
  public StringPrintWriter append(CharSequence csq) {
    write(csq != null ? csq.toString() : "null");
    return this;
  }

  @Override
  public StringPrintWriter append(CharSequence csq, int start, int end) {
    write(csq != null ? csq.subSequence(start, end).toString() : "null");
    return this;
  }

  /**
   * Return the builder's current value as a string.
   */
  @Override
  public String toString() {
    return this.sb.toString();
  }

  /**
   * Return the string builder itself.
   */
  public StringBuilder getBuilder() {
    return this.sb;
  }

  @Override
  public void flush() {
    // nothing to be done
  }

  @Override
  public void close() {
    // nothing to be done
  }
}
