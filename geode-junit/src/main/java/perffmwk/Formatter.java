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

package perffmwk;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

/**
 * Contains common code used to format reports.
 */
public class Formatter {

  /**
   * The width (in characters) of the report output.
   *
   * @see #center(String, PrintWriter)
   */
  protected static final int WIDTH = 80;

  protected static final String DIVIDER =
      "================================================================================";
  protected static final String SUBDIVIDER =
      "--------------------------------------------------------------------------------";

  /**
   * Centers the given string on the <code>PrintWriter</code>
   */
  public static void center(String s, PrintWriter pw) {
    int indent = (WIDTH / 2) - (s.length() / 2);
    for (int i = 0; i < indent; i++) {
      pw.print(" ");
    }
    pw.println(s);
  }

  /**
   * Returns a collection of decimals based on the given collection but with the doubles formatted
   * according to the pattern. String values are ignored.
   */
  public static Vector formatDecimal(Collection c, String pattern) {
    DecimalFormat f = new DecimalFormat(pattern);
    Vector v = new Vector();
    for (Iterator i = c.iterator(); i.hasNext();) {
      Object o = i.next();
      if (o instanceof Double)
        v.add(f.format(o));
      else
        v.add(o);
    }
    return v;
  }

  /**
   * Returns a collection of doubles based on the given collection but with the doubles formatted
   * according to the pattern and all entries, both doubles and string, padded on the left to the
   * length of the longest one.
   */
  public static Vector padLeft(Collection doubles, String pattern) {
    return padLeft(formatDecimal(doubles, pattern));
  }

  /**
   * Returns a collection of doubles based on the given collection but with the doubles formatted
   * according to the pattern and all entries, both doubles and string, padded on the right to the
   * length of the longest one.
   */
  public static Vector padRight(Collection doubles, String pattern) {
    return padRight(formatDecimal(doubles, pattern));
  }

  /**
   * Returns a collection of strings based on the given collection but with all strings padded on
   * the left to the length of the longest one.
   */
  public static Vector padLeft(Collection strings) {
    Vector v = new Vector();
    int length = maxLength(strings);
    for (Iterator i = strings.iterator(); i.hasNext();) {
      String string = (String) i.next();
      v.add(padLeft(string, length));
    }
    return v;
  }

  /**
   * Returns a collection of strings based on the given collection but with all strings padded on
   * the right to the length of the longest one.
   */
  public static Vector padRight(Collection strings) {
    Vector v = new Vector();
    int length = maxLength(strings);
    for (Iterator i = strings.iterator(); i.hasNext();) {
      String string = (String) i.next();
      v.add(padRight(string, length));
    }
    return v;
  }

  /**
   * Returns a string that is the given string padded on the left to the given length.
   */
  public static String padLeft(String s, int length) {
    if (s.length() > length) {
      throw new RuntimeException(s + " cannot be padded to length " + length + ", it is too long");
    }
    String t = "";
    for (int i = 0; i < length - s.length(); i++) {
      t += " ";
    }
    return t + s;
  }

  /**
   * Returns a string that is the given string padded on the right to the given length.
   */
  public static String padRight(String s, int length) {
    if (s.length() > length) {
      throw new RuntimeException(s + " cannot be padded to length " + length + ", it is too long");
    }
    String t = new String(s);
    for (int i = 0; i < length - s.length(); i++) {
      t += " ";
    }
    return t;
  }

  /**
   * Returns the length of the longest string in the collection (0 if the collection is empty).
   */
  public static int maxLength(Collection strings) {
    int max = 0;
    for (Iterator i = strings.iterator(); i.hasNext();) {
      String string = (String) i.next();
      max = Math.max(max, string.length());
    }
    return max;
  }
}
