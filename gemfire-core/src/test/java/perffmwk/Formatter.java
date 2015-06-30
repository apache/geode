/*
*========================================================================
* Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

package perffmwk;

import hydra.HydraRuntimeException;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 *  Contains common code used to format reports.
 */
public class Formatter {

  /** The width (in characters) of the report output.
   *
   * @see #center(String, PrintWriter) */
  protected static final int WIDTH = 80;

  protected static final String DIVIDER =
    "================================================================================";
  protected static final String SUBDIVIDER =
    "--------------------------------------------------------------------------------";

  /**
   *  Centers the given string on the <code>PrintWriter</code>
   */
  public static void center(String s, PrintWriter pw) {
    int indent = (WIDTH / 2) - (s.length() / 2);
    for (int i = 0; i < indent; i++) {
      pw.print(" ");
    }
    pw.println(s);
  }
  /**
   *  Returns a collection of decimals based on the given collection but with
   *  the doubles formatted according to the pattern.  String values are ignored.
   */
  public static Vector formatDecimal( Collection c, String pattern ) {
    DecimalFormat f = new DecimalFormat( pattern );
    Vector v = new Vector();
    for ( Iterator i = c.iterator(); i.hasNext(); ) {
      Object o = i.next();
      if ( o instanceof Double ) 
        v.add( f.format( o ) );
      else
        v.add( o );
    }
    return v;
  }
  /**
   *  Returns a collection of doubles based on the given collection but with
   *  the doubles formatted according to the pattern and all entries, both
   *  doubles and string, padded on the left to the length of the longest one.
   */
  public static Vector padLeft( Collection doubles, String pattern ) {
    return padLeft( formatDecimal( doubles, pattern ) );
  }
  /**
   *  Returns a collection of doubles based on the given collection but with
   *  the doubles formatted according to the pattern and all entries, both
   *  doubles and string, padded on the right to the length of the longest one.
   */
  public static Vector padRight( Collection doubles, String pattern ) {
    return padRight( formatDecimal( doubles, pattern ) );
  }
  /**
   *  Returns a collection of strings based on the given collection but with
   *  all strings padded on the left to the length of the longest one.
   */
  public static Vector padLeft( Collection strings ) {
    Vector v = new Vector();
    int length = maxLength( strings );
    for ( Iterator i = strings.iterator(); i.hasNext(); ) {
      String string = (String) i.next();
      v.add( padLeft( string, length ) );
    }
    return v;
  }
  /**
   *  Returns a collection of strings based on the given collection but with
   *  all strings padded on the right to the length of the longest one.
   */
  public static Vector padRight( Collection strings ) {
    Vector v = new Vector();
    int length = maxLength( strings );
    for ( Iterator i = strings.iterator(); i.hasNext(); ) {
      String string = (String) i.next();
      v.add( padRight( string, length ) );
    }
    return v;
  }
  /**
   *  Returns a string that is the given string padded on the left to the given length.
   */
  public static String padLeft( String s, int length ) {
    if ( s.length() > length ) {
      throw new HydraRuntimeException( s + " cannot be padded to length " + length + ", it is too long" );
    }
    String t = "";
    for ( int i = 0; i < length - s.length(); i++ ) {
      t += " ";
    }
    return t + s;
  }
  /**
   *  Returns a string that is the given string padded on the right to the given length.
   */
  public static String padRight( String s, int length ) {
    if ( s.length() > length ) {
      throw new HydraRuntimeException( s + " cannot be padded to length " + length + ", it is too long" );
    }
    String t = new String( s );
    for ( int i = 0; i < length - s.length(); i++ ) {
      t += " ";
    }
    return t;
  }
  /**
   *  Returns the length of the longest string in the collection (0 if the
   *  collection is empty).
   */
  public static int maxLength( Collection strings ) {
    int max = 0;
    for ( Iterator i = strings.iterator(); i.hasNext(); ) {
      String string = (String) i.next();
      max = Math.max( max, string.length() );
    }
    return max;
  }
}
