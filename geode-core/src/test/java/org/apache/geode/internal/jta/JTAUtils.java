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
/*
 * JTAUtils.java
 *
 * Created on April 12, 2005, 11:45 AM
 */

package org.apache.geode.internal.jta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;

public class JTAUtils {


  public Region currRegion;
  public Cache cache;
  /** This is used to store each Regions original attributes for reset purposes */
  private HashMap regionDefaultAttrMap = new HashMap();

  public JTAUtils(Cache cache, Region region) {
    this.cache = cache;
    this.currRegion = region;
  }

  public static long start() {
    long beginTime = getCurrentTimeMillis();
    return beginTime;
  }

  public static long stop() {
    long endTime = getCurrentTimeMillis();
    return endTime;
  }

  public static long elapsedTime(long endTime, long beginTime) {
    return endTime - beginTime;
  }

  static long getCurrentTimeMillis() {
    return System.currentTimeMillis();
  }

  /**
   * Calls the corresponding cache APIs to create a sub-region by name 'command' in the current
   * region.
   */
  public void mkrgn(String command) throws Exception {

    try {
      String name = command;
      AttributesFactory fac = new AttributesFactory(this.currRegion.getAttributes());
      Region nr = this.currRegion.createSubregion(name, fac.create());
      regionDefaultAttrMap.put(nr.getFullPath(), fac.create());
    } catch (Exception e) {
      // fail (" unable to make region..." + e.getMessage ());
      throw new Exception(" failed in mkrgn " + command);
    }

  }

  /**
   * Checks whether a region passed in the param exists in the curr region or not. If doesnt exist
   * then calls mkrgn() to create it. Finally makes data member currRegion point to that region.
   */
  public void getRegionFromCache(String region) throws Exception {
    try {
      Region subr = this.currRegion.getSubregion(region);

      if (subr == null) {
        mkrgn(region);
        currRegion = this.currRegion.getSubregion(region);
      } else {
        currRegion = subr;
      }

    } catch (Exception e) {
      // fail (" unable to get sub-region...");
      System.out.println("err: " + e);
      e.printStackTrace();
      throw new Exception(" failed in getRegionFromCache ");
    }

  }

  /**
   * Parses a <code>command</code> and places each of its tokens in a <code>List</code>.
   */
  public boolean parseCommand(String command, List list) {
    // String strTemp = command;
    boolean done = false;
    boolean success = false;
    int space = -1;
    do {
      space = command.indexOf(' ');
      if (space < 0) {
        done = true;
        list.add(command);
        break;
      }
      String str = command.substring(0, space);
      list.add(str);
      command = command.substring(space + 1, command.length());
      success = true;
    } while (!done);
    return success;
  }

  /**
   * Gets the value of the key entry into the current region. Returns the value and prints the pair
   * with help of printEntry() to stdout
   *
   * @see Region#put Modified to return string value
   */

  public String get(String command) throws CacheException {
    String value = null;

    // try {
    String name = command;
    Object valueBytes = this.currRegion.get(name);
    value = printEntry(name, valueBytes);
    // }
    // catch (CacheException e) {
    // //fail (" unable to get value..." + e.getMessage ());
    // throw new CacheExistsException("failed getting region: " + command);
    // }
    return value;
  }

  /**
   * Puts an entry into the current region
   *
   * @see Region#put
   */
  public void put(String command, String val) throws Exception {
    try {
      LinkedList list = new LinkedList();
      // syntax of put from CacheRunner cli help-- prabir
      command = "put " + command + " " + val;
      parseCommand(command, list);
      if (list.size() < 3) {
        System.out.println("Error:put requires a name and a value");
      } else {
        String name = (String) list.get(1);
        String value = (String) list.get(2);
        if (list.size() > 3) {
          String objectType = (String) list.get(3);
          if (objectType.equalsIgnoreCase("int")) {
            this.currRegion.put(name, Integer.valueOf(value));
          } else if (objectType.equalsIgnoreCase("str")) {
            this.currRegion.put(name, value);
          } else {
            System.out.println("Invalid object type specified. Please see help.");
            // fail (" Invalid object type specified !!");
          }
        } else {
          this.currRegion.put(name, value.getBytes());
        }
      }
    } catch (Exception e) {
      // fail (" unable to put..." + e.getMessage ());
      throw new Exception("unable to put: " + e);
    }

  }

  /**
   * Prints the key/value pair for an entry to stdout This method recognizes a subset of all
   * possible object types. Modified to return the value string
   */
  public String printEntry(String key, Object valueBytes) {
    String value = null;

    if (valueBytes != null) {
      if (valueBytes instanceof byte[]) {
        value = new String("byte[]: \"" + new String((byte[]) valueBytes) + "\"");
      } else if (valueBytes instanceof String) {
        value = new String("String: \"" + valueBytes + "\"");
      } else if (valueBytes instanceof Integer) {
        value = new String("Integer: \"" + valueBytes.toString() + "\"");
      } else {
        value = new String("No value in cache.");
      }

      System.out.print("     " + key + " -> " + value);
    }
    return value;
  }

  /**
   * Returns a constant width space to stdout This method is used to print test pass/fail msg after
   * a constant width from the test execution stage
   */

  public static String repeatChar(char character, int repeatCount) {

    StringBuffer stringBuffer = new StringBuffer(repeatCount);
    for (int i = 1; i <= repeatCount; i++) {
      stringBuffer.append(character);
    }
    return stringBuffer.toString();
  }

  /**
   * This method is used to parse the string with delimeter ':'returned by get(). The delimeter is
   * appended by printEntry().
   */
  public String parseGetValue(String str) {

    String returnVal = null;
    if (str == null) {
      // returnVal = str; (redundant assignment)
    }
    if (str.indexOf(':') != -1) {
      String tokens[] = str.split(":");
      returnVal = tokens[1].trim();
    } else if (str.equals("No value in cache.")) { // dont change this string!!
      returnVal = str;
    }
    return returnVal;
  }


  /**
   * This method is used to delete all rows from the timestamped table created by createTable() in
   * CacheUtils class.
   */
  public int deleteRows(String tableName) throws NamingException, SQLException {

    Context ctx = cache.getJNDIContext();
    DataSource da = (DataSource) ctx.lookup("java:/SimpleDataSource"); // doesn't req txn

    Connection conn = da.getConnection();
    Statement stmt = conn.createStatement();
    int rowsDeleted = 0; // assume that rows are always inserted in CacheUtils
    String sql = "";
    sql = "select * from " + tableName;
    ResultSet rs = stmt.executeQuery(sql);
    if (rs.next()) {

      sql = "delete from  " + tableName;
      rowsDeleted = stmt.executeUpdate(sql);
    }

    rs.close();

    stmt.close();
    conn.close();

    return rowsDeleted;
  }


  /**
   * This method is used to return number rows from the timestamped table created by createTable()
   * in CacheUtils class.
   */
  public int getRows(String tableName) throws NamingException, SQLException {

    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");

    String sql = "select * from " + tableName;

    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    ResultSet rs = sm.executeQuery(sql);
    int counter = 0;
    while (rs.next()) {
      counter++;
      // System.out.println("id "+rs.getString(1)+ " name "+rs.getString(2));
    }
    rs.close();
    conn.close();

    return counter;
  }


  /**
   * This method is used to search for pattern which is the PK in the timestamped table created by
   * createTable() in CacheUtils class.
   */
  public boolean checkTableAgainstData(String tableName, String pattern)
      throws NamingException, SQLException {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    boolean found = false;
    String id_str = "";

    String sql = "select * from " + tableName;

    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    ResultSet rs = sm.executeQuery(sql);
    while (rs.next()) {
      System.out.println("id:" + rs.getString(1));
      System.out.println("name:" + rs.getString(2));
      id_str = rs.getString(1);
      if (id_str.equals(pattern)) {
        found = true;
        break;
      } else
        continue;
    }
    rs.close();
    conn.close();
    return found;
  }


} // end of class
