/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.persistence.logging;

/**
 * A level measures the importance of a entry in a log file.
 *
 * The priorty of level from highest to lowest is:
 * <OL>
 * <LI>ALL</LI>
 * <LI>SEVERE</LI>
 * <LI>WARNING</LI>
 * <LI>INFO</LI>
 * <LI>CONFIG</LI>
 * <LI>FINE</LI>
 * <LI>FINER</LI>
 * <LI>FINEST</LI>
 * <LI>OFF</LI>
 * </OL>
 */
public class Level {

  public static final Level OFF = new Level("OFF", 4);
  public static final Level SEVERE = new Level("SEVERE", 3);
  public static final Level WARNING = new Level("WARNING", 2);
  public static final Level INFO = new Level("INFO", 1);
  public static final Level CONFIG = new Level("CONFIG", 0);
  public static final Level FINE = new Level("FINE", -1);
  public static final Level FINER = new Level("FINER", -2);
  public static final Level FINEST = new Level("FINEST", -3);
  public static final Level ALL = new Level("ALL", -4);

  private String name;
  private int value;

  /**
   * Creates a new <code>Level</code> with a given name and integer
   * value.
   */
  protected Level(String name, int value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Creates a new <code>Level</code> from a string.  The string
   * should be something like "FINER" or "42".
   */
  public static Level parse(String name) {
    if(name.equalsIgnoreCase("OFF")) {
      return(OFF);

    } else if(name.equalsIgnoreCase("SEVERE")) {
      return(SEVERE);

    } else if(name.equalsIgnoreCase("WARNING")) {
      return(WARNING);

    } else if(name.equalsIgnoreCase("INFO")) {
      return(INFO);

    } else if(name.equalsIgnoreCase("CONFIG")) {
      return(CONFIG);

    } else if(name.equalsIgnoreCase("FINE")) {
      return(FINE);

    } else if(name.equalsIgnoreCase("FINER")) {
      return(FINER);

    } else if(name.equalsIgnoreCase("FINEST")) {
      return(FINEST);

    } else if(name.equalsIgnoreCase("ALL")) {
      return(ALL);
    }

    try {
      return(new Level(name, Integer.parseInt(name)));

    } catch(NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid level: " + name);
    }
  }

  /**
   * Returns the integer value for this level
   */
  public int intValue() {
    return(this.value);
  }

  /**
   * Returns a textual representation of this level
   */
  public String toString() {
    return("Level " + this.name + " (" + this.value + ")");
  }

  /**
   * Two levels are equal if they have the same integer value
   */
  public boolean equals(Object o) {
    if(o instanceof Level) {
      Level l = (Level) o;
      if(l.value == this.value) {
        return(true);
      } 
    }

    return(false);
  }

}
