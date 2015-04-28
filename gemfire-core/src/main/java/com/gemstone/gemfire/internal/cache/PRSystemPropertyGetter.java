/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Dec 22, 2005
 *
 */
package com.gemstone.gemfire.internal.cache;

/**
 * @author rreja
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public  class PRSystemPropertyGetter
{
  public static int parseInt(String i, int defaultValue)
  {
    if (i == null || i.equals("")) {
      return defaultValue;
    }
    try {
      return (Integer.parseInt(i));
    }
    catch (NumberFormatException nfe) {
      return defaultValue;
    }
  }

  public static long parseLong(String l, long defaultValue)
  {
    if (l == null || l.equals("")) {
      return defaultValue;
    }
    try {
      return (Long.parseLong(l));
    }
    catch (NumberFormatException nfe) {
      return defaultValue;
    }
  }

  public static boolean booleanvalueOf(String s, boolean defaultValue)
  {
    if (s == null) {
      return defaultValue;
    }
    try {
      return (Boolean.valueOf(s).booleanValue());
    }
    catch (NumberFormatException nfe) {
      return defaultValue;
    }

  }
}
