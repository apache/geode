/*=========================================================================
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export.xml;


public class TypeConversion {

  public static String getXmlType(Class type) {
    if(java.lang.Number.class.isAssignableFrom(type) ||
       type.equals(short.class) ||
       type.equals(int.class) ||
       type.equals(long.class) ||
       type.equals(float.class) ||
       type.equals(double.class)) {
         return "xsd:decimal";
    }

    if(java.lang.CharSequence.class.isAssignableFrom(type)) {
      return "xsd:string";
    }

    if(java.lang.Byte.class.isAssignableFrom(type) ||
       type.equals(byte.class)
    ) {
      return "xsd:byte";
    }

    if(java.lang.Boolean.class.isAssignableFrom(type)
       || type.equals(boolean.class)) {
      return "xsd:boolean";
    }

//    if(java.util.Date.class.isAssignableFrom(type)) {
//      return "xsd:date";
//    }

    //If the type is not known, just print the string representation.
    return "xsd:string";
  }

  public static boolean isOptional(Class type) {
    return !type.isPrimitive();
  }

}
