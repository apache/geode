/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: NumericComparator.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.types;

import java.util.*;


/**
 * Comparator for mixed comparisons between numbers.
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */


class NumericComparator implements Comparator
{
        // all numeric comparators are created equal
  @Override
    public boolean equals(Object obj)
    {
        return obj instanceof NumericComparator;
    }

        // throws ClassCastExcepton if obj1 or obj2 is not a Number
    public int compare(Object obj1, Object obj2)
    {
        Number num1 = (Number)obj1;
        Number num2 = (Number)obj2;
        

            //create as few objects as possible, but
            // use compareTo algorithm in the wrapper classes for float
            // and double so it has the same behavior as index lookups.
            // for integers, the algorithm is the same for wrappers and primitives,
            // so use primitives there.
        if (num1 instanceof Double)
            if (num2 instanceof Double)
                    // this case may happen if one is a Double.TYPE
                    // and the other is Double.class
                return ((Double)num1).compareTo((Double)num2); 
            else
                return ((Double)num1).compareTo(Double.valueOf(num2.doubleValue()));
        else if (num2 instanceof Double)
            return Double.valueOf(num1.doubleValue()).compareTo((Double)num2);

        if (num1 instanceof Float)
            if (num2 instanceof Float)
                return ((Float)num1).compareTo((Float)num2);
            else
                return ((Float)num1).compareTo(new Float(num2.doubleValue()));
        else if (num2 instanceof Float)
            return new Float(num1.doubleValue()).compareTo((Float)num2);


        if (num1 instanceof Long)
            if (num2 instanceof Long)
                return ((Long)num1).compareTo((Long)num2);
            else
            {
                long l1 = num1.longValue();
                long l2 = num2.longValue();
                return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
            }
        else if (num2 instanceof Long)
        {
            long l1 = num1.longValue();
            long l2 = num2.longValue();
            return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
        }

            // ints
        int i1 = num1.intValue();
        int i2 = num2.intValue();
        return i1 == i2 ? 0 : (i1 < i2 ? -1 : 1);
    }
}

            
                                                             
    
