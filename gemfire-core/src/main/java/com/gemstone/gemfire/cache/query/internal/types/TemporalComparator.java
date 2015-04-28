/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: TemporalComparator.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.types;

import java.util.*;


/**
 * Comparator for mixed comparisons between instances of
 * java.util.Date, java.sql.Date, java.sql.Time, and java.sql.Timestamp.
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */


class TemporalComparator implements Comparator
{
        // all temporal comparators are created equal
    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof TemporalComparator;
    }

        // throws ClassCastExcepton if obj1 or obj2 is not a java.util.Date or subclass
    public int compare(Object obj1, Object obj2)
    {
        java.util.Date date1 = (java.util.Date)obj1;
        java.util.Date date2 = (java.util.Date)obj2;
        long ms1 = date1.getTime();
        long ms2 = date2.getTime();

            // if we're dealing with Timestamps, then we need to extract milliseconds
            // out of the nanos and then do a compare with the "extra" nanos
        int extraNanos1 = 0;
        int extraNanos2 = 0;
        if (date1 instanceof java.sql.Timestamp)
        {
            int nanos = ((java.sql.Timestamp)date1).getNanos();
            int ms = nanos / 1000000;
            ms1 += ms;
            extraNanos1 = nanos - (ms * 1000000);
        }

        if (date2 instanceof java.sql.Timestamp)
        {
            int nanos = ((java.sql.Timestamp)date2).getNanos();
            int ms = nanos / 1000000;
            ms2 += ms;
            extraNanos2 = nanos - (ms * 1000000);
        }

        if (ms1 != ms2)
            return ms1 < ms2 ? -1 : 1;
        return extraNanos1 == extraNanos2 ? 0 : (extraNanos1 < extraNanos2 ? -1 : 1);
    }
}

            
                                                             
    
