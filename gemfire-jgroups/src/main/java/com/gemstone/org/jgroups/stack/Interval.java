/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Interval.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package com.gemstone.org.jgroups.stack;


/**
 * Manages retransmission timeouts. Always returns the next timeout, until the last timeout in the
 * array is reached. Returns the last timeout from then on, until reset() is called.
 * @author John Giorgiadis
 * @author Bela Ban
 */
public class Interval {
    private int    next=0;
    private long[] interval=null;

    public Interval(long[] interval) {
	if (interval.length == 0)
	    throw new IllegalArgumentException("Interval()");
	this.interval=interval;
    }

    public long first() { return interval[0]; }
    
    /** @return the next interval */
    public synchronized long next() {
	if (next >= interval.length)
	    return(interval[interval.length-1]);
	else
	    return(interval[next++]);
    }
    
    public long[] getInterval() { return interval; }

    public synchronized void reset() { next = 0; }
}

