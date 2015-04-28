/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: ReaderPreferenceReadWriteLock.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/** 
 * A ReadWriteLock that prefers waiting readers over
 * waiting writers when there is contention. The range of applicability
 * of this class is very limited. In the majority of situations,
 * writer preference locks provide more reasonable semantics.
 * 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class ReaderPreferenceReadWriteLock extends WriterPreferenceReadWriteLock {
  @Override // GemStoneAddition
  protected boolean allowReader() {
    return activeWriter_ == null;
  }
}

