/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.oswego.concurrent.Sync;

/**
 * @author Bela Ban
 * @version $Id: NullSync.java,v 1.1 2005/04/08 08:52:44 belaban Exp $
 */
public class NullSync implements Sync {

    public void acquire() throws InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    }

    public boolean attempt(long l) throws InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        return true;
    }

    public void release() {
    }
}
