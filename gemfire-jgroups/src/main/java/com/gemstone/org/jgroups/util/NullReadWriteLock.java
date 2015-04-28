/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.oswego.concurrent.ReadWriteLock;
import com.gemstone.org.jgroups.oswego.concurrent.Sync;


/**
 * @author Bela Ban
 * @version $Id: NullReadWriteLock.java,v 1.1 2005/04/08 08:52:44 belaban Exp $
 */
public class NullReadWriteLock implements ReadWriteLock {
    Sync sync=new NullSync();

    public Sync readLock() {
        return sync;
    }

    public Sync writeLock() {
        return sync;
    }
}
