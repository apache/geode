/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ThreadPool.java,v 1.9 2005/07/17 11:33:58 chrislott Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.util.GemFireTracer;


// @todo Shrink thread pool if threads are unused after some configurable time.
/**
 * Maintains a set of ReusableThreads. When a thread is to be returned, all existing threads
 * are checked: when one is available, it will be returned. Otherwise, a new thread is created
 * and returned, unless the pool limit is reached, in which case <code>null</code> is returned.
 * Creates threads only as needed, up to the MAX_NUM limit. However, does not shrink the pool
 * when more threads become available than are used.
 * @author Bela Ban
 */
public class ThreadPool {
    int              MAX_NUM=255;
    int              current_index=0;   /// next available thread
    ReusableThread[] pool=null;
    boolean[]        available_threads=null;
    protected static final GemFireTracer log=GemFireTracer.getLog(ThreadPool.class);



    public ThreadPool(int max_num) {
        MAX_NUM=max_num;
        pool=new ReusableThread[MAX_NUM];
        available_threads=new boolean[MAX_NUM];
        for(int i=0; i < pool.length; i++) {
            pool[i]=null;
            available_threads[i]=true;
        }
        if(log.isDebugEnabled()) log.debug("created a pool of " + MAX_NUM + " threads");
    }


    public ReusableThread getThread() {
        ReusableThread retval=null, tmp;

        synchronized(pool) {
            // check whether a previously created thread can be reused
            for(int i=0; i < current_index; i++) {
                tmp=pool[i];
                if(tmp.available()) {
                    return tmp;
                }
            }

            // else create a new thread and add it to the pool
            if(current_index >= MAX_NUM) {
                if(log.isErrorEnabled()) log.error("could not create new thread because " +
                        "pool's max size reached (" + MAX_NUM + ") !");
                return null;
            }
            else {
                retval=new ReusableThread();
                pool[current_index++]=retval;
                return retval;
            }
        }

    }


    public void destroy() {
        deletePool();
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer();

        synchronized(pool) {
            ret.append("ThreadPool: capacity=" + pool.length + ", index=" + current_index + '\n');
            ret.append("Threads are:\n");
            for(int i=0; i < current_index; i++)
                ret.append("[" + i + ": " + pool[i] + "]\n");
        }
        return ret.toString();
    }


    void deletePool() {
        ReusableThread t;
        synchronized(pool) {
            for(int i=0; i < MAX_NUM; i++) {
                t=pool[i];
                if(t != null) {
                    t.stop();
                    pool[i]=null;
                }
            }
            current_index=0;
        }
    }


}
