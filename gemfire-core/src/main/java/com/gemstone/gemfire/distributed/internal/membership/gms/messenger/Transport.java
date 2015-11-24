/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.SocketException;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.UDP;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.LazyThreadFactory;
import org.jgroups.util.Util;

public class Transport extends UDP {

  /**
   * This is the initial part of the name of all JGroups threads that deliver messages
   */
  public static final String THREAD_POOL_NAME_PREFIX = "Geode UDP";
  
  private JGroupsMessenger messenger;
  
  public void setMessenger(JGroupsMessenger m) {
    messenger = m;
  }
  
  /*
   * (non-Javadoc)
   * Copied from JGroups 3.6.6 UDP and modified to suppress
   * stack traces when unable to set the ttl due to the TCP implementation
   * not supporting the setting, and to only set ttl when multicast is
   * going to be used.
   * 
   * @see org.jgroups.protocols.UDP#setTimeToLive(int)
   */
  @Override
  protected void setTimeToLive(int ttl) {
    if (ip_mcast) {
      if(getImpl != null && setTimeToLive != null) {
        try {
            Object impl=getImpl.invoke(sock);
            setTimeToLive.invoke(impl, ttl);
        }
        catch(InvocationTargetException e) {
          log.info("Unable to set ip_ttl - TCP/IP implementation does not support this setting");
        }
        catch(Exception e) {
            log.error("failed setting ip_ttl", e);
        }
      } else {
        log.warn("ip_ttl %d could not be set in the datagram socket; ttl will default to 1 (getImpl=%s, " +
                   "setTimeToLive=%s)", ttl, getImpl, setTimeToLive);
      }
    }
  }

  /*
   * (non-Javadoc)
   * copied from JGroups to perform Geode-specific error handling when there
   * is a network partition
   * @see org.jgroups.protocols.TP#_send(org.jgroups.Message, org.jgroups.Address)
   */
  @Override
  protected void _send(Message msg, Address dest) {
    try {
        send(msg, dest);
    }
    catch(InterruptedIOException iex) {
    }
    catch(InterruptedException interruptedEx) {
        Thread.currentThread().interrupt(); // let someone else handle the interrupt
    }
    catch(SocketException e) {
      if (!this.sock.isClosed() && !stack.getChannel().isClosed()) {
        log.error("Exception caught while sending message", e);
      }
//        log.trace(Util.getMessage("SendFailure"),
//                  local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
    }
    catch (IOException e) {
      if (messenger != null
          /*&& e.getMessage().contains("Operation not permitted")*/) { // this is the english Oracle JDK exception condition we really want to catch
        messenger.handleJGroupsIOException(e, msg, dest);
      }
    }
    catch(Throwable e) {
        log.error("Exception caught while sending message", e);
//        Util.getMessage("SendFailure"),
//                  local_addr, (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
    }
}
    
  /*
   * (non-Javadoc)
   * JGroups does not currently (3.6.6) allow you to specify that
   * threads should be daemon, so we override the init() method here
   * and create the factories before initializing UDP
   * @see org.jgroups.protocols.UDP#init()
   */
  @Override
  public void init() throws Exception {
    global_thread_factory=new DefaultThreadFactory("Geode ", true);
    timer_thread_factory=new LazyThreadFactory(THREAD_POOL_NAME_PREFIX + " Timer", true, true);
    default_thread_factory=new DefaultThreadFactory(THREAD_POOL_NAME_PREFIX + " Incoming", true, true);
    oob_thread_factory=new DefaultThreadFactory(THREAD_POOL_NAME_PREFIX + " OOB", true, true);
    internal_thread_factory=new DefaultThreadFactory(THREAD_POOL_NAME_PREFIX + " INT", true, true);
    super.init();
  }

  /*
   * (non-Javadoc)
   * @see org.jgroups.protocols.UDP#stop()
   * JGroups is not terminating its timer.  I contacted the jgroups-users
   * email list about this.
   */
  @Override
  public void stop() {
    super.stop();
    if (!getTimer().isShutdown()) {
      getTimer().stop();
    }
  }


}
