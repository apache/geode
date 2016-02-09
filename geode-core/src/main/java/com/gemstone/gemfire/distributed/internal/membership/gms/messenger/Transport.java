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
import org.jgroups.util.AsciiString;
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
        messenger.handleJGroupsIOException(e, dest);
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
   * copied from JGroups to perform Geode-specific error handling when there
   * is a network partition
   */
  @Override
  protected void doSend(AsciiString cluster_name, byte[] buf, int offset, int length, Address dest) throws Exception {
    try {
      super.doSend(cluster_name, buf, offset, length, dest);
    } catch(SocketException sock_ex) {
      if (!this.sock.isClosed() && !stack.getChannel().isClosed()) {
        log.error("Exception caught while sending message", sock_ex);
      }
    } catch (IOException e) {
      if (messenger != null
          /*&& e.getMessage().contains("Operation not permitted")*/) { // this is the english Oracle JDK exception condition we really want to catch
        messenger.handleJGroupsIOException(e, dest);
      }
    } catch(Throwable e) {
        log.error("Exception caught while sending message", e);
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

  // overridden to implement AvailablePort response
  @Override
  public void receive(Address sender, byte[] data, int offset, int length) {
    if(data == null) return;

    // drop message from self; it has already been looped back up (https://issues.jboss.org/browse/JGRP-1765)
    if(local_physical_addr != null && local_physical_addr.equals(sender))
        return;

    if (length-offset == 4
        && data[offset] == 'p'
        && data[offset+1] == 'i'
        && data[offset+2] == 'n'
        && data[offset+3] == 'g') {
      // AvailablePort check
      data[offset+1] = 'o';
      try {
        sendToSingleMember(sender, data, offset, length);
      } catch (Exception e) {
        log.fatal("Unable to respond to available-port check", e);
      }
      return;
    }

    super.receive(sender,  data,  offset,  length);
  }

}
