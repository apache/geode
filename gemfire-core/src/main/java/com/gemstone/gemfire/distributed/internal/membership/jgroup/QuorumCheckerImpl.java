/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal.membership.jgroup;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.membership.QuorumChecker;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.stack.IpAddress;

/*
 * Implementation of QuorumChecker interface using JGroups artifacts
 * @author Bruce Schuchardt
 * @since 8.1 11/2014
 */
public class QuorumCheckerImpl implements QuorumChecker {
  private static final Logger logger = LogService.getLogger();
  
  /**
   * the last view before a Forced Disconnect
   */
  private View lastView;
  /**
   * The old system's membership socket
   */
  private volatile DatagramSocket sock;
  /**
   * A boolean to stop the pingResponder thread
   */
  private AtomicBoolean stopper = new AtomicBoolean();
  /**
   * The thread that responds to ping requests from other members
   */
  private Thread pingResponder;
  /**
   * The jgroups network partition threshold percentage
   */
  private int partitionThreshold;
  
  /**
   * ping-pong responses received
   */
  private Set<IpAddress> receivedAcks;
  
  /**
   * map converting from SocketAddresses to member IDs
   */
  private Map<SocketAddress, IpAddress> addressConversionMap;
  
  /**
   * Whether the quorum checker is currently suspended
   */
  private volatile boolean suspended;
  
  /**
   * Whether a quorum has been reached
   * guardedby this
   */
  private boolean quorumAchieved;
  
  
  @Override
  public void suspend() {
    this.suspended = true;
  }
  
  @Override
  public void resume() {
    this.suspended = false;
  }
  

  @Override
  public synchronized boolean checkForQuorum(long timeout)
    throws InterruptedException {
    
    if (this.quorumAchieved) {
      return true;
    }
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (this.sock == null || this.sock.isClosed()) {
      if (isDebugEnabled) {
        logger.debug("quorum check: UDP socket is closed.  Unable to perform a quorum check");
      }
      return false;
    }
    
    boolean wasSuspended = this.suspended;
    if (this.suspended) {
      this.suspended = false;
    }
    
    
    byte[] buffer = new byte[] { 'p', 'i', 'n', 'g' };
    
    
    if (isDebugEnabled) {
      logger.debug("beginning quorum check with {}", this);
    }
    try {
      // send a ping message to each member and read pong responses
      Vector<IpAddress> members = this.lastView.getMembers();
      for (int i=0; i<members.size(); i++) {
        IpAddress addr = members.get(i);
        if (!receivedAcks.contains(addr)) {
          SocketAddress sockaddr = new InetSocketAddress(addr.getIpAddress(), addr.getPort());
          if (isDebugEnabled) {
            logger.debug("quorum check: sending request to {}", addr);
          }
          try {
            DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length, sockaddr);
            this.sock.send(packet);
          } catch (IOException io) {
            // continue to the next member
          }
        }
      }
      
      
      long endTime = System.currentTimeMillis() + timeout;
      for ( ;; ) {
        long time = System.currentTimeMillis();
        long remaining = (endTime - time);
        if (remaining <= 0) {
          if (isDebugEnabled) {
            logger.debug("quorum check: timeout waiting for responses.  {} responses received", receivedAcks.size());
          }
          break;
        }
        if (isDebugEnabled) {
          logger.debug("quorum check: waiting up to {}ms to receive a quorum of responses", remaining);
        }
        Thread.sleep(500);
        if (receivedAcks.size() == members.size()) {
          // we've heard from everyone now so we've got a quorum
          if (isDebugEnabled) {
            logger.debug("quorum check: received responses from all members that were in the old distributed system");
          }
          this.quorumAchieved = true;
          return true;
        }
      }
      
      // quorum check
      int weight = GMS.getWeight(this.lastView.getMembers(), this.lastView.getLeadMember());
      int ackedWeight = GMS.getWeight(receivedAcks, this.lastView.getLeadMember());
      int lossThreshold = (int)Math.round((weight * this.partitionThreshold) / 100.0); 
      if (isDebugEnabled) {
        logger.debug("quorum check: contacted {} processes with {} member weight units.  Threshold for a quorum is {}", receivedAcks.size(), ackedWeight, lossThreshold);
      }
      this.quorumAchieved = (ackedWeight >= lossThreshold);
      return this.quorumAchieved;

    } finally {
      if (wasSuspended) {
        this.suspended = true;
      }
    }
  }


  @Override
  public Object getMembershipInfo() {
    if (this.sock == null || this.sock.isClosed()) {
      return null;
    }
    return this.sock;
  }
  
  /**
   * Create a new QuorumCheckerImpl.  It must be initialized with initialize() before
   * it can be used for quorum checks
   */
  protected QuorumCheckerImpl(View jgView, int partitionThreshold, DatagramSocket jgSock) {
    this.sock = jgSock;
    this.lastView = jgView;
    this.partitionThreshold = partitionThreshold;
  }

  
  protected void initialize() {
    if (this.sock == null  ||  this.sock.isClosed()) {
      return;
    }
    receivedAcks = new ConcurrentHashSet<IpAddress>(this.lastView.size());
    addressConversionMap = new ConcurrentHashMap<SocketAddress, IpAddress>(this.lastView.size());
    Vector<IpAddress> members = this.lastView.getMembers();
    for (int i=0; i<members.size(); i++) {
      IpAddress addr = members.get(i);
      SocketAddress sockaddr = new InetSocketAddress(addr.getIpAddress(), addr.getPort());
      addressConversionMap.put(sockaddr, addr);
    }
    startPingResponder();
  }
  
  
  // start a background thread to respond to "ping" requests
  private void startPingResponder() {
    this.stopper.set(false);
    this.pingResponder = new Thread("GemFire Auto-reconnect responder") {
      public void run() {
        byte[] pongBuffer = new byte[] {'p', 'o', 'n', 'g'};
        byte[] buffer = new byte[100];

        while (!stopper.get()) {
          DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
          try {
            boolean sleep = QuorumCheckerImpl.this.suspended;
            if (sleep) {
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                return;
              }
              continue;
            }
            sock.receive(packet);
            SocketAddress senderSockAddr = packet.getSocketAddress();
            logger.info("received {} bytes from {}", packet.getLength(), senderSockAddr);
            if (packet.getLength() == 4) {
              if (buffer[0] == 'p' && buffer[1] == 'i' && buffer[2] == 'n' && buffer[3] == 'g') {
                logger.info("received ping-pong request from {} - sending response", senderSockAddr);
                DatagramPacket response = new DatagramPacket(pongBuffer, pongBuffer.length, senderSockAddr);
                sock.send(response);
              } else if (buffer[0] == 'p' && buffer[1] == 'o' && buffer[2] == 'n' && buffer[3] == 'g') {
                pongReceived(senderSockAddr);
              }
            }
          } catch (IOException e) {
            try {
              // if the network is down sleep a bit to keep from running hot
              Thread.sleep(500);
            } catch (InterruptedException ie) {
              return;
            }
          }
        }
      }
    };
    this.pingResponder.setDaemon(true);
    this.pingResponder.start();
  }
  
  public void pongReceived(SocketAddress senderSockAddr) {
    logger.info("received ping-pong response from {}", senderSockAddr);
    
    IpAddress memberAddr = addressConversionMap.get(senderSockAddr);
    if (memberAddr != null) {
      logger.info("quorum check: mapped address to member ID {}", memberAddr);
      receivedAcks.add(memberAddr);
    }
  }
  
  protected void teardown() {
    if (this.sock != null) {
      stopper.set(true);
    }
  }
  
  @Override
  public String toString() {
    if (this.sock != null) {
      return "QuorumChecker(port="+this.sock.getLocalPort()+"; view="+this.lastView+")";
    } else {
      return "QuorumChecker(disabled; view="+this.lastView+")";
    }
  }

}
