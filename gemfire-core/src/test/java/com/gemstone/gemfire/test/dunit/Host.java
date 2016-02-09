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
package com.gemstone.gemfire.test.dunit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.gemstone.gemfire.test.dunit.standalone.RemoteDUnitVMIF;

/**
 * <P>This class represents a host on which a remote method may be
 * invoked.  It provides access to the VMs and GemFire systems that
 * run on that host.</P>
 *
 * <P>Additionally, it provides access to the Java RMI registry that
 * runs on the host.  By default, an RMI registry is only started on
 * the host on which Hydra's Master VM runs.  RMI registries may be
 * started on other hosts via additional Hydra configuration.</P>
 *
 * @author David Whitlock
 */
@SuppressWarnings("serial")
public abstract class Host implements Serializable {

  /** The available hosts */
  protected static List hosts = new ArrayList();

  private static VM locator;

  /** Indicates an unstarted RMI registry */
  protected static int NO_REGISTRY = -1;

  ////////////////////  Instance Fields  ////////////////////

  /** The name of this host machine */
  private String hostName;

  /** The VMs that run on this host */
  private List vms;

  /** The GemFire systems that are available on this host */
  private List systems;
  
  /** Key is system name, value is GemFireSystem instance */
  private HashMap systemNames;
  
  ////////////////////  Static Methods  /////////////////////

  /**
   * Returns the number of known hosts
   */
  public static int getHostCount() {
    return hosts.size();
  }

  /**
   * Makes note of a new <code>Host</code>
   */
  protected static void addHost(Host host) {
    hosts.add(host);
  }

  /**
   * Returns a given host
   *
   * @param n
   *        A zero-based identifier of the host
   *
   * @throws IllegalArgumentException
   *         <code>n</code> is more than the number of hosts
   */
  public static Host getHost(int n) {
    int size = hosts.size();
    if (n >= size) {
      String s = "Cannot request host " + n + ".  There are only " +
        size + " hosts.";
      throw new IllegalArgumentException(s);

    } else {
      return (Host) hosts.get(n);
    }
  }

  /////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>Host</code> with the given name
   */
  protected Host(String hostName) {
    if (hostName == null) {
      String s = "Cannot create a Host with a null name";
      throw new NullPointerException(s);
    }

    this.hostName = hostName;
    this.vms = new ArrayList();
    this.systems = new ArrayList();
    this.systemNames = new HashMap();
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the machine name of this host
   */
  public String getHostName() {
    return this.hostName;
  }

  /**
   * Returns the number of VMs that run on this host
   */
  public int getVMCount() {
    return this.vms.size();
  }

  /**
   * Returns a VM that runs on this host
   *
   * @param n
   *        A zero-based identifier of the VM
   *
   * @throws IllegalArgumentException
   *         <code>n</code> is more than the number of VMs
   */
  public VM getVM(int n) {
    int size = vms.size();
    if (n >= size) {
      String s = "Cannot request VM " + n + ".  There are only " +
        size + " VMs on " + this;
      throw new IllegalArgumentException(s);

    } else {
      return (VM) vms.get(n);
    }
  }

  /**
   * Adds a VM to this <code>Host</code> with the given process id and client record.
   */
  protected void addVM(int pid, RemoteDUnitVMIF client) {
    VM vm = new VM(this, pid, client);
    this.vms.add(vm);
  }

  public static VM getLocator() {
    return locator;
  }
  
  private static void setLocator(VM l) {
    locator = l;
  }
  
  protected void addLocator(int pid, RemoteDUnitVMIF client) {
    setLocator(new VM(this, pid, client));
  }

  /**
   * Returns the number of GemFire systems that run on this host
   */
  public int getSystemCount() {
    return this.systems.size();
  }

  ////////////////////  Utility Methods  ////////////////////

  public String toString() {
    StringBuffer sb = new StringBuffer("Host ");
    sb.append(this.getHostName());
    sb.append(" with ");
    sb.append(getVMCount());
    sb.append(" VMs");
    return sb.toString();
  }

  /**
   * Two <code>Host</code>s are considered equal if they have the same
   * name.
   */
  public boolean equals(Object o) {
    if (o instanceof Host) {
      return ((Host) o).getHostName().equals(this.getHostName());

    } else {
      return false;
    }
  }

  /**
   * A <code>Host</code>'s hash code is based on the hash code of its
   * name. 
   */
  public int hashCode() {
    return this.getHostName().hashCode();
  }

}
