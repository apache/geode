/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.net.InetAddress;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireIOException;
import org.apache.geode.SystemIsRunningException;
import org.apache.geode.UncreatedSystemException;
import org.apache.geode.UnstartedSystemException;

/**
 * Represents the information about the manager that is stored in its SystemAdmin manager VM's main
 * thread.
 * <p>
 * For internal use only.
 *
 *
 */
public class ManagerInfo implements DataSerializable {
  private static final long serialVersionUID = 5792809580026325378L;
  /**
   * The name of the file the locator will create when the system is started. It will be deleted
   * when the system is stopped. It contains a single serialize copy of {@link ManagerInfo}. The
   * file will always be located in the system directory.
   */
  private static final String LOCATOR_INFO_FILE_NAME = ".locator";
  /**
   * The status code constant that means the system is stopped.
   * <p>
   * Current value is <code>0</code>.
   */
  public static final int STOPPED_STATUS_CODE = 0;
  /**
   * The status code constant that means the system is stopping.
   * <p>
   * Current value is <code>1</code>.
   */
  public static final int STOPPING_STATUS_CODE = 1;
  /**
   * The status code constant that means the system was killed.
   * <p>
   * Current value is <code>2</code>.
   */
  public static final int KILLED_STATUS_CODE = 2;
  /**
   * The status code constant that means the system is starting.
   * <p>
   * Current value is <code>3</code>.
   */
  public static final int STARTING_STATUS_CODE = 3;
  /**
   * The status code constant that means the system is started.
   * <p>
   * Current value is <code>4</code>.
   */
  public static final int STARTED_STATUS_CODE = 4;

  public static void setLocatorStarted(File directory, int port, InetAddress bindAddress) {
    ManagerInfo.saveManagerInfo(OSProcess.getId(), STARTED_STATUS_CODE, directory, port,
        bindAddress);
  }

  public static File setLocatorStarting(File directory, int port, InetAddress bindAddress) {
    if (ManagerInfo.isManagerRunning(directory, true)) {
      throw new SystemIsRunningException(String.format("%s %s is already running.",
          new Object[] {"Locator", directory.getPath()}));
    }
    File result = getManagerInfoFile(directory, true);
    ManagerInfo.saveManagerInfo(OSProcess.getId(), STARTING_STATUS_CODE, directory, port,
        bindAddress);
    return result;
  }

  // fix for bug #44059. store the port and address for the locator in the persistent information
  // so we can use "-dir" to stop the locator.
  public static void setLocatorStopping(File directory, int port, InetAddress bindAddress) {
    ManagerInfo.saveManagerInfo(OSProcess.getId(), STOPPING_STATUS_CODE, directory, port,
        bindAddress);
  }

  /**
   * Saves the manager information to the info file in the given <code>directory</code>.
   *
   * @param pid the process id of the manager VM.
   * @param status the status of the manager
   * @param directory the manager's directory.
   * @param port the tcp/ip port for the locator
   * @param bindAddress the tcp/ip address for the locator
   * @throws GemFireIOException if the file could not be written.
   */
  private static void saveManagerInfo(int pid, int status, File directory, int port,
      InetAddress bindAddress) {
    ManagerInfo info = new ManagerInfo(pid, status, port, bindAddress);
    File infoFile = getManagerInfoFile(directory, true);
    try {
      FileOutputStream ostream = new FileOutputStream(infoFile);
      DataOutputStream dos = new DataOutputStream(ostream);
      DataSerializer.writeObject(info, dos);
      ostream.close();
    } catch (IOException io) {
      throw new GemFireIOException(
          String.format("Could not write file %s.", infoFile), io);
    }
  }

  /**
   * Gets the process ID of the manager.
   */
  public int getManagerProcessId() {
    return this.managerPid;
  }

  /**
   * Gets the status of the manager.
   */
  public int getManagerStatus() {
    return this.managerStatus;
  }

  /**
   * get the port number of the manager
   */
  public int getManagerPort() {
    return this.port;
  }

  /**
   * get the bind address of the manager
   */
  public InetAddress getManagerAddress() {
    return this.bindAddress;
  }

  static final String[] statusNames =
      new String[] {"stopped",
          "stopping",
          "killed",
          "starting",
          "running"};

  /**
   * Gets the string representation for the given <code>status</code> int code.
   */
  public static String statusToString(int status) {
    return statusNames[status];
  }

  /**
   * Gets the status code for the given <code>statusName</code>.
   *
   * @throws IllegalArgumentException if an unknown status name is given.
   */
  public static int statusNameToCode(String statusName) {
    for (int i = STOPPED_STATUS_CODE; i <= STARTED_STATUS_CODE; i++) {
      if (statusNames[i].equalsIgnoreCase(statusName)) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        String.format("Unknown statusName %s", statusName));
  }

  public static ManagerInfo loadLocatorInfo(File directory) {
    return loadManagerInfo(directory, true);
  }

  private static ManagerInfo loadManagerInfo(File directory, boolean locator) {
    if (!directory.exists() || !directory.isDirectory()) {
      throw new UncreatedSystemException(
          String.format("%s does not exist or is not a directory.",
              directory.getPath()));
    }
    File infoFile = getManagerInfoFile(directory, locator);
    if (!infoFile.exists()) {
      throw new UnstartedSystemException(String.format("The info file %s does not exist.",
          infoFile.getPath()));
    }
    try {
      FileInputStream fis = new FileInputStream(infoFile);
      if (fis.available() == 0) {
        throw new GemFireIOException(
            String.format(
                "Could not load file %s because the file is empty. Wait for the %s to finish starting.",
                new Object[] {infoFile, (locator ? "locator" : "system")}),
            null);
      }
      DataInputStream dis = new DataInputStream(fis);
      ManagerInfo result = (ManagerInfo) DataSerializer.readObject(dis);
      fis.close();
      return result;
    } catch (IOException io) {
      throw new GemFireIOException(
          String.format("Could not load file %s.", infoFile), io);
    } catch (ClassNotFoundException ex) {
      throw new GemFireIOException(
          String.format("Could not load file %s because a class could not be found.",
              infoFile),
          ex);
    }
  }

  public static File getLocatorInfoFile(File directory) {
    return getManagerInfoFile(directory, true);
  }

  private static File getManagerInfoFile(File directory, boolean locator) {
    if (!locator) {
      throw new IllegalArgumentException(
          "Only locators are supported");
    }
    File res = new File(directory, LOCATOR_INFO_FILE_NAME);
    try {
      res = res.getCanonicalFile();
    } catch (IOException ex) {
      res = res.getAbsoluteFile();
    }
    return res;
  }

  public static String getLocatorStatusCodeString(File directory) {
    return statusToString(getLocatorStatusCode(directory));
  }

  public static int getLocatorStatusCode(File directory) {
    return getManagerStatusCode(directory, true);
  }

  private static int getManagerStatusCode(File directory, boolean locator) {
    boolean interrupted = false;
    try {
      ManagerInfo mi = ManagerInfo.loadManagerInfo(directory, locator);
      if (!PureJavaMode.isPure() && !OSProcess.exists(mi.getManagerProcessId())) {
        return KILLED_STATUS_CODE;
      } else {
        return mi.getManagerStatus();
      }
    } catch (UnstartedSystemException ex) {
      return STOPPED_STATUS_CODE;
    } catch (GemFireIOException ex) {
      // wait a bit and try again in case we caught the manager rewriting
      // its info file
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        interrupted = true;
      }
      try {
        ManagerInfo.loadManagerInfo(directory, locator);
        // all is well so do a recursive call
        return getManagerStatusCode(directory, locator);
      } catch (UnstartedSystemException ignore) {
        return STOPPED_STATUS_CODE;
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static boolean isLocatorStarted(File directory) {
    return isManagerStarted(directory, true);
  }

  private static boolean isManagerStarted(File directory, boolean locator) {
    try {
      ManagerInfo mi = loadManagerInfo(directory, locator);
      int status = mi.getManagerStatus();
      if (status != STARTED_STATUS_CODE) {
        return false;
      }
      // Check to see if manager process exists, assume it is for PureJava
      if (PureJavaMode.isPure() || OSProcess.exists(mi.getManagerProcessId())) {
        return true;
      }
      return false;
    } catch (UnstartedSystemException ignore) {
      return false;
    } catch (GemFireIOException ex) {
      Throwable cause = ex.getCause();
      if (cause == null) {
        // this happens when the file was zero size
        return false;
      } else if (cause instanceof InvalidClassException) {
        // This happens when we have a serialVersionUID mismatch.
        // We don't want to hide this so throw the exception
        throw ex;
      } else {
        return false;
      }
    }
  }

  public static boolean isLocatorRunning(File directory) {
    return isManagerRunning(directory, true);
  }

  private static boolean isManagerRunning(File directory, boolean locator) {
    try {
      ManagerInfo mi = loadManagerInfo(directory, locator);
      int status = mi.getManagerStatus();
      if (status != STARTED_STATUS_CODE && status != STARTING_STATUS_CODE
          && status != STOPPING_STATUS_CODE) {
        return false;
      }
      // Check to see if manager process exists
      if (!PureJavaMode.isPure() && !OSProcess.exists(mi.getManagerProcessId())) {
        return false;
      }
      return true;
    } catch (UnstartedSystemException ignore) {
      return false;
    } catch (GemFireIOException ex) {
      Throwable cause = ex.getCause();
      if (cause == null) {
        // this happens when the file was zero size
        // This indicates the manager is changing its info file
        return true;
      } else if (cause instanceof InvalidClassException) {
        // This happens when we have a serialVersionUID mismatch.
        // We don't want to hide this so throw the exception
        throw ex;
      } else {
        // This indicates the manager is changing its info file
        return true;
      }
    }
  }

  public static boolean isLocatorStopped(File directory) {
    return isManagerStopped(directory, true);
  }

  private static boolean isManagerStopped(File directory, boolean locator) {
    try {
      ManagerInfo mi = loadManagerInfo(directory, locator);
      if (!OSProcess.exists(mi.getManagerProcessId())) {
        return true;
      }
      return false;
    } catch (UnstartedSystemException ignore) {
      return true;
    } catch (GemFireIOException ex) {
      Throwable cause = ex.getCause();
      if (cause == null) {
        // this happens when the file was zero size
        return false;
      } else if (cause instanceof InvalidClassException) {
        // This happens when we have a serialVersionUID mismatch.
        // We don't want to hide this so throw the exception
        throw ex;
      } else {
        return false;
      }
    }
  }

  /**
   * Constructs a manager info instance given the process id of the manager VM.
   *
   * @param port TODO
   * @param bindAddress TODO
   */
  private ManagerInfo(int pid, int status, int port, InetAddress bindAddress) {
    this.managerPid = pid;
    this.managerStatus = status;
    this.port = port;
    this.bindAddress = bindAddress;
  }

  /**
   * Constructor for <code>DataSerializable</code>
   */
  public ManagerInfo() {}

  // instance variables
  private int managerPid;
  private int managerStatus;
  private int port;
  private InetAddress bindAddress;

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.managerPid);
    out.writeInt(this.managerStatus);
    out.writeInt(this.port);
    if (this.bindAddress == null) {
      out.writeByte(0);
    } else {
      byte[] address = this.bindAddress.getAddress();
      out.writeByte(address.length);
      out.write(address, 0, address.length);
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    this.managerPid = in.readInt();
    this.managerStatus = in.readInt();
    this.port = in.readInt();
    byte len = in.readByte();
    if (len > 0) {
      byte[] addr = new byte[len];
      in.readFully(addr);
      this.bindAddress = InetAddress.getByAddress(addr);
    }
  }
}
