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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.CacheServerLauncher;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;


/**
 * A message that is sent to a particular distribution manager to get its current
 * {@link org.apache.geode.internal.Config}
 */
public class FetchHostResponse extends AdminResponse {
  private static final Logger logger = LogService.getLogger();

  // instance variables

  InetAddress host;
  File geodeHomeDir;
  File workingDir;
  long birthDate;
  boolean isDedicatedCacheServer = false;

  /** The connection/system name (not guaranteed to be unique) */
  String name;

  /**
   * Returns a <code>FetchHostResponse</code> that will be returned to the specified recipient. The
   * message will contains a copy of this vm's local host.
   */
  public static FetchHostResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    FetchHostResponse m = new FetchHostResponse();
    m.setRecipient(recipient);
    try {
      InetAddress host = null;
      String bindAddress = dm.getConfig().getBindAddress();
      try {
        if (bindAddress != null && !bindAddress.equals(DistributionConfig.DEFAULT_BIND_ADDRESS)) {
          host = InetAddress.getByName(bindAddress);
        }
      } catch (UnknownHostException uhe) {
        // handled in the finally block
      } finally {
        if (host == null) {
          host = SocketCreator.getLocalHost();
        }
      }
      m.host = host;
      m.isDedicatedCacheServer = CacheServerLauncher.isDedicatedCacheServer;

      DistributionConfig config = dm.getSystem().getConfig();
      m.name = config.getName();

      m.workingDir = new File(System.getProperty("user.dir")).getAbsoluteFile();

      URL url = GemFireVersion.getJarURL();
      if (url == null) {
        throw new IllegalStateException(
            "Could not find gemfire.jar.");
      }
      String path = url.getPath();
      if (path.startsWith("file:")) {
        path = path.substring("file:".length());
      }

      File gemfireJar = new File(path);
      File lib = gemfireJar.getParentFile();
      File product = lib.getParentFile();
      m.geodeHomeDir = product.getCanonicalFile();// may thro' IOException if url is not in a proper
      // format
    } catch (Exception ex) {
      if (dm != null && !dm.getCancelCriterion().isCancelInProgress()) {
        logger.debug(ex.getMessage(), ex);
      }
      m.name = m.name != null ? m.name : DistributionConfig.DEFAULT_NAME;
      m.host = m.host != null ? m.host : null;
      m.geodeHomeDir = m.geodeHomeDir != null ? m.geodeHomeDir : new File("");
      m.workingDir = m.workingDir != null ? m.workingDir
          : new File(System.getProperty("user.dir")).getAbsoluteFile();
    }

    return m;
  }

  // instance methods
  public InetAddress getHost() {
    return this.host;
  }

  public File getGeodeHomeDir() {
    return this.geodeHomeDir;
  }

  public File getWorkingDirectory() {
    return this.workingDir;
  }

  public long getBirthDate() {
    return this.birthDate;
  }

  public String getName() {
    return this.name;
  }

  public boolean isDedicatedCacheServer() {
    return this.isDedicatedCacheServer;
  }

  public int getDSFID() {
    return FETCH_HOST_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeObject(this.host, out);
    DataSerializer.writeObject(this.geodeHomeDir, out);
    DataSerializer.writeObject(this.workingDir, out);
    out.writeLong(this.birthDate);
    out.writeBoolean(this.isDedicatedCacheServer);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.name = DataSerializer.readString(in);
    this.host = (InetAddress) DataSerializer.readObject(in);
    this.geodeHomeDir = (File) DataSerializer.readObject(in);
    this.workingDir = (File) DataSerializer.readObject(in);
    this.birthDate = in.readLong();
    this.isDedicatedCacheServer = in.readBoolean();
  }

  @Override
  public String toString() {
    return String.format("FetchHostResponse for %s host=%s",
        new Object[] {this.getRecipient(), this.host});
  }
}
