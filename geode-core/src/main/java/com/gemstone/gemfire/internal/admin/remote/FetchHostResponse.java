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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * get its current {@link com.gemstone.gemfire.internal.Config}
 */
public final class FetchHostResponse extends AdminResponse {
  private static final Logger logger = LogService.getLogger();
  
  // instance variables
  
  InetAddress host;
  File gemfireDir;
  File workingDir;
  long birthDate;
  boolean isDedicatedCacheServer = false;
  
  /** The connection/system name (not guaranteed to be unique) */
  String name;
  
  /**
   * Returns a <code>FetchHostResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of this vm's local host.
   */
  public static FetchHostResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    FetchHostResponse m = new FetchHostResponse();
    m.setRecipient(recipient);
    try {
      InetAddress host = null;
      String bindAddress = dm.getConfig().getBindAddress();
      try {
        if (bindAddress != null && 
            !bindAddress.equals(DistributionConfig.DEFAULT_BIND_ADDRESS)) {
          host = InetAddress.getByName(bindAddress);
        }
      } catch (UnknownHostException uhe) {
        // handled in the finally block
      } finally {
        if ( host == null) {
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
        throw new IllegalStateException(LocalizedStrings.FetchHostResponse_COULD_NOT_FIND_GEMFIREJAR.toLocalizedString());
      }
      String path = url.getPath();
      if (path.startsWith("file:")) {
        path = path.substring("file:".length());
      }

      File gemfireJar = new File(path);
      File lib = gemfireJar.getParentFile();
      File product = lib.getParentFile();
      m.gemfireDir = product.getCanonicalFile();//may thro' IOException if url is not in a proper format
    } catch (Exception ex) {
      if (dm != null && dm.getCancelCriterion().cancelInProgress() == null) {
        logger.debug(ex.getMessage(), ex);
      }
      m.name = m.name != null ? m.name : DistributionConfig.DEFAULT_NAME;
      m.host = m.host != null ? m.host : null;
      m.gemfireDir = m.gemfireDir != null ? m.gemfireDir : new File("");
      m.workingDir = m.workingDir != null ? m.workingDir : new File(System.getProperty("user.dir")).getAbsoluteFile();
    }

    return m;
  }

  // instance methods
  public InetAddress getHost() {
    return this.host;
  }

  public File getGemFireDir() {
    return this.gemfireDir;
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
    DataSerializer.writeObject(this.gemfireDir, out);
    DataSerializer.writeObject(this.workingDir, out);
    out.writeLong(this.birthDate);
    out.writeBoolean(this.isDedicatedCacheServer);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.name = DataSerializer.readString(in);
    this.host = (InetAddress)DataSerializer.readObject(in);
    this.gemfireDir = (File)DataSerializer.readObject(in);
    this.workingDir = (File)DataSerializer.readObject(in);
    this.birthDate = in.readLong();
    this.isDedicatedCacheServer = in.readBoolean();
  }

  @Override  
  public String toString() {
    return LocalizedStrings.FetchHostResponse_FETCHHOSTRESPONSE_FOR_0_HOST_1.toLocalizedString(new Object[] {this.getRecipient(), this.host});
  }
}

