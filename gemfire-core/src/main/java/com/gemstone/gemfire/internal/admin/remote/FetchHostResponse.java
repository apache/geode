/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

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

