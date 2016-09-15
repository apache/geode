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
package org.apache.geode.internal.admin.remote;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import java.io.*;
import java.util.*;

/**
 * A message that is sent in response to a {@link
 * BridgeServerResponse}.  It perform an operation on a bridge server
 * and returns the result to the sender.
 *
 * @since GemFire 4.0
 */
public final class BridgeServerResponse extends AdminResponse {

  /** Information about the bridge server that was operated on */
  private RemoteBridgeServer bridgeInfo;

  /** An exception thrown while performing the operation */
  private Exception exception;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Creates a <code>BridgeServerResponse</code> in response to the
   * given request.
   */
  static BridgeServerResponse create(DistributionManager dm,
                                     BridgeServerRequest request) {
    BridgeServerResponse m = new BridgeServerResponse();
    m.setRecipient(request.getSender());

    try {
      GemFireCacheImpl cache =
        (GemFireCacheImpl) CacheFactory.getInstanceCloseOk(dm.getSystem());

      if (request.getCacheId() != System.identityHashCode(cache)) {
        m.bridgeInfo = null;

      } else {
        int operation = request.getOperation();
        switch (operation) {
        case BridgeServerRequest.ADD_OPERATION: {
          CacheServerImpl bridge =
            (CacheServerImpl) cache.addCacheServer();
          m.bridgeInfo = new RemoteBridgeServer(bridge);
          break;
        }

        case BridgeServerRequest.INFO_OPERATION: {
          int id = request.getBridgeId();
          // Note that since this is only an informational request
          // it is not necessary to synchronize on allBridgeServersLock
          for (Iterator iter = cache.getCacheServers().iterator();
               iter.hasNext(); ) {
            CacheServerImpl bridge = (CacheServerImpl) iter.next();
            if (System.identityHashCode(bridge) == id) {
              m.bridgeInfo = new RemoteBridgeServer(bridge);
              break;

            } else {
              m.bridgeInfo = null;
            }
          }
          break;
        }

        case BridgeServerRequest.START_OPERATION: {
          RemoteBridgeServer config = request.getBridgeInfo();
          for (Iterator iter = cache.getCacheServers().iterator();
               iter.hasNext(); ) {
            CacheServerImpl bridge = (CacheServerImpl) iter.next();
            if (System.identityHashCode(bridge) == config.getId()) {
              bridge.configureFrom(config);
              bridge.start();
              m.bridgeInfo = new RemoteBridgeServer(bridge);
              break;

            } else {
              m.bridgeInfo = null;
            }
          }
          break;
        }

        case BridgeServerRequest.STOP_OPERATION: {
          RemoteBridgeServer config = request.getBridgeInfo();
          for (Iterator iter = cache.getCacheServers().iterator();
               iter.hasNext(); ) {
            CacheServerImpl bridge = (CacheServerImpl) iter.next();
            if (System.identityHashCode(bridge) == config.getId()) {
              bridge.stop();
              m.bridgeInfo = new RemoteBridgeServer(bridge);
              break;

            } else {
              m.bridgeInfo = null;
            }
          }
          break;
        }

        default:
          Assert.assertTrue(false, "Unknown bridge server operation: " +
                            operation);
        }

      }

    } catch (CancelException ex) {
      m.bridgeInfo = null;

    } catch (Exception ex) {
      m.exception = ex;
      m.bridgeInfo = null;
    }
    return m;
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns information about the bridge operated on
   */
  public RemoteBridgeServer getBridgeInfo() {
    return this.bridgeInfo;
  }

  /**
   * Returns an exception that was thrown while processing the
   * request.
   */
  public Exception getException() {
    return this.exception;
  }

  public int getDSFID() {
    return BRIDGE_SERVER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.bridgeInfo, out);
    DataSerializer.writeObject(this.exception, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bridgeInfo =
      (RemoteBridgeServer) DataSerializer.readObject(in);
    this.exception =
      (Exception) DataSerializer.readObject(in);
  }

}
