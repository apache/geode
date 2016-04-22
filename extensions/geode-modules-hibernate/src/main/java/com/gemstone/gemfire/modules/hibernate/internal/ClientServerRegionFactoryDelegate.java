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
package com.gemstone.gemfire.modules.hibernate.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.modules.util.BootstrappingFunction;
import com.gemstone.gemfire.modules.util.CreateRegionFunction;
import com.gemstone.gemfire.modules.util.RegionConfiguration;

public class ClientServerRegionFactoryDelegate extends RegionFactoryDelegate {

  private static final String DEFAULT_SERVER_REGION_TYPE = RegionShortcut.PARTITION.name();

  private static final String DEFAULT_CLIENT_REGION_TYPE = ClientRegionShortcut.PROXY.name();

  private ClientCache clientCache;
  
  public ClientServerRegionFactoryDelegate(Properties gemfireProperties,
      Properties regionProperties) {
    super(gemfireProperties, regionProperties);
  }

  @Override
  public GemFireCache startCache() {
    log.info("Creating a GemFire client cache");
    String locatorsString = (String)gemfireProperties.remove("locators");
    checkExistingCache();
    ClientCacheFactory ccf = new ClientCacheFactory(gemfireProperties).setPoolSubscriptionEnabled(true);
    List<LocatorHolder> locators = getLocatorsMap(locatorsString);
    for (LocatorHolder locHolder : locators) {
      log.debug("adding pool locator with host {} port {}", locHolder.host, locHolder.port);
      ccf.addPoolLocator(locHolder.host, locHolder.port);
    }
    this.clientCache = ccf.create();
    
    log.debug("GemFire client cache creation completed");
    // bootstrap the servers
    FunctionService.onServers(this.clientCache).execute(new BootstrappingFunction()).getResult();
    FunctionService.registerFunction(new CreateRegionFunction(this.clientCache));
    return this.clientCache;
  }

  private List<LocatorHolder> getLocatorsMap(String locatorsString) {
    List<LocatorHolder> retval = new ArrayList<LocatorHolder>();
    if (locatorsString == null || locatorsString.isEmpty()) {
      return retval;
    }
    StringTokenizer st = new StringTokenizer(locatorsString, ",");
    while (st.hasMoreTokens()) {
      String locator = st.nextToken();
      int portIndex = locator.indexOf('[');
      if (portIndex < 1) {
        portIndex = locator.lastIndexOf(':');
      }
      // starting in 5.1.0.4 we allow '@' as the bind-addr separator
      // to let people use IPv6 numeric addresses (which contain colons)
      int bindAddrIdx = locator.lastIndexOf('@', portIndex - 1);
      
      if (bindAddrIdx < 0) {
        bindAddrIdx = locator.lastIndexOf(':', portIndex - 1);
      }

      String host = locator.substring(0,
          bindAddrIdx > -1 ? bindAddrIdx : portIndex);

      if (host.indexOf(':') >= 0) {
        bindAddrIdx = locator.lastIndexOf('@');
        host = locator.substring(0, bindAddrIdx > -1 ? bindAddrIdx : portIndex);
      }
      int lastIndex = locator.lastIndexOf(']');
      if (lastIndex == -1) {
        if (locator.indexOf('[') >= 0) {
          throw new IllegalArgumentException("Invalid locator");
        } else {
          // Using host:port syntax
          lastIndex = locator.length();
        }
      }
      String port = locator.substring(portIndex + 1, lastIndex);
      int portVal = 0;
      try {
        portVal = Integer.parseInt(port);
        if (portVal < 1 || portVal > 65535) {
          throw new IllegalArgumentException("port should be grater than zero and less than 65536");
        }
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException("Invalid Locator");
      }
      retval.add(new LocatorHolder(host, portVal));
    }
    return retval;
  }

  @Override
  public Region<Object, EntityWrapper> createRegion(String regionName) {
    // first create the region on the server
    String serverRegionType = getServerRegionType(regionName);
    RegionConfiguration regionConfig = new RegionConfiguration();
    regionConfig.setRegionName(regionName);
    regionConfig.setRegionAttributesId(serverRegionType);
    regionConfig.setCacheWriterName(EntityRegionWriter.class.getCanonicalName());
    FunctionService.onServer(this.clientCache).withArgs(regionConfig)
        .execute(CreateRegionFunction.ID).getResult();
    // now create region on the client
    Region<Object, EntityWrapper> r = this.clientCache.getRegion(regionName);
    if (r != null) {
      return r;
    }
    String clientRegionType = getClientRegionType(regionName);
    ClientRegionFactory<Object, EntityWrapper> rf = this.clientCache
        .createClientRegionFactory(ClientRegionShortcut
            .valueOf(clientRegionType));
    r = rf.create(regionName);
    return r;
  }

  private String getClientRegionType(String regionName) {
    String rType = getOverridenClientRegionType(regionName);
    if (rType != null) {
      return rType.toUpperCase();
    }
    rType = regionProperties.getProperty("gemfire.default-client-region-attributes-id");
    if (rType == null) {
      rType = DEFAULT_CLIENT_REGION_TYPE;
    }
    return rType.toUpperCase();
  }

  private String getServerRegionType(String regionName) {
    String rType = getOverridenServerRegionType(regionName);
    if (rType != null) {
      return rType.toUpperCase();
    }
    rType = regionProperties.getProperty("gemfire.default-region-attributes-id");
    if (rType == null) {
      rType = DEFAULT_SERVER_REGION_TYPE;
    }
    return rType.toUpperCase();
  }

  private String getOverridenServerRegionType(String regionName) {
    String rType = null;
    Iterator<Object> it = regionProperties.keySet().iterator();
    while (it.hasNext()) {
      String current = (String)it.next();
      if (current.contains(regionName) && !current.contains("client")) {
        rType = regionProperties.getProperty(current);
        break;
      }
    }
    return rType;
  }

  private String getOverridenClientRegionType(String regionName) {
    String rType = null;
    Iterator<Object> it = regionProperties.keySet().iterator();
    while (it.hasNext()) {
      String current = (String)it.next();
      if (current.contains(regionName) && current.contains("client")) {
        rType = regionProperties.getProperty(current);
        break;
      }
    }
    return rType;
  }
  
  private static class LocatorHolder {
    private String host;
    private int port;
    private LocatorHolder(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }
}
