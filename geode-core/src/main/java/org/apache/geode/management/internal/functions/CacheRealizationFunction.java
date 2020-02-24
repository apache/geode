/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.functions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.configuration.HasFile;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Member;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.beans.FileUploader;
import org.apache.geode.management.internal.configuration.realizers.ConfigurationRealizer;
import org.apache.geode.management.internal.configuration.realizers.DeploymentRealizer;
import org.apache.geode.management.internal.configuration.realizers.GatewayReceiverRealizer;
import org.apache.geode.management.internal.configuration.realizers.IndexRealizer;
import org.apache.geode.management.internal.configuration.realizers.MemberRealizer;
import org.apache.geode.management.internal.configuration.realizers.PdxRealizer;
import org.apache.geode.management.internal.configuration.realizers.RegionConfigRealizer;
import org.apache.geode.management.runtime.RuntimeInfo;

public class CacheRealizationFunction implements InternalFunction<List> {
  private static final Logger logger = LogService.getLogger();
  @Immutable
  private static final Map<Class, ConfigurationRealizer> realizers = new HashMap<>();

  static {
    realizers.put(Region.class, new RegionConfigRealizer());
    realizers.put(GatewayReceiver.class, new GatewayReceiverRealizer());
    realizers.put(Member.class, new MemberRealizer());
    realizers.put(Pdx.class, new PdxRealizer());
    realizers.put(Deployment.class, new DeploymentRealizer());
    realizers.put(Index.class, new IndexRealizer());
  }

  @Override
  public void execute(FunctionContext<List> context) {
    AbstractConfiguration cacheElement = (AbstractConfiguration) context.getArguments().get(0);
    CacheElementOperation operation = (CacheElementOperation) context.getArguments().get(1);
    RemoteInputStream jarStream = (RemoteInputStream) context.getArguments().get(2);

    InternalCache cache = (InternalCache) context.getCache();
    try {
      if (operation == CacheElementOperation.GET) {
        context.getResultSender().lastResult(executeGet(context, cache, cacheElement));
      } else {
        context.getResultSender()
            .lastResult(executeUpdate(context, cache, cacheElement, operation, jarStream));
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      context.getResultSender().lastResult(new RealizationResult()
          .setSuccess(false)
          .setMemberName(context.getMemberName())
          .setMessage(e.getMessage()));
    }

  }

  public RuntimeInfo executeGet(FunctionContext<List> context,
      InternalCache cache, AbstractConfiguration cacheElement) {
    ConfigurationRealizer realizer = realizers.get(cacheElement.getClass());

    if (realizer == null) {
      return null;
    }
    RuntimeInfo runtimeInfo = realizer.get(cacheElement, cache);

    // set the membername if this is not a global runtime
    if (!cacheElement.isGlobalRuntime()) {
      runtimeInfo.setMemberName(context.getMemberName());
    }

    return runtimeInfo;
  }

  public RealizationResult executeUpdate(FunctionContext<List> context,
      InternalCache cache, AbstractConfiguration cacheElement,
      CacheElementOperation operation, RemoteInputStream jarStream) throws Exception {

    ConfigurationRealizer realizer = realizers.get(cacheElement.getClass());

    RealizationResult result = new RealizationResult();
    result.setMemberName(context.getMemberName());

    if (realizer == null || realizer.isReadyOnly()) {
      return result.setMessage("Server '" + context.getMemberName()
          + "' needs to be restarted for this configuration change to be realized.");
    }

    // the the function parameter contains streamed file, staging the file first
    if ((cacheElement instanceof HasFile) && jarStream != null) {
      HasFile configuration = (HasFile) cacheElement;
      Set<File> files = stageFileContent(Collections.singletonList(configuration.getFileName()),
          Collections.singletonList(jarStream));
      configuration.setFile(files.iterator().next());
    }

    switch (operation) {
      case CREATE:
        if (realizer.exists(cacheElement, cache)) {
          return result.setMessage(cacheElement.getClass().getSimpleName() + " '"
              + cacheElement.getId() + "' already exists. Skipped creation.");
        }
        result = realizer.create(cacheElement, cache);
        break;
      case DELETE:
        if (!realizer.exists(cacheElement, cache)) {
          return result.setMessage(
              cacheElement.getClass().getSimpleName() + " '" + cacheElement.getId()
                  + "' does not exist.");
        }
        result = realizer.delete(cacheElement, cache);
        break;
      case UPDATE:
        if (!realizer.exists(cacheElement, cache)) {
          return result.setSuccess(false).setMessage(
              cacheElement.getClass().getSimpleName() + " '" + cacheElement.getId()
                  + "' does not exist.");
        }
        result = realizer.update(cacheElement, cache);
        break;
    }
    result.setMemberName(context.getMemberName());
    return result;
  }

  public static Set<File> stageFileContent(List<String> jarNames,
      List<RemoteInputStream> jarStreams) throws IOException {
    Set<File> stagedJars = new HashSet<>();

    try {
      Path tempDir = FileUploader.createSecuredTempDirectory("deploy-");

      for (int i = 0; i < jarNames.size(); i++) {
        Path tempJar = Paths.get(tempDir.toString(), jarNames.get(i));
        FileOutputStream fos = new FileOutputStream(tempJar.toString());

        InputStream input = RemoteInputStreamClient.wrap(jarStreams.get(i));

        IOUtils.copyLarge(input, fos);

        fos.close();
        input.close();

        stagedJars.add(tempJar.toFile());
      }
    } catch (IOException iox) {
      for (int i = 0; i < jarStreams.size(); i++) {
        try {
          jarStreams.get(i).close(true);
        } catch (IOException ex) {
          // Ignored
        }
      }
      throw iox;
    }

    return stagedJars;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
