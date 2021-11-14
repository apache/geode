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

package org.apache.geode;

import static java.lang.Thread.currentThread;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotFilter;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.RegionEntry;

public class ExportLocalDataFunction implements Function<Object[]>, Declarable {

  private static final long serialVersionUID = 4380042210718815441L;

  public ExportLocalDataFunction() {}

  public void execute(final FunctionContext<Object[]> context) {
    final Cache cache = context.getCache();
    final LogWriter logger = cache.getLogger();

    logger.info("EXPORT: Executing ExportLocalDataFunction on " + cache.getName());

    Object[] arguments = context.getArguments();
    String directoryName = null;
    if (arguments != null && arguments.length > 0) {
      directoryName = (String) arguments[0];
    }

    final String memberName = context.getMemberName();

    // Get local data set
    final RegionFunctionContext rfc = (RegionFunctionContext) context;
    final Region<Object, Object> localData = PartitionRegionHelper.getLocalDataForContext(rfc);

    // Create the file
    final String fileName =
        "server_" + memberName + "_region_" + localData.getName() + "_snapshot.gfd";
    final File file = new File(directoryName, fileName);

    // Export local data set
    final RegionSnapshotService<Object, Object> service = localData.getSnapshotService();
    try {
      logger.warning("EXPORT: " +
          currentThread().getName() + ": Exporting " + localData.size() + " entries in region "
          + localData.getName() + " to file " + file.getAbsolutePath() + " started");
      final SnapshotOptions<Object, Object> options = service.createOptions();
      options.setFilter(getRejectingFilter(localData, logger));
      service.save(file, SnapshotFormat.GEMFIRE, options);
      logger.warning("EXPORT: " +
          currentThread().getName() + ": Exporting " + localData.size() + " entries in region "
          + localData.getName() + " to file " + file.getAbsolutePath() + " completed");
    } catch (Exception e) {
      context.getResultSender().sendException(e);
      return;
    }

    context.getResultSender().lastResult(true);
  }

  private <K, V> SnapshotFilter<K, V> getRejectingFilter(final Region<K, V> localData,
      final LogWriter logger) {
    return entry -> {
      boolean accept = true;
      try {
        // noinspection ResultOfMethodCallIgnored
        entry.getValue();
      } catch (Exception e) {
        final byte[] valueBytes = getValueBytes(entry);
        logger.warning("Caught the following exception attempting to deserialize value region="
            + localData.getName() + "; key=" + entry.getKey() + "; valueLength="
            + valueBytes.length
            + "; value=" + Arrays.toString(valueBytes) + ":", e);
        accept = false;
      }
      return accept;
    };
  }

  private static <K, V> byte[] getValueBytes(final Map.Entry<K, V> entry) {
    byte[] valueBytes = null;
    if (entry instanceof EntrySnapshot) {
      EntrySnapshot es = (EntrySnapshot) entry;
      RegionEntry re = es.getRegionEntry();
      Object valueInVm = re.getValueInVM(null);
      if (valueInVm instanceof CachedDeserializable) {
        Object cdValue = ((CachedDeserializable) valueInVm).getValue();
        if (cdValue instanceof byte[]) {
          valueBytes = (byte[]) cdValue;
        }
      }
    }
    return valueBytes;
  }

  public String getId() {
    return getClass().getSimpleName();
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean hasResult() {
    return true;
  }

  public boolean isHA() {
    return false;
  }

  public void init(Properties properties) {}
}
