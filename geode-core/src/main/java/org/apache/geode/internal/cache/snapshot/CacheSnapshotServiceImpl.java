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
package org.apache.geode.internal.cache.snapshot;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.geode.admin.RegionNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.CacheSnapshotService;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.snapshot.GFSnapshot.GFSnapshotImporter;

/**
 * Provides an implementation for cache snapshots. Most of the implementation delegates to
 * {@link RegionSnapshotService}.
 */
public class CacheSnapshotServiceImpl implements CacheSnapshotService {
  /** the cache */
  private final InternalCache cache;

  public CacheSnapshotServiceImpl(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public SnapshotOptions<Object, Object> createOptions() {
    return new SnapshotOptionsImpl<>();
  }

  @Override
  public void save(File dir, SnapshotFormat format) throws IOException {
    save(dir, format, createOptions());
  }

  @Override
  public void save(File dir, SnapshotFormat format, SnapshotOptions<Object, Object> options)
      throws IOException {
    createDirectoryIfNeeded(dir);

    for (Region<?, ?> region : cache.rootRegions()) {
      for (Region<?, ?> subRegion : region.subregions(true)) {
        saveRegion(subRegion, dir, format, options);
      }
      saveRegion(region, dir, format, options);
    }
  }

  private void createDirectoryIfNeeded(File dir) throws IOException {
    if (!dir.exists()) {
      boolean created = dir.mkdirs();
      if (!created) {
        throw new IOException(
            String.format("Unable to create snapshot directory %s", dir));
      }
    }
  }

  @Override
  public void load(File dir, SnapshotFormat format) throws IOException, ClassNotFoundException {
    if (!dir.exists() || !dir.isDirectory()) {
      throw new FileNotFoundException("Unable to load snapshot from " + dir.getCanonicalPath()
          + " as the file does not exist or is not a directory");
    }
    File[] snapshotFiles = getSnapshotFiles(dir);
    load(snapshotFiles, format, createOptions());
  }

  private File[] getSnapshotFiles(File dir) throws IOException {
    File[] snapshotFiles = dir.listFiles(pathname -> pathname.getName().endsWith(".gfd"));

    if (snapshotFiles == null) {
      throw new IOException("Unable to access " + dir.getCanonicalPath());
    } else if (snapshotFiles.length == 0) {
      throw new FileNotFoundException(
          String.format("No snapshot files found in %s", dir));
    }

    return snapshotFiles;
  }

  @Override
  public void load(File[] snapshotFiles, SnapshotFormat format,
      SnapshotOptions<Object, Object> options) throws IOException, ClassNotFoundException {

    for (File file : snapshotFiles) {
      GFSnapshotImporter in = new GFSnapshotImporter(file, cache.getPdxRegistry());
      try {
        byte version = in.getVersion();
        if (version == GFSnapshot.SNAP_VER_1) {
          throw new IOException(
              String.format("Unsupported snapshot version: %s", version));
        }

        String regionName = in.getRegionName();
        Region<Object, Object> region = cache.getRegion(regionName);
        if (region == null) {
          throw new RegionNotFoundException(String.format(
              "Could not find region %s. Ensure that the region is created prior to importing the snapshot file %s.",
              regionName, file));
        }

        RegionSnapshotService<Object, Object> rs = region.getSnapshotService();
        rs.load(file, format, options);

      } finally {
        in.close();
      }
    }
  }

  private void saveRegion(Region<?, ?> region, File dir, SnapshotFormat format,
      SnapshotOptions options) throws IOException {
    RegionSnapshotService<?, ?> regionSnapshotService = region.getSnapshotService();
    String name = "snapshot" + region.getFullPath().replace('/', '-')
        + RegionSnapshotService.SNAPSHOT_FILE_EXTENSION;
    File f = new File(dir, name);
    regionSnapshotService.save(f, format, options);
  }
}
