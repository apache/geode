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
package org.apache.geode.internal.cache.snapshot;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.admin.RegionNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.snapshot.CacheSnapshotService;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.snapshot.GFSnapshot.GFSnapshotImporter;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Provides an implementation for cache snapshots.  Most of the implementation
 * delegates to {@link RegionSnapshotService}.
 * 
 */
public class CacheSnapshotServiceImpl implements CacheSnapshotService {
  /** the cache */
  private final GemFireCacheImpl cache;
  
  public CacheSnapshotServiceImpl(GemFireCacheImpl cache) {
    this.cache = cache;
  }
  
  @Override
  public SnapshotOptions<Object, Object> createOptions() {
    return new SnapshotOptionsImpl<Object, Object>();
  }

  @Override
  public void save(File dir, SnapshotFormat format) throws IOException {
    save(dir, format, createOptions());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void save(File dir, SnapshotFormat format,
      SnapshotOptions<Object, Object> options) throws IOException {
    if (!dir.exists()) {
      boolean created = dir.mkdirs();
      if (!created) {
        throw new IOException(LocalizedStrings.Snapshot_UNABLE_TO_CREATE_DIR_0.toLocalizedString(dir));
      }
    }
    
    for (Region<?, ?> r : (Set<Region<?, ?>>) cache.rootRegions()) {
      for (Region<?, ?> sub : r.subregions(true)) {
        saveRegion(sub, dir, format, options);
      }
      saveRegion(r, dir, format, options);
    }
  }

  @Override
  public void load(File dir, SnapshotFormat format) throws IOException,
      ClassNotFoundException {
    File[] snapshots = dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.isDirectory();
      }
    });
    
    if (snapshots == null) {
      throw new FileNotFoundException(LocalizedStrings.Snapshot_NO_SNAPSHOT_FILES_FOUND_0.toLocalizedString(dir));
    }
    load(snapshots, format, createOptions());
  }

  @Override
  public void load(File[] snapshots, SnapshotFormat format,
      SnapshotOptions<Object, Object> options) throws IOException,
      ClassNotFoundException {
    
    for (File f : snapshots) {
      GFSnapshotImporter in = new GFSnapshotImporter(f);
      try {
        byte version = in.getVersion();
        if (version == GFSnapshot.SNAP_VER_1) {
          throw new IOException(LocalizedStrings.Snapshot_UNSUPPORTED_SNAPSHOT_VERSION_0.toLocalizedString(version));
        }
        
        String regionName = in.getRegionName();
        Region<Object, Object> region = cache.getRegion(regionName);
        if (region == null) {
          throw new RegionNotFoundException(LocalizedStrings.Snapshot_COULD_NOT_FIND_REGION_0_1.toLocalizedString(regionName, f));
        }
        
        RegionSnapshotService<Object, Object> rs = region.getSnapshotService();
        rs.load(f, format, options);

      } finally {
        in.close();
      }
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void saveRegion(Region<?, ?> region, File dir, SnapshotFormat format, SnapshotOptions options) 
      throws IOException {
    RegionSnapshotService<?, ?> rs = region.getSnapshotService();
    String name = "snapshot" + region.getFullPath().replace('/', '-');
    File f = new File(dir, name);
    rs.save(f, format, options);
  }
}
