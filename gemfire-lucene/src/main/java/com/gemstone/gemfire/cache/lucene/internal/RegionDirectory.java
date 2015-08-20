package com.gemstone.gemfire.cache.lucene.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystem;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

public class RegionDirectory extends BaseDirectory {

  private static final class FileIndexInput extends BufferedIndexInput {
    private final File file;
    InputStream in;

    private FileIndexInput(String resourceDesc, File file) {
      super(resourceDesc);
      this.file = file;
      in = file.getInputStream();
    }

    @Override
    public long length() {
      return file.getLength();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
      in.close();
      in = file.getInputStream();
      in.skip(pos);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      in.read(b, offset, length);
    }
  }

  static private final AtomicInteger cacheCount = new AtomicInteger();
  static private final boolean CREATE_CACHE = Boolean.getBoolean("lucene.createCache");
  private static final Logger logger = LogService.getLogger();
  
  private Cache cache;
  private final FileSystem fs;
  
  /**
   * Create RegionDirectory to save index documents in file format into Gemfire region.
   * @param dataRegionName data region's full name to build index from
   */
  public RegionDirectory(String dataRegionName) {
    super(new SingleInstanceLockFactory());

    Cache cache = null;
    try {
       cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      //ignore
    }
    if (null == cache) {
      if (CREATE_CACHE) {
        cache = new CacheFactory().set("mcast-port", "0").set("log-level", "error").create();
        logger.info("Created cache in RegionDirectory");
      } else {
        throw new IllegalStateException(LocalizedStrings.CqService_CACHE_IS_NULL.toLocalizedString());
      }
    } else {
      logger.info("Found cache in RegionDirectory");
    }
    this.cache = cache;
    assert this.cache != null;
    cacheCount.incrementAndGet();
    
    Region dataRegion = cache.getRegion(dataRegionName);
    assert dataRegion != null;
    RegionAttributes ra = dataRegion.getAttributes();
    DataPolicy dp = ra.getDataPolicy();
    final boolean isPartitionedRegion = (ra.getPartitionAttributes() == null) ? false : true;
    final boolean withPersistence = dp.withPersistence();
    final boolean withStorage = isPartitionedRegion?ra.getPartitionAttributes().getLocalMaxMemory()>0:dp.withStorage();
    RegionShortcut regionShortCut;
    if (isPartitionedRegion) {
      if (withPersistence) {
        regionShortCut = RegionShortcut.PARTITION_PERSISTENT;
      } else {
        regionShortCut = RegionShortcut.PARTITION;
      }
    } else {
      if (withPersistence) {
        regionShortCut = RegionShortcut.REPLICATE_PERSISTENT;
      } else {
        regionShortCut = RegionShortcut.REPLICATE;
      }
    }
    
//    final boolean isOffHeap = ra.getOffHeap();
    // TODO: 1) dataRegion should be withStorage
    //       2) Persistence to Persistence
    //       3) Replicate to Replicate, Partition To Partition
    //       4) Offheap to Offheap
    if (!withStorage) {
      throw new IllegalStateException("The data region to create lucene index should be with storage");
    }
    
    final String fileRegionName = dataRegionName+".files";
    Region<String, File> fileRegion = cache.<String, File> getRegion(fileRegionName);
    if (null == fileRegion) {
      fileRegion = cache.<String, File> createRegionFactory(regionShortCut)
          .setPartitionAttributes(new PartitionAttributesFactory<String, File>().setColocatedWith(dataRegionName).create())
          .create(fileRegionName);
    }

    final String chunkRegionName = dataRegionName + ".chunks";
    Region<ChunkKey, byte[]> chunkRegion = cache.<ChunkKey, byte[]> getRegion(chunkRegionName);
    if (null == chunkRegion) {
      chunkRegion = cache.<ChunkKey, byte[]> createRegionFactory(regionShortCut)
          .setPartitionAttributes(new PartitionAttributesFactory<ChunkKey, byte[]>().setColocatedWith(fileRegion.getFullPath()).create())
          .create(chunkRegionName);
    }
    
    fs = new FileSystem(fileRegion, chunkRegion);
  }
  
  @Override
  public String[] listAll() throws IOException {
    return fs.listFileNames().toArray(new String[] {});
  }

  @Override
  public void deleteFile(String name) throws IOException {
    fs.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    return fs.getFile(name).getLength();
  }

  @Override
  public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
    final File file = fs.createFile(name);
    final OutputStream out = file.getOutputStream();

    return new OutputStreamIndexOutput(name, out, 1000);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    // Region does not need to sync to disk
  }

  @Override
  public void renameFile(String source, String dest) throws IOException {
    fs.renameFile(source, dest);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    final File file = fs.getFile(name);

    return new FileIndexInput(name, file);
  }

  @Override
  public void close() throws IOException {
    if (0 == cacheCount.decrementAndGet()) {
      cache.close();
    }
    isOpen = false;
  }

}
