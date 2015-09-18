package com.gemstone.gemfire.cache.lucene.internal.directory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

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

/**
 * An implementation of Directory that stores data in geode regions.
 * 
 * Directory is an interface to file/RAM storage for lucene. This class uses
 * the {@link FileSystem} class to store the data in the provided geode
 * regions.
 */
public class RegionDirectory extends BaseDirectory {

  private static final Logger logger = LogService.getLogger();
  
  private final FileSystem fs;
  
  /**
   * Create a region directory with a given file and chunk region. These regions
   * may be bucket regions or they may be replicated regions.
   */
  public RegionDirectory(ConcurrentMap<String, File> fileRegion, ConcurrentMap<ChunkKey, byte[]> chunkRegion) {
    super(new SingleInstanceLockFactory());
    fs = new FileSystem(fileRegion, chunkRegion);
  }
  
  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    return fs.listFileNames().toArray(new String[] {});
  }

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    fs.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    return fs.getFile(name).getLength();
  }

  @Override
  public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
    ensureOpen();
    final File file = fs.createFile(name);
    final OutputStream out = file.getOutputStream();

    return new OutputStreamIndexOutput(name, out, 1000);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
    // Region does not need to sync to disk
  }

  @Override
  public void renameFile(String source, String dest) throws IOException {
    ensureOpen();
    fs.renameFile(source, dest);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    final File file = fs.getFile(name);

    return new FileIndexInput(name, file);
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
  }

}
