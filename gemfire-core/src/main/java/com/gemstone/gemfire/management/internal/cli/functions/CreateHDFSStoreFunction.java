package com.gemstone.gemfire.management.internal.cli.functions;

import org.apache.logging.log4j.Logger;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;


/**
 * Function used by the 'create hdfs-store' gfsh command to create a hdfs store
 * on each member.
 * 
 * @author Namrata Thanvi
 */

public class CreateHDFSStoreFunction extends FunctionAdapter implements InternalEntity {
  
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();

  public static final CreateHDFSStoreFunction INSTANCE = new CreateHDFSStoreFunction();

  private static final String ID = CreateHDFSStoreFunction.class.getName();

  @Override
  public void execute(FunctionContext context) {
    String memberId = "";
    try {
      Cache cache = getCache();      
      DistributedMember member = getDistributedMember(cache);
      
      memberId = member.getId();
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }
      HDFSStoreConfigHolder configHolder = (HDFSStoreConfigHolder)context.getArguments();
     
      HDFSStore hdfsStore = createHdfsStore(cache, configHolder);
      // TODO - Need to verify what all attributes needs to be persisted in
      // cache.xml
      XmlEntity xmlEntity = getXMLEntity(hdfsStore.getName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity, "Success"));

    } catch (CacheClosedException cce) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, null));

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not create hdfs store: {}", CliUtil.stackTraceAsString(th), th);
      context.getResultSender().lastResult(new CliFunctionResult(memberId, th, th.getMessage()));
    }
  }

  @Override
  public String getId() {
    return ID;
  } 
  
  /**
   * Creates the HDFSStore with given configuration.
   * 
   * @param cache
   * @param configHolder
   * @return HDFSStore
   */

  protected HDFSStore createHdfsStore(Cache cache, HDFSStoreConfigHolder configHolder) {    
    HDFSStoreFactory hdfsStoreFactory = cache.createHDFSStoreFactory();
    hdfsStoreFactory.setName(configHolder.getName());
    hdfsStoreFactory.setNameNodeURL(configHolder.getNameNodeURL());
    hdfsStoreFactory.setBlockCacheSize(configHolder.getBlockCacheSize());
    hdfsStoreFactory.setWriteOnlyFileRolloverInterval(configHolder.getWriteOnlyFileRolloverInterval());
    hdfsStoreFactory.setHomeDir(configHolder.getHomeDir());
    hdfsStoreFactory.setHDFSClientConfigFile(configHolder.getHDFSClientConfigFile());
    hdfsStoreFactory.setWriteOnlyFileRolloverSize(configHolder.getWriteOnlyFileRolloverSize());
    hdfsStoreFactory.setMajorCompaction(configHolder.getMajorCompaction());
    hdfsStoreFactory.setMajorCompactionInterval(configHolder.getMajorCompactionInterval());
    hdfsStoreFactory.setMajorCompactionThreads(configHolder.getMajorCompactionThreads());
    hdfsStoreFactory.setMinorCompaction(configHolder.getMinorCompaction());
    hdfsStoreFactory.setMaxMemory(configHolder.getMaxMemory());
    hdfsStoreFactory.setBatchSize(configHolder.getBatchSize());
    hdfsStoreFactory.setBatchInterval(configHolder.getBatchInterval());
    hdfsStoreFactory.setDiskStoreName(configHolder.getDiskStoreName());
    hdfsStoreFactory.setDispatcherThreads(configHolder.getDispatcherThreads());
    hdfsStoreFactory.setMinorCompactionThreads(configHolder.getMinorCompactionThreads());
    hdfsStoreFactory.setPurgeInterval(configHolder.getPurgeInterval());
    hdfsStoreFactory.setSynchronousDiskWrite(configHolder.getSynchronousDiskWrite());
    hdfsStoreFactory.setBufferPersistent(configHolder.getBufferPersistent());
    
    return hdfsStoreFactory.create(configHolder.getName());   
  }
  
  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }
  
  protected DistributedMember getDistributedMember(Cache cache){
    return ((InternalCache)cache).getMyId();
  }
  
  protected XmlEntity getXMLEntity(String storeName){
    return new XmlEntity(CacheXml.HDFS_STORE, "name", storeName);
  }
}

