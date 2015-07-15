/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreMutatorImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * Function used by the 'alter hdfs-store' gfsh command to alter a hdfs store on
 * each member.
 * 
 * @author Namrata Thanvi
 */

public class AlterHDFSStoreFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();

  private static final String ID = AlterHDFSStoreFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    String memberId = "";

    try {
      final AlterHDFSStoreAttributes alterAttributes = (AlterHDFSStoreAttributes)context.getArguments();      
      GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
      DistributedMember member = getDistributedMember(cache);

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }      
      HDFSStore hdfsStore = cache.findHDFSStore(alterAttributes.getHdfsUniqueName());      
      CliFunctionResult result;
      if (hdfsStore != null) {
        // TODO - Need to verify what all attributes needs to be persisted in
        // cache.xml
        XmlEntity xmlEntity = getXMLEntity(hdfsStore.getName());
        alterHdfsStore(hdfsStore, alterAttributes);
        result = new CliFunctionResult(memberId, xmlEntity, "Success");
      }
      else {
        result = new CliFunctionResult(memberId, false, "Hdfs store not found on this member");
      }
      context.getResultSender().lastResult(result);

    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;

    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not alter hdfs store: {}", th.getMessage(), th);

      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }

  }

  @Override
  public String getId() {
    return ID;
  }

  /**
   * Alter HDFSStore with given configuration.
   * 
   * @param hdfsStore
   * @param alterAttributes
   * @return HDFSStore
   */

  protected HDFSStore alterHdfsStore(HDFSStore hdfsStore, AlterHDFSStoreAttributes alterAttributes) {
    HDFSStoreMutator storeMutator = new HDFSStoreMutatorImpl(hdfsStore);
  
		if (alterAttributes.getFileRolloverInterval() != null)
			storeMutator.setWriteOnlyFileRolloverInterval(alterAttributes
					.getFileRolloverInterval());

		if (alterAttributes.getMaxWriteonlyFileSize() != null)
			storeMutator.setWriteOnlyFileRolloverSize(alterAttributes.getMaxWriteonlyFileSize());

		if (alterAttributes.getMinorCompact() != null)
			storeMutator.setMinorCompaction(alterAttributes.getMinorCompact());

		if (alterAttributes.getMajorCompact() != null)
		  storeMutator.setMajorCompaction(alterAttributes.getMajorCompact());

		if (alterAttributes.getMajorCompactionInterval() != null)
		  storeMutator.setMajorCompactionInterval(alterAttributes.getMajorCompactionInterval());

		if (alterAttributes.getMajorCompactionThreads() != null)
		  storeMutator.setMajorCompactionThreads(alterAttributes.getMajorCompactionThreads());

		if (alterAttributes.getMajorCompactionThreads() != null)
		  storeMutator.setMinorCompactionThreads(alterAttributes.getMajorCompactionThreads());

		if (alterAttributes.getPurgeInterval() != null)
			storeMutator.setPurgeInterval(alterAttributes.getPurgeInterval());

		if (alterAttributes.getBatchSize() != null)
		  storeMutator.setBatchSize(alterAttributes.getBatchSize());

		if (alterAttributes.getBatchInterval() != null)
		  storeMutator.setBatchInterval(alterAttributes.getBatchInterval());

		hdfsStore.alter(storeMutator);
		return hdfsStore;
  }
  
  
  public static class AlterHDFSStoreAttributes implements Serializable {
	private static final long serialVersionUID = 1L;
	String hdfsUniqueName;
      Integer batchSize , batchInterval;
      Boolean minorCompact,  majorCompact;
      Integer minorCompactionThreads, majorCompactionInterval, majorCompactionThreads, purgeInterval;
      Integer fileRolloverInterval, maxWriteonlyFileSize;
      
	public AlterHDFSStoreAttributes(String hdfsUniqueName, Integer batchSize,
			Integer batchInterval, Boolean minorCompact, Boolean majorCompact,
			Integer minorCompactionThreads, Integer majorCompactionInterval,
			Integer majorCompactionThreads, Integer purgeInterval,
			Integer fileRolloverInterval, Integer maxWriteonlyFileSize) {
		this.hdfsUniqueName = hdfsUniqueName;
		this.batchSize = batchSize;
		this.batchInterval = batchInterval;
		this.minorCompact = minorCompact;
		this.majorCompact = majorCompact;
		this.minorCompactionThreads = minorCompactionThreads;
		this.majorCompactionInterval = majorCompactionInterval;
		this.majorCompactionThreads = majorCompactionThreads;
		this.purgeInterval = purgeInterval;
		this.fileRolloverInterval = fileRolloverInterval;
		this.maxWriteonlyFileSize = maxWriteonlyFileSize;
	}

	public String getHdfsUniqueName() {
		return hdfsUniqueName;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public Integer getBatchInterval() {
		return batchInterval;
	}

	public Boolean getMinorCompact() {
		return minorCompact;
	}

	public Boolean getMajorCompact() {
		return majorCompact;
	}

	public Integer getMinorCompactionThreads() {
		return minorCompactionThreads;
	}

	public Integer getMajorCompactionInterval() {
		return majorCompactionInterval;
	}

	public Integer getMajorCompactionThreads() {
		return majorCompactionThreads;
	}

	public Integer getPurgeInterval() {
		return purgeInterval;
	}

	public Integer getFileRolloverInterval() {
		return fileRolloverInterval;
	}

	public Integer getMaxWriteonlyFileSize() {
		return maxWriteonlyFileSize;
	}
	  
	
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
