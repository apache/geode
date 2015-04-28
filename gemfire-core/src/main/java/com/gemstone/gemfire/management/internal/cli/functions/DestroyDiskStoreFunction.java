/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * Function used by the 'destroy disk-store' gfsh command to destroy a disk
 * store on each member.
 * 
 * @author David Hoots
 * @since 8.0
 */

public class DestroyDiskStoreFunction extends FunctionAdapter implements InternalEntity {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    
    try {
      final Object[] args = (Object[]) context.getArguments();
      final String diskStoreName = (String) args[0];

      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      DiskStore diskStore = cache.findDiskStore(diskStoreName);

      CliFunctionResult result;
      if (diskStore != null) {
        XmlEntity xmlEntity = new XmlEntity(CacheXml.DISK_STORE, "name", diskStoreName);
        diskStore.destroy();
        result = new CliFunctionResult(memberId, xmlEntity, "Success");
      } else {
        result = new CliFunctionResult(memberId, false, "Disk store not found on this member");
      }
      context.getResultSender().lastResult(result);

    } catch (IllegalStateException isex) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, isex.getMessage());
      context.getResultSender().lastResult(result);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      logger.error("Could not destroy disk store: {}", th.getMessage(), th);

      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return CreateDiskStoreFunction.class.getName();
  }
}
