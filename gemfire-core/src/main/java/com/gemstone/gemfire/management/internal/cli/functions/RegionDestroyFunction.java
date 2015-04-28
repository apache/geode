/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class RegionDestroyFunction implements Function, InternalEntity {
  private static final long serialVersionUID = 9172773671865750685L;

  public static final RegionDestroyFunction INSTANCE = new RegionDestroyFunction();

  private static final String ID = RegionDestroyFunction.class.getName();

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(FunctionContext context) {
    String regionPath = null;
    try {
      String functionId = context.getFunctionId();
      if (getId().equals(functionId)) {
        Object arguments = context.getArguments();
        if (arguments != null) {
          regionPath = (String) arguments;
          Cache cache = CacheFactory.getAnyInstance();
          Region<?, ?> region = cache.getRegion(regionPath);
          region.destroyRegion();
          String regionName = regionPath.startsWith(Region.SEPARATOR) ? regionPath.substring(1) : regionPath;
          XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", regionName);
          context.getResultSender().lastResult(new CliFunctionResult("", xmlEntity, regionPath));
        }
      }
      context.getResultSender().lastResult(new CliFunctionResult("", false, "FAILURE"));
    } catch (IllegalStateException e) {
      context.getResultSender().lastResult(new CliFunctionResult("", e, null));
    } catch (Exception ex) {
      context.getResultSender().lastResult(new CliFunctionResult("", new RuntimeException(CliStrings.format(CliStrings.DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1, new Object[] {regionPath, ex.getMessage()})), null));
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  } 
}
