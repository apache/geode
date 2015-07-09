/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.extension.Extension;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * Function to destroy {@link MockRegionExtension} on a {@link Region}.
 * 
 * <dl>
 * <dt>Arguments:</dt>
 * <dd>
 * <dl>
 * <dt>{@link String} regionName</dt>
 * <dd>Name of region on which to destroy {@link MockCacheExtension}.</dd>
 * </dl>
 * </dt>
 * </dl>
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public class DestroyMockRegionExtensionFunction extends FunctionAdapter {

  private static final long serialVersionUID = 1L;
  public static final Function INSTANCE = new DestroyMockRegionExtensionFunction();

  @Override
  public void execute(FunctionContext context) {
    final Cache cache = CacheFactory.getAnyInstance();

    final Region<?, ?> region = cache.getRegion((String) ((Object[]) context.getArguments())[0]);
    if (!(region instanceof Extensible)) {
      throw new FunctionException("Not extensible region.");
    }

    @SuppressWarnings("unchecked")
    final Extensible<Region<?, ?>> extensible = (Extensible<Region<?, ?>>) region;
    for (Extension<Region<?, ?>> extension : extensible.getExtensionPoint().getExtensions()) {
      if (extension instanceof MockRegionExtension) {
        extensible.getExtensionPoint().removeExtension(extension);
        break;
      }
    }

    XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", region.getName());

    final ResultSender<Object> resultSender = context.getResultSender();
    final String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings.format("Mock region extension \"{0}\" destroyed on \"{1}\"",
        new Object[] { region.getFullPath(), memberNameOrId })));

  }

  @Override
  public String getId() {
    return DestroyMockRegionExtensionFunction.class.getName();
  }

  /**
   * @param regionName
   * @return
   * @since 8.1
   */
  public static Object[] toArgs(final String regionName) {
    return new Object[] { regionName };
  }
}