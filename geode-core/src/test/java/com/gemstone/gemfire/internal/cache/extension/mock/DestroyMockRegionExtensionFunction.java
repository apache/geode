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