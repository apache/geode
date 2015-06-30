/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import static com.gemstone.gemfire.internal.cache.extension.mock.MockExtensionXmlParser.*;

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
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/**
 * Function to create {@link MockCacheExtension} on a {@link Region}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public class AlterMockCacheExtensionFunction extends FunctionAdapter {

  private static final long serialVersionUID = 1L;

  public static final Function INSTANCE = new AlterMockCacheExtensionFunction();

  @Override
  public void execute(FunctionContext context) {
    final Cache cache = CacheFactory.getAnyInstance();

    if (!(cache instanceof Extensible)) {
      throw new FunctionException("Not extensible cache.");
    }

    final String value = (String) ((Object[]) context.getArguments())[0];

    @SuppressWarnings("unchecked")
    final Extensible<Cache> extensible = (Extensible<Cache>) cache;
    for (Extension<Cache> extension : extensible.getExtensionPoint().getExtensions()) {
      if (extension instanceof MockCacheExtension) {
        ((MockCacheExtension) extension).setValue(value);
      }
    }

    final XmlEntity xmlEntity = XmlEntity.builder().withType(ELEMENT_CACHE).withNamespace(PREFIX, NAMESPACE).build();

    final ResultSender<Object> resultSender = context.getResultSender();
    final String memberNameOrId = CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings.format("Mock cache extension altered on \"{0}\"",
        new Object[] { memberNameOrId })));
  }

  @Override
  public String getId() {
    return AlterMockCacheExtensionFunction.class.getName();
  }

  /**
   * @param value
   * @return
   * @since 8.1
   */
  public static Object[] toArgs(final String value) {
    return new Object[] { value };
  }
}