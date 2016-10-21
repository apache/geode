/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache.extension.mock;

import static org.apache.geode.internal.cache.extension.mock.MockExtensionXmlParser.*;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * Function to create {@link MockCacheExtension} on a {@link Region}.
 * 
 *
 * @since GemFire 8.1
 */
public class CreateMockCacheExtensionFunction extends FunctionAdapter {

  private static final long serialVersionUID = 1L;

  public static final Function INSTANCE = new CreateMockCacheExtensionFunction();

  @Override
  public void execute(FunctionContext context) {
    final Cache cache = CacheFactory.getAnyInstance();

    if (!(cache instanceof Extensible)) {
      throw new FunctionException("Not extensible cache.");
    }

    final String value = (String) ((Object[]) context.getArguments())[0];

    @SuppressWarnings("unchecked")
    final Extensible<Cache> extensible = (Extensible<Cache>) cache;
    final MockCacheExtension extension = new MockCacheExtension(value);
    extension.beforeCreate(extensible, cache);
    extension.onCreate(extensible, extensible);

    final XmlEntity xmlEntity =
        XmlEntity.builder().withType(ELEMENT_CACHE).withNamespace(PREFIX, NAMESPACE).build();

    final ResultSender<Object> resultSender = context.getResultSender();
    final String memberNameOrId =
        CliUtil.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());

    resultSender.lastResult(new CliFunctionResult(memberNameOrId, xmlEntity, CliStrings
        .format("Mock cache extension created on \"{0}\"", new Object[] {memberNameOrId})));
  }

  @Override
  public String getId() {
    return CreateMockCacheExtensionFunction.class.getName();
  }

  /**
   * @param value
   * @return
   * @since GemFire 8.1
   */
  public static Object[] toArgs(final String value) {
    return new Object[] {value};
  }
}
