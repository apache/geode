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
package org.apache.geode.security.templates;

import org.apache.geode.cache.query.Query;
import org.apache.geode.security.PostProcessor;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 * This is example that implements PostProcessor
 */
public class SamplePostProcessor implements PostProcessor {

  @Override
  public void init(final Properties securityProps) {}

  /**
   * This simply modifies the value with all the parameter values
   *
   * @param principal The principal that's accessing the value
   * @param regionName The region that's been accessed. This could be null.
   * @param key the key of the value that's been accessed. This could be null.
   * @param value the value, this could be null.
   * @return the processed value
   */
  @Override
  public Object processRegionValue(Object principal, String regionName, Object key, Object value) {
    return getUsername(principal) + "/" + regionName + "/" + key + "/" + value;
  }

  private String getUsername(final Object principal) {
    String name = null;
    if (principal instanceof Principal) {
      name = ((Principal) principal).getName();
    } else {
      name = principal.toString();
    }
    return name;
  }

  /**
   * Modify the values within the result set, prepending the user and the list of regions used in
   * the query.
   *
   * @param principal The principal that's accessing the value
   * @param query The query object that was executed
   * @param regions A collection of region names
   * @param results The result(s) of the query
   * @return the post-processed results
   */
  @Override
  public Collection<Object> processQueryResult(final Object principal, final Query query,
      final Collection<String> regions, final Collection<Object> results) {
    final String name = getUsername(principal);
    Collection<Object> processedResults = new ArrayList<>(results.size());
    StringBuilder stringBuilder = new StringBuilder(100);
    for (Object region : regions) {
      stringBuilder.append(region);
    }
    for (Object result : results) {
      processedResults.add(name + stringBuilder.toString() + "/" + result);
    }
    return processedResults;
  }


}
