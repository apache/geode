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

package org.apache.geode.management.api;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheElement;

/**
 * provides additional information about a restful service request beyond the minimum required in
 * {#link RestfulEndpoint}, namely the return type to expect when `list` or other operations are
 * performed
 */

// "type parameter is never used" warning is suppressed, because actually it is used to create
// the linkage between request and response type in the signature of ClusterManagementService.list
@SuppressWarnings("unused")
@Experimental
public interface RespondsWith<R extends CacheElement> {
}
