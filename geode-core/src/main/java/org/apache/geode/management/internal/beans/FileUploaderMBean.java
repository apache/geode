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

package org.apache.geode.management.internal.beans;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.healthmarketscience.rmiio.RemoteInputStream;

import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
    operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
public interface FileUploaderMBean {

  List<String> uploadFile(Map<String, RemoteInputStream> remoteFiles) throws IOException;

  void deleteFiles(List<String> files);
}
