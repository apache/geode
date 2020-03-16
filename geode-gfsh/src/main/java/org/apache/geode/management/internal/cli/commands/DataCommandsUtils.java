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

package org.apache.geode.management.internal.cli.commands;

import java.util.List;
import java.util.Set;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.domain.DataCommandRequest;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;

public class DataCommandsUtils {

  static String makeBrokenJsonCompliant(String json) {
    if (json == null) {
      return null;
    }

    if (json.startsWith("(") && json.endsWith(")")) {
      return "{" + json.substring(1, json.length() - 1) + "}";
    } else {
      return json;
    }
  }

  static DataCommandResult callFunctionForRegion(DataCommandRequest request,
      DataCommandFunction putfn, Set<DistributedMember> members) {
    if (members.size() == 1) {
      DistributedMember member = members.iterator().next();
      @SuppressWarnings("unchecked")
      ResultCollector<Object, List<Object>> collector =
          FunctionService.onMember(member).setArguments(request).execute(putfn);
      List<Object> list = collector.getResult();
      Object object = list.get(0);
      if (object instanceof Throwable) {
        Throwable error = (Throwable) object;
        DataCommandResult result = new DataCommandResult();
        result.setErorr(error);
        result.setErrorString(error.getMessage());
        return result;
      }
      DataCommandResult result = (DataCommandResult) list.get(0);
      result.aggregate(null);
      return result;
    } else {
      @SuppressWarnings("unchecked")
      ResultCollector<Object, List<Object>> collector =
          FunctionService.onMembers(members).setArguments(request).execute(putfn);
      List<Object> list = collector.getResult();
      DataCommandResult result = null;
      for (Object object : list) {
        if (object instanceof Throwable) {
          Throwable error = (Throwable) object;
          result = new DataCommandResult();
          result.setErorr(error);
          result.setErrorString(error.getMessage());
          return result;
        }

        if (result == null) {
          result = (DataCommandResult) object;
          result.aggregate(null);
        } else {
          result.aggregate((DataCommandResult) object);
        }
      }
      return result;
    }
  }
}
