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

package templates.security;

import java.util.HashSet;

/**
 * This is a sample class for objects which hold information of the authorized
 * function names and authorized value for the optimizeForWrite.
 * 
 * @author Aneesh Karayil
 * @since 6.0
 */
public class FunctionSecurityPrmsHolder {

  private final Boolean isOptimizeForWrite;

  private final HashSet<String> functionIds;

  private final HashSet<String> keySet;

  public FunctionSecurityPrmsHolder(Boolean isOptimizeForWrite,
      HashSet<String> functionIds, HashSet<String> keySet) {
    this.isOptimizeForWrite = isOptimizeForWrite;
    this.functionIds = functionIds;
    this.keySet = keySet;
  }

  public Boolean isOptimizeForWrite() {
    return isOptimizeForWrite;
  }

  public HashSet<String> getFunctionIds() {
    return functionIds;
  }

  public HashSet<String> getKeySet() {
    return keySet;
  }
}
