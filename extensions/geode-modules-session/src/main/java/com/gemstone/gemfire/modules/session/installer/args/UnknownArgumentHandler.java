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

package com.gemstone.gemfire.modules.session.installer.args;

/**
 * Interface defining unknown argument handlers, given the opportunity to either
 * ignore the issue or force the parameter to be dealt with.
 */
public interface UnknownArgumentHandler {

  /**
   * Called when an unknown argument is supplied.
   *
   * @param form   argument name used
   * @param params parameters passed into it
   * @throws UsageException when the user needs to fix it
   */
  void handleUnknownArgument(String form, String[] params)
      throws UsageException;

}
