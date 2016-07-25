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
package com.gemstone.gemfire.internal.process;

import javax.management.ObjectName;

import com.gemstone.gemfire.internal.process.ProcessController.Arguments;

/**
 * Defines {@link ProcessController} {@link Arguments} that must be implemented
 * to support the {@link MBeanProcessController}.

 * @since GemFire 8.0
 */
interface MBeanControllerParameters extends Arguments {
  public ObjectName getNamePattern();
  public String getPidAttribute();
  public String getStatusMethod();
  public String getStopMethod();
  public String[] getAttributes();
  public Object[] getValues();
}
