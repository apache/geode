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
package com.company.data;
/**
 * A <code>Declarable</code> <code>ObjectSizer</code> for used for XML testing
 *
 * @since 5.0
 */
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.util.ObjectSizer;

public class MySizer implements ObjectSizer, Declarable {

  String name;

  public int sizeof( Object o ) {
    return ObjectSizer.DEFAULT.sizeof(o);
  }

  public void init(Properties props) {
      this.name = props.getProperty("name", "defaultName");
  }
}
