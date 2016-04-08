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
package com.gemstone.gemfire.internal;

/**
 * A marker interface to identify internal objects that are stored in the Cache at the same place that user objects
 * would typically be stored. For example, registering functions for internal use. When determining what to do, or how
 * to display these objects, other classes may use this interface as a filter to eliminate internal objects.
 * 
 * @since 7.0
 * 
 */
public interface InternalEntity {

}
