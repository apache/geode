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
package org.apache.geode.cache.execute;

/**
 * Application developers can extend this class instead of implementing the
 * {@link Function} interface.
 * 
 * <p>
 * This implementation provides the following defaults
 * </p>
 * <ol>
 * <li>{@link Function#hasResult()} returns true</li>
 * <li>{@link Function#optimizeForWrite()} returns false</li>
 * <li>{@link Function#isHA()} returns true</li>
 * </ol>
 * </p>
 * 
 * @since GemFire 6.0
 * @see Function
 * @deprecated Use {@link Function} instead. Function has default
 * methods that now mimic the behavior of FunctionAdapter.
 * 
 */
public abstract class FunctionAdapter implements Function {
}
