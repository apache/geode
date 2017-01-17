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

#ifndef _GF_NATIVETYPE_HPP
#define _GF_NATIVETYPE_HPP

/**
* This class is to test GC invocation for managed wrapper objects
* when a method on the underlying native object is still in progress.
* See bug #309 for detailed scenario.
*/
class NativeType
{
public:
  /** default constructor */
  NativeType();
  /** destructor */
  ~NativeType();
  /** test method that allocated large amounts of memory in steps */
  bool doOp(int size, int numOps, int numGCOps);
};

#endif // _GF_NATIVETYPE_HPP

