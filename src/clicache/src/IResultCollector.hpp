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


#pragma once

#include "gf_defs.hpp"

using namespace System;
namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

			 interface class IGFSerializable;
      /*
      generic<class TKey>
      ref class ResultCollector;
      */

      /// <summary>
      /// collect function execution results, can be overriden 
      /// </summary>
      generic<class TResult>
      public interface class IResultCollector
      {
      public:

        /// <summary>
        /// add result from a single function execution
        /// </summary>
        void AddResult(  TResult rs );

        /// <summary>
        /// get result 
        /// </summary>
        System::Collections::Generic::ICollection<TResult>^  GetResult(); 

        /// <summary>
        /// get result 
        /// </summary>
        System::Collections::Generic::ICollection<TResult>^  GetResult(UInt32 timeout); 

        /// <summary>
        ///Call back provided to caller, which is called after function execution is
        ///complete and caller can retrieve results using getResult()
        /// </summary>
  //generic<class TKey>
	void EndResults(); 

  /// <summary>
  ///GemFire will invoke this method before re-executing function (in case of
  /// Function Execution HA) This is to clear the previous execution results from
   /// the result collector
  /// @since 6.5
  /// </summary>
  //generic<class TKey>
  void ClearResults(/*bool*/);

      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

