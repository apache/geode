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
#include "IResultCollector.hpp"
#include <gfcpp/ResultCollector.hpp>
#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

     generic<class TResult>
	   interface class IResultCollector;

      /// <summary>
      /// collect function execution results, default collector
      /// </summary>
     generic<class TResult>
      public ref class ResultCollector
        : public Internal::SBWrap<apache::geode::client::ResultCollector>, public IResultCollector<TResult>
      {
      public:

        /// <summary>
        /// add result from a single function execution
        /// </summary>
        virtual void AddResult( TResult rs );

        /// <summary>
        /// get result 
        /// </summary>
        virtual System::Collections::Generic::ICollection<TResult>^  GetResult(); 

        /// <summary>
        /// get result 
        /// </summary>
        virtual System::Collections::Generic::ICollection<TResult>^  GetResult(UInt32 timeout); 

        /// <summary>
        ///Call back provided to caller, which is called after function execution is
        ///complete and caller can retrieve results using getResult()
        /// </summary>
  //generic<class TKey>
	virtual void EndResults(); 

  //generic<class TKey>
  virtual void ClearResults();

      internal:

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline ResultCollector( ):
        SBWrap( ){ }

        //~ResultCollector<TKey>( ) { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline ResultCollector( apache::geode::client::ResultCollector* nativeptr ):
        SBWrap( nativeptr ){ }

        /// <summary>
        /// Used to assign the native Serializable pointer to a new object.
        /// </summary>
        /// <remarks>
        /// Note the order of preserveSB() and releaseSB(). This handles the
        /// corner case when <c>m_nativeptr</c> is same as <c>nativeptr</c>.
        /// </remarks>
        inline void AssignSPGeneric( apache::geode::client::ResultCollector* nativeptr )
        {
          AssignPtr( nativeptr );
        }

        /// <summary>
        /// Used to assign the native CqListener pointer to a new object.
        /// </summary>
        inline void SetSPGeneric( apache::geode::client::ResultCollector* nativeptr )
        {
          if ( nativeptr != nullptr ) {
            nativeptr->preserveSB( );
          }
          _SetNativePtr( nativeptr );
        }

        //void Silence_LNK2022_BUG() { };

      };

    }
  }
}
 } //namespace 
