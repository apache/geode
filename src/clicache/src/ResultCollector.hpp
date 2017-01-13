/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#pragma once

#include "gf_defs.hpp"
#include "IResultCollector.hpp"
#include <gfcpp/ResultCollector.hpp>
#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

     generic<class TResult>
	   interface class IResultCollector;

      /// <summary>
      /// collect function execution results, default collector
      /// </summary>
     generic<class TResult>
      public ref class ResultCollector
        : public Internal::SBWrap<gemfire::ResultCollector>, public IResultCollector<TResult>
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
        inline ResultCollector( gemfire::ResultCollector* nativeptr ):
        SBWrap( nativeptr ){ }

        /// <summary>
        /// Used to assign the native Serializable pointer to a new object.
        /// </summary>
        /// <remarks>
        /// Note the order of preserveSB() and releaseSB(). This handles the
        /// corner case when <c>m_nativeptr</c> is same as <c>nativeptr</c>.
        /// </remarks>
        inline void AssignSPGeneric( gemfire::ResultCollector* nativeptr )
        {
          AssignPtr( nativeptr );
        }

        /// <summary>
        /// Used to assign the native CqListener pointer to a new object.
        /// </summary>
        inline void SetSPGeneric( gemfire::ResultCollector* nativeptr )
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
