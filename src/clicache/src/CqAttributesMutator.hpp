/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqAttributesMutator.hpp>
#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;
using namespace System::Threading;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TResult>
	  interface class ICqListener;

      generic<class TKey, class TResult>
      private ref class CqListenerHelper sealed{
        public:
        static IDictionary<Generic::ICqListener<TKey, TResult>^, IntPtr>^
          m_ManagedVsUnManagedCqLstrDict = gcnew 
          Dictionary<Generic::ICqListener<TKey, TResult>^, IntPtr>();

        static ReaderWriterLock^ g_readerWriterLock = gcnew ReaderWriterLock();
      };

      /// <summary>
      /// Supports modification of certain cq attributes after the cq
      /// has been created.
      /// </summary>
      generic<class TKey, class TResult>
      public ref class CqAttributesMutator sealed
        : public Internal::SBWrap<gemfire::CqAttributesMutator>
      {
      public:

        /// <summary>
        /// Adds the CqListener for the cq.
        /// </summary>
        /// <param name="cqListener">
        /// user-defined cq listener, or null for no cache listener
        /// </param>
        void AddCqListener( Generic::ICqListener<TKey, TResult>^ cqListener );

        /// <summary>
        /// Remove a CqListener for the cq.
        /// </summary>
        
        void RemoveCqListener(Generic::ICqListener<TKey, TResult>^ aListener);


        /// <summary>
	/// Initialize with an array of listeners
        /// </summary>
        
        void SetCqListeners(array<Generic::ICqListener<TKey, TResult>^>^ newListeners);


      internal:
        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Generic::CqAttributesMutator<TKey, TResult>^ Create( gemfire::CqAttributesMutator* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew Generic::CqAttributesMutator<TKey, TResult>( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqAttributesMutator<TKey, TResult>( gemfire::CqAttributesMutator* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
