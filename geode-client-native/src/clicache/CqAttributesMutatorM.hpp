/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CqAttributesMutator.hpp"
#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
	  interface class ICqListener;

      /// <summary>
      /// Supports modification of certain cq attributes after the cq
      /// has been created.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
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
        void AddCqListener( ICqListener^ cqListener );

        /// <summary>
        /// Remove a CqListener for the cq.
        /// </summary>
        
        void RemoveCqListener(ICqListener^ aListener);


        /// <summary>
	/// Initialize with an array of listeners
        /// </summary>
        
        void SetCqListeners(array<ICqListener^>^ newListeners);


      internal:
        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static CqAttributesMutator^ Create( gemfire::CqAttributesMutator* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew CqAttributesMutator( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqAttributesMutator( gemfire::CqAttributesMutator* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
