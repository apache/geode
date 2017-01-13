/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "ICqListener.hpp"
//#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
  generic<class TKey, class TResult>
	ref class CqEvent;
	//interface class ICqListener;

      /// <summary>
      /// This class wraps the native C++ <c>gemfire::Serializable</c> objects
      /// as managed <see cref="../../IGFSerializable" /> objects.
      /// </summary>
      generic<class TKey, class TResult>    
      public ref class CqListener
        : public Internal::SBWrap<gemfire::CqListener>, public ICqListener<TKey, TResult>
      {
      public:

        /// <summary>
        /// Invoke on an event
        /// </summary>
	virtual void OnEvent( CqEvent<TKey, TResult>^ ev) 
	{
	}

        /// <summary>
        /// Invoke on an error
        /// </summary>
	virtual void OnError( CqEvent<TKey, TResult>^ ev) 
	{
	}

        /// <summary>
        /// Invoke on close
        /// </summary>
	virtual void Close()
	{
	}

      internal:

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline CqListener<TKey, TResult>( )
          : SBWrap( ) { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqListener<TKey, TResult>( gemfire::CqListener* nativeptr )
          : SBWrap( nativeptr ) { }

        /// <summary>
        /// Used to assign the native Serializable pointer to a new object.
        /// </summary>
        /// <remarks>
        /// Note the order of preserveSB() and releaseSB(). This handles the
        /// corner case when <c>m_nativeptr</c> is same as <c>nativeptr</c>.
        /// </remarks>
        inline void AssignSP( gemfire::CqListener* nativeptr )
        {
          AssignPtr( nativeptr );
        }

        /// <summary>
        /// Used to assign the native CqListener pointer to a new object.
        /// </summary>
        inline void SetSP( gemfire::CqListener* nativeptr )
        {
          if ( nativeptr != nullptr ) {
            nativeptr->preserveSB( );
          }
          _SetNativePtr( nativeptr );
        }

      };

    }
  }
}
 } //namespace 
