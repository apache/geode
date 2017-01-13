/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once



#include "../gf_defs.hpp"

#ifdef _WIN32
#define snprintf _snprintf
#endif


using namespace System;

namespace GemStone
{
  namespace GemFire
	{namespace Cache { namespace Generic{

    ref class ManagedString sealed
    {
    private:

      IntPtr m_str;


    public:

      // Constructors

      inline ManagedString( String^ str )
      {
        m_str = (str == nullptr) ? IntPtr::Zero :
          System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi( str );
      }

      // Destructor

      inline ~ManagedString( )
      {
        if (m_str != IntPtr::Zero)
        {
          System::Runtime::InteropServices::Marshal::FreeHGlobal( m_str );
        }
      }

      // The finalizer should normally never be called; either use non-pointer object
      // or call delete explicitly.
      !ManagedString( )
      {
        if (m_str != IntPtr::Zero)
        {
          System::Runtime::InteropServices::Marshal::FreeHGlobal( m_str );
        }
#if GF_DEVEL_ASSERTS == 1
        throw gcnew System::ApplicationException(
          "Finalizer for ManagedString should not have been called!!" );
#endif
      }

      inline static String^ Get( const char* str )
      {
        return ((str == nullptr) ? nullptr : gcnew String( str ));
      }

      inline static String^ Get( const wchar_t* str )
      {
        return ((str == nullptr) ? nullptr : gcnew String( str ));
      }

      // Properties

      property const char* CharPtr
      {
        inline const char* get( )
        {
          return ((m_str == IntPtr::Zero) ? nullptr :
            static_cast<const char*>( m_str.ToPointer( ) ));
        }
      }
    };
	}}
  }
}
