/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "MemoryPressureHandler.hpp"
#include "windows.h"
#include "psapi.h"
#include "../Log.hpp"

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      int64_t g_prevUnmanagedSize = 0;

      int MemoryPressureHandler::handle_timeout( const ACE_Time_Value&
          current_time, const void* arg )
      {
        HANDLE hProcess = GetCurrentProcess( );

        PROCESS_MEMORY_COUNTERS pmc;

        if ( GetProcessMemoryInfo( hProcess, &pmc, sizeof(pmc)) ) {
          int64_t totalmem  = (int64_t)pmc.WorkingSetSize;
          int64_t curr_managed_size = GC::GetTotalMemory( false );
          int64_t curr_unmanagedMemory = totalmem - curr_managed_size;
          Log::Finest( "Current total memory usage: {0}, managed memory: {1}, "
              "unmanaged memory: {2}", totalmem, curr_managed_size,
              curr_unmanagedMemory );
          if ( curr_unmanagedMemory > 0 ) {
            int64_t increase = curr_unmanagedMemory - g_prevUnmanagedSize;
            if ( Math::Abs( increase ) > 20*1024*1024 ) {
              if ( increase > 0 ) {
                Log::Fine( "Adding memory pressure information to assist .NET GC: {0} bytes", increase );
                GC::AddMemoryPressure( increase );
              }
              else {
                Log::Fine( "Removing memory pressure information to assist .NET GC: {0} bytes", -increase );
                GC::RemoveMemoryPressure( -increase );
              }
              g_prevUnmanagedSize = curr_unmanagedMemory;
            }
          }
        }
        else {
          return -1;
        }
        return 0;
      }

      int MemoryPressureHandler::handle_close( ACE_HANDLE handle,
          ACE_Reactor_Mask close_mask )
      {
        return 0;
      }
    }
  }
}
 } //namespace 
