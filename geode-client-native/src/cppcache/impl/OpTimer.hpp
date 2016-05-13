/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GEMFIRE_IMPL_OPTIMER_HPP_
#define _GEMFIRE_IMPL_OPTIMER_HPP_

#include "../gfcpp_globals.hpp"
#include <ace/OS.h>
#include <ace/Time_Value.h>
#include <ace/High_Res_Timer.h>

namespace gemfire {

/** Use as a stack object to time an operation. 

So to use:

  OpTimer optime;
  // do something measurable...
  region->put( foobar, barfoo );
  int64_t elapsedMillis = optime.finish();

*/
class OpTimer 
{
  private:
#if DEBUG
    ACE_Time_Value m_start;
#endif
    
  public:
  
    inline OpTimer( )
#if DEBUG
    : m_start( ACE_High_Res_Timer::gettimeofday_hr() )
#endif
    {
    }

//  Does absolutely nothing.
//    ~OpTimer( )
//    {
//    }

    inline int64_t finish()
    {
#if DEBUG
      ACE_Time_Value end = ACE_High_Res_Timer::gettimeofday_hr();
      end -= m_start;
      return end.msec();
#else
      return 0;
#endif
    }

};

}


#endif

