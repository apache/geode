/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "gf_defs.hpp"
#include "impl/NativeWrapper.hpp"
#include <gfcpp/statistics/StatisticDescriptor.hpp>

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// A class that describes an individual statistic whose value is updated by an
      /// application and may be archived by GemFire. These descriptions are gathered
      /// together in a <see cref="StatisticsType" /> class.
      /// </summary>
      /// <para>
      /// To get an instance of this interface use an instance of
      /// <see cref="StatisticsFactory" /> class.
      /// </para>
      /// <para>
      /// StatisticDescriptors are naturally ordered by their name.
      /// </para>
      public ref class StatisticDescriptor sealed
        : public Internal::UMWrap<gemfire_statistics::StatisticDescriptor>
       {
         public:
           /// <summary>
           /// Returns the id of this statistic in a <see cref="StatisticsType" /> class.
           /// The id is initialized when its statistics
           /// type is created.
           /// </summary>
           virtual property int32_t ID
           {
             virtual int32_t get( );
           }

           /// <summary>
           /// Returns the name of this statistic
           /// </summary>
           virtual property String^ Name
           {
             virtual String^ get( );
           }

           /// <summary>
           /// Returns the description of this statistic
           /// </summary>
           virtual property String^ Description
           {
             virtual String^ get( );
           }

           /// <summary>
           /// Returns true if this statistic is a counter; false if its a gauge.
           /// Counter statistics have values that always increase.
           /// Gauge statistics have unconstrained values.
           /// </summary>
           virtual property int8_t IsCounter
           {
             virtual int8_t get( );
           }

           /// <summary>
           /// Returns true if a larger statistic value indicates better performance.
           /// </summary>
           virtual property int8_t IsLargerBetter
           {
             virtual int8_t get( );
           }

           /// <summary>
           /// Returns the unit in which this statistic is measured.
           /// </summary>
           virtual property String^ Unit
           {
             virtual String^ get( );
           }

           internal:
           /// <summary>
           /// Internal factory function to wrap a native object pointer inside
           /// this managed class, with null pointer check.
           /// </summary>
           /// <param name="nativeptr">native object pointer</param>
           /// <returns>
           /// the managed wrapper object, or null if the native pointer is null.
           /// </returns>
           inline static StatisticDescriptor^ Create(
           gemfire_statistics::StatisticDescriptor* nativeptr )
           {
             return ( nativeptr != nullptr ?
             gcnew StatisticDescriptor( nativeptr ) : nullptr );
           }
           
           private:
           /// <summary>
           /// Private constructor to wrap a native object pointer
           /// </summary>
           /// <param name="nativeptr">The native object pointer</param>
           inline StatisticDescriptor( gemfire_statistics::StatisticDescriptor* nativeptr )
           : UMWrap( nativeptr, false ) { }
       };      
    }
  }
} } //namespace 

