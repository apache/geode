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
#include "impl/NativeWrapper.hpp"
#include <gfcpp/statistics/StatisticDescriptor.hpp>

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
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
        : public Internal::UMWrap<apache::geode::statistics::StatisticDescriptor>
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
           apache::geode::statistics::StatisticDescriptor* nativeptr )
           {
             return ( nativeptr != nullptr ?
             gcnew StatisticDescriptor( nativeptr ) : nullptr );
           }
           
           private:
           /// <summary>
           /// Private constructor to wrap a native object pointer
           /// </summary>
           /// <param name="nativeptr">The native object pointer</param>
           inline StatisticDescriptor( apache::geode::statistics::StatisticDescriptor* nativeptr )
           : UMWrap( nativeptr, false ) { }
       };      
    }
  }
} } //namespace 

