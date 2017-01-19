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
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      ref class StatisticDescriptor;

      /// <summary>
      /// This class is used to describe a logical collection of StatisticDescriptors.These descriptions
      /// are used to create an instance of <see cref="Statistics" /> class.
      /// </summary>
      /// <para>
      /// To get an instance of this interface use an instance of
      /// <see cref="StatisticsFactory" /> class.
      /// </para>
      /// <para>
      /// The class is purposefully inherited from UMWrapN and not UMWrap as the destructor
      /// of the class is protected, and so it is now not called from inside the InternalCleanup
      /// method.
      /// </para>
      public ref class StatisticsType sealed
        : public Internal::UMWrap<apache::geode::statistics::StatisticsType>
      {
      public:
        /// <summary>
        /// Returns the name of this statistics type.
        /// </summary>
        virtual property String^ Name
        {
          virtual String^ get( );
        }

        /// <summary>
        /// Returns a description of this statistics type.
        /// </summary>
        virtual property String^ Description
        {
          virtual String^ get( );
        }

        /// <summary>
        /// Returns descriptions of the statistics that this statistics type
        /// gathers together.
        /// </summary>
        virtual property array<StatisticDescriptor^>^ Statistics
        {
          virtual array<StatisticDescriptor^>^ get( );
        }

        /// <summary>
        /// Returns the id of the statistic with the given name in this
        /// statistics instance.
        /// </summary>
        /// <param name="name">the statistic name</param>
        /// <returns>the id of the statistic with the given name</returns>
        /// <exception cref="IllegalArgumentException">
        /// if no statistic named <c>name</c> exists in this
        /// statistic instance.
        /// </exception>
        virtual int32_t NameToId(String^ name);

        /// <summary>
        /// Returns the descriptor of the statistic with the given name in this
        /// statistics instance.
        /// </summary>
        /// <param name="name">the statistic name</param>
        /// <returns>the descriptor of the statistic with the given name</returns>
        /// <exception cref="IllegalArgumentException">
        /// if no statistic named <c>name</c> exists in this
        /// statistic instance.
        /// </exception>
        virtual StatisticDescriptor^ NameToDescriptor(String^ name);

        /// <summary>
        /// Returns the total number of statistics descriptors in the type.
        /// </summary>
        virtual property int32_t DescriptorsCount
        {
          virtual int32_t get( );
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
        inline static StatisticsType^ Create(
          apache::geode::statistics::StatisticsType* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew StatisticsType( nativeptr ) : nullptr );
        }

      private:
        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline StatisticsType( apache::geode::statistics::StatisticsType* nativeptr )
          : UMWrap( nativeptr, false ) { }

      };
    }
  }
}


 } //namespace 

