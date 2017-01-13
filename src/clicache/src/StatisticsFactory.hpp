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
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include <gfcpp/statistics/Statistics.hpp>

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      ref class StatisticDescriptor;
      ref class StatisticsType;
      ref class Statistics; 

      /// <summary>
      /// Instances of this interface provide methods that create instances
      /// of <see cref="StatisticDescriptor" /> and <see cref="StatisticsType" />.
      /// Every <see cref="StatisticsFactory" /> is also a type factory.
      /// </summary>
      /// <para>
      /// A <c>StatisticsFactory</c> can create a <see cref="StatisticDescriptor" />
      /// statistic of three numeric types:
      /// <c>int</c>, <c>long</c>, and <c>double</c>.  A
      /// statistic (<c>StatisticDescriptor</c>) can either be a
      /// <I>gauge</I> meaning that its value can increase and decrease or a
      /// <I>counter</I> meaning that its value is strictly increasing.
      /// Marking a statistic as a counter allows the GemFire Manager Console
      /// to properly display a statistics whose value "wraps around" (that
      /// is, exceeds its maximum value).
      /// </para>
      public ref class StatisticsFactory sealed
        : public Internal::UMWrap<gemfire_statistics::StatisticsFactory>
      {
        protected:
          StatisticsFactory(){}
          StatisticsFactory(StatisticsFactory^){}
      public:
          /// <summary>
          /// Return a pre-existing statistics factory. Typically configured through
          /// creation of a distributed system.
          /// </summary>
          static StatisticsFactory^ GetExistingInstance();

          /// <summary>
          /// Creates and returns an int counter  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>, and with larger values indicating better performance.
          /// </summary>
          virtual StatisticDescriptor^ CreateIntCounter( String^ name, String^ description, String^ units, int8_t largerBetter );

          /// <summary>
          /// Creates and returns an int counter  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>.
          /// </summary>
          virtual StatisticDescriptor^ CreateIntCounter( String^ name, String^ description, String^ units );

          /// <summary>
          /// Creates and returns an long counter  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>, and with larger values indicating better performance.
          /// </summary>
          virtual StatisticDescriptor^ CreateLongCounter( String^ name, String^ description, String^ units, int8_t largerBetter );

          /// <summary>
          /// Creates and returns an long counter  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>.
          /// </summary>
          virtual StatisticDescriptor^ CreateLongCounter( String^ name, String^ description, String^ units );

          /// <summary>
          /// Creates and returns an double counter  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>, and with larger values indicating better performance.
          /// </summary>

          virtual StatisticDescriptor^ CreateDoubleCounter( String^ name, String^ description, String^ units, int8_t largerBetter );

          /// <summary>
          /// Creates and returns an double counter  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>.
          /// </summary>
          virtual StatisticDescriptor^ CreateDoubleCounter( String^ name, String^ description, String^ units );

          /// <summary>
          /// Creates and returns an int gauge  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>, and with smaller values indicating better performance.
          /// </summary>
          virtual StatisticDescriptor^ CreateIntGauge( String^ name, String^ description, String^ units, int8_t largerBetter );

          /// <summary>
          /// Creates and returns an int gauge  <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>.
          /// </summary>
          virtual StatisticDescriptor^ CreateIntGauge( String^ name, String^ description, String^ units );

          /// <summary>
          /// Creates and returns an long gauge <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>, and with smaller values indicating better performance.
          /// </summary>
          virtual StatisticDescriptor^ CreateLongGauge( String^ name, String^ description, String^ units, int8_t largerBetter );

          /// <summary>
          /// Creates and returns an long gauge <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>.
          /// </summary>
          virtual StatisticDescriptor^ CreateLongGauge( String^ name, String^ description, String^ units );

          /// <summary>
          /// Creates and returns an double gauge <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>, and with smaller values indicating better performance.
          /// </summary>
          virtual StatisticDescriptor^ CreateDoubleGauge( String^ name, String^ description, String^ units, int8_t largerBetter );

          /// <summary>
          /// Creates and returns an double gauge <see cref="StatisticDescriptor" />
          /// with the given <c>name</c>, <c>description</c>,
          /// <c>units</c>.
          /// </summary>
          virtual StatisticDescriptor^ CreateDoubleGauge( String^ name, String^ description, String^ units );

          /// <summary>
          /// Creates and returns a <see cref="StatisticsType" /> 
          /// with the given <c>name</c>, <c>description</c>,and <see cref="StatisticDescriptor" />
          /// </summary>
          /// <exception cref="IllegalArgumentException">
          /// if a type with the given <c>name</c> already exists.
          /// </exception>
          virtual StatisticsType^ CreateType( String^ name, String^ description,
                                   array<StatisticDescriptor^>^ stats, int32 statsLength);

          /// <summary>
          /// Finds and returns an already created <see cref="StatisticsType" /> 
          /// with the given <c>name</c>. Returns <c>null</c> if the type does not exist.
          /// </summary>
          virtual StatisticsType^ FindType( String^ name );

          /// <summary>
          /// Creates and returns a <see cref="Statistics" /> instance of the given <see cref="StatisticsType" /> type, <c>textId</c>, and with default ids.
          /// </summary>
          /// <para>
          /// The created instance may not be <see cref="Statistics#isAtomic" /> atomic.
          /// </para>
          virtual Statistics^ CreateStatistics(StatisticsType^ type);

          /// <summary>
          /// Creates and returns a <see cref="Statistics" /> instance of the given <see cref="StatisticsType" /> type, <c>textId</c>, and with a default numeric id.
          /// </summary>
          /// <para>
          /// The created instance may not be <see cref="Statistics#isAtomic" /> atomic.
          /// </para>
          virtual Statistics^ CreateStatistics(StatisticsType^ type, String^ textId);

          /// <summary>
          /// Creates and returns a <see cref="Statistics" /> instance of the given <see cref="StatisticsType" /> type, <c>textId</c>, and <c>numericId</c>.
          /// </summary>
          /// <para>
          /// The created instance may not be <see cref="Statistics#isAtomic" /> atomic.
          /// </para>
          virtual Statistics^ CreateStatistics(StatisticsType^ type, String^ textId, int64_t numericId);

          /// <summary>
          /// Creates and returns a <see cref="Statistics" /> instance of the given <see cref="StatisticsType" /> type, <c>textId</c>, and with default ids.
          /// </summary>
          /// <para>
          /// The created instance will be <see cref="Statistics#isAtomic" /> atomic.
          /// </para>
          virtual Statistics^ CreateAtomicStatistics(StatisticsType^ type);

          /// <summary>
          /// Creates and returns a <see cref="Statistics" /> instance of the given <see cref="StatisticsType" /> type, <c>textId</c>, and with a default numeric id.
          /// </summary>
          /// <para>
          /// The created instance will be <see cref="Statistics#isAtomic" /> atomic.
          /// </para>
          virtual Statistics^ CreateAtomicStatistics(StatisticsType^ type, String^ textId);

          /// <summary>
          /// Creates and returns a <see cref="Statistics" /> instance of the given <see cref="StatisticsType" /> type, <c>textId</c>, and <c>numericId</c>.
          /// </summary>
          /// <para>
          /// The created instance will be <see cref="Statistics#isAtomic" /> atomic.
          /// </para>
          virtual Statistics^ CreateAtomicStatistics(StatisticsType^ type, String^ textId, int64 numericId);

          /// <summary>
          /// Return the first instance that matches the type, or NULL
          /// </summary>
          virtual Statistics^ FindFirstStatisticsByType( StatisticsType^ type );
          
          /// <summary>
          /// Returns a name that can be used to identify the manager
          /// </summary>
          virtual property String^ Name
          {
            virtual String^ get( );
          }

          /// <summary>
          /// Returns a numeric id that can be used to identify the manager
          /// </summary>
          virtual property int64_t ID
          {
            virtual int64_t get( );
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
           inline static StatisticsFactory^ Create(
           gemfire_statistics::StatisticsFactory* nativeptr )
           {
             return ( nativeptr != nullptr ?
             gcnew StatisticsFactory( nativeptr ) : nullptr );
           }

         private:
           /// <summary>
           /// Private constructor to wrap a native object pointer
           /// </summary>
           /// <param name="nativeptr">The native object pointer</param>
           inline StatisticsFactory( gemfire_statistics::StatisticsFactory* nativeptr )
           : UMWrap( nativeptr, false ) { }
      };
    }
  }
} } //namespace 

