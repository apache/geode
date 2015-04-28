/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.support;

import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * The DistributedSystemAdapter class is an adapter extending DistributedSystem to provide default behavior for the
 * abstract methods when testing.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.DistributedSystem
 * @since 8.0
 */
@SuppressWarnings("unused")
public abstract class DistributedSystemAdapter extends DistributedSystem {

  @Override
  public LogWriter getLogWriter() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public LogWriter getSecurityLogWriter() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Properties getProperties() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Properties getSecurityProperties() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public void disconnect() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean isConnected() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public boolean isReconnecting() {
    return false;
  }
  
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    return false;
  }
  
  @Override
  public void stopReconnecting() {
  }
  
  @Override
  public DistributedSystem getReconnectedSystem() {
    return null;
  }

  @Override
  public long getId() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getMemberId() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public DistributedMember getDistributedMember() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Set<DistributedMember> getAllOtherMembers() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Set<DistributedMember> getGroupMembers(final String group) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics createStatistics(final StatisticsType type) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics createStatistics(final StatisticsType type, final String textId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics createStatistics(final StatisticsType type, final String textId, final long numericId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics createAtomicStatistics(final StatisticsType type) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics createAtomicStatistics(final StatisticsType type, final String textId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics createAtomicStatistics(final StatisticsType type, final String textId, final long numericId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics[] findStatisticsByType(final StatisticsType type) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics[] findStatisticsByTextId(final String textId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Statistics[] findStatisticsByNumericId(final long numericId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createIntCounter(final String name, final String description, final String units) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createLongCounter(final String name, final String description, final String units) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createDoubleCounter(final String name, final String description, final String units) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createIntGauge(final String name, final String description, final String units) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createLongGauge(final String name, final String description, final String units) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createDoubleGauge(final String name, final String description, final String units) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createIntCounter(final String name, final String description, final String units, final boolean largerBetter) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createLongCounter(final String name, final String description, final String units, final boolean largerBetter) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createDoubleCounter(final String name, final String description, final String units, final boolean largerBetter) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createIntGauge(final String name, final String description, final String units, final boolean largerBetter) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createLongGauge(final String name, final String description, final String units, final boolean largerBetter) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticDescriptor createDoubleGauge(final String name, final String description, final String units, final boolean largerBetter) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public StatisticsType createType(final String name, final String description, final StatisticDescriptor[] stats) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override public StatisticsType findType(final String name) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override public StatisticsType[] createTypesFromXml(final Reader reader) throws IOException {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Set<DistributedMember> findDistributedMembers(InetAddress address) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public DistributedMember findDistributedMember(String name) {
    throw new UnsupportedOperationException("Not Implemented!");
  }
  
  

}
