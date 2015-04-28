/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import com.gemstone.gemfire.internal.admin.GemFireVM;
import com.gemstone.gemfire.internal.admin.StatAlert;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;

/**
 * This interface represents an Aggregator entity and resides in JMXAgent.
 * Responsibilities are as follows:
 * <ol>
 * <li> set AlertsManager in the newly joined members
 * <li> create/update/remove alert
 * <li> manage refresh interval
 * <li> process notification from members
 * <li> Aggregate stats & make available for clients thro' JMXAgent
 * </ol>
 * 
 * @author abhishek
 */
public interface StatAlertsAggregator {

  /**
   * This method can be used to get an alert definition.
   * 
   * @param alertDefinition
   *                StatAlertDefinition to retrieve
   * @return StatAlertDefinition
   */
  public StatAlertDefinition getAlertDefinition(
      StatAlertDefinition alertDefinition);

  /**
   * This method can be used to retrieve all available stat alert definitions.
   * 
   * @return An array of all available StatAlertDefinition objects
   */
  public StatAlertDefinition[] getAllStatAlertDefinitions();

  /**
   * This method can be used to update alert definition for the Stat mentioned.
   * This method should update the collection maintained at the aggregator and
   * should notify members for the newly added alert definitions.
   * <p>
   * A new alert definition will be created if matching one not found.
   * 
   * @param alertDefinition
   *                alertDefinition to be updated
   */
  public void updateAlertDefinition(StatAlertDefinition alertDefinition);

  /**
   * This method can be used to remove alert definition for the Stat mentioned.
   * <p>
   * This method should update the collection maintained at the aggregator and
   * should notify members for the newly added alert definitions.
   * 
   * @param defId
   *                id of the alert definition to be removed
   */
  public void removeAlertDefinition(Integer defId);

  /**
   * Convenience method to check whether an alert definition is created.
   * 
   * @param alert
   *                alert definition to check whether already created
   * @return true if the alert definition is already created, false otherwise
   */
  public boolean isAlertDefinitionCreated(StatAlertDefinition alert);

  /**
   * This method can be used to set the AlertManager for the newly joined member
   * VM.
   * 
   * @param memberVM
   *                Member VM to set AlertsManager for
   */
  public void setAlertsManager(GemFireVM memberVM);

  /**
   * Returns the refresh interval for the Stats in seconds.
   * 
   * @return refresh interval for the Stats(in seconds)
   */
  public int getRefreshIntervalForStatAlerts();

  /**
   * This method is used to set the refresh interval for the Stats Alerts in
   * seconds
   * 
   * @param refreshInterval
   *                refresh interval for the Stats(in seconds)
   */
  public void setRefreshIntervalForStatAlerts(int refreshInterval);

  /**
   * This method can be used to process the notifications sent by the member(s).
   * Actual aggregation of stats can occur here. The array contains alert
   * objects with alert def. ID & value. AlertHelper class can be used to
   * retrieve the corresponding alert definition.
   * 
   * @param alerts
   *                array of Alert class(contains alert def. ID & value)
   * @param remoteVM
   */
  public void processNotifications(StatAlert[] alerts, GemFireVM remoteVM);

  public void processSystemwideNotifications();
}
