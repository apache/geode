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
package com.gemstone.gemfire.internal.admin;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.admin.jmx.internal.StatAlertsAggregator;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.admin.remote.AlertsNotificationMessage;
import com.gemstone.gemfire.internal.admin.remote.UpdateAlertDefinitionMessage;
import com.gemstone.gemfire.internal.admin.statalerts.DummyStatisticInfoImpl;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfoImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * The alert manager maintains the list of alert definitions (added by client
 * e.g GFMon 2.0).
 * 
 * It retrieved the value of statistic( defined in alert definition) and notify
 * alert aggregator sitting on admin VM
 * 
 * @see StatAlertDefinition
 * @see StatAlert
 * 
 * 
 * @since 5.7
 */
public class StatAlertsManager {
  private static final Logger logger = LogService.getLogger();
  
  /**
   * Instance for current DM
   * 
   * Guarded by StatAlertsManager.class
   */
  private static StatAlertsManager alertManager;

  /**
   * Guarded by this
   */
  private long refreshInterval;

  /**
   * Guarded by this.alertDefinitionsMap
   */
  protected final HashMap alertDefinitionsMap = new HashMap();

  /**
   * Guarded by this
   */
  private SystemTimer timer;

  /**
   * Guarded by this
   */
  private boolean refreshAtFixedRate;

  /**
   * Provides life cycle support
   */
  protected final DistributionManager dm;
  
  private StatAlertsManager(DistributionManager dm) {
    this.dm = dm;
    logger.info(LocalizedMessage.create(LocalizedStrings.StatAlertsManager_STATALERTSMANAGER_CREATED));
  }

  /**
   * @return singleton instance of StatAlertsManager
   */
  public synchronized static StatAlertsManager getInstance(DistributionManager dm) {
    // As per current implementation set up request will be send only once ,
    // when member joined to Admin distributed system
    // we don't need to care about race condition
    if (alertManager != null && alertManager.dm == dm) {
      return alertManager;
    }
    if (alertManager != null) {
      alertManager.close();
    }
    
    /* 
     * Throw DistributedSystemDisconnectedException if cancel operation is in 
     * progress 
     */
    dm.getCancelCriterion().checkCancelInProgress(null);
    
    alertManager = new StatAlertsManager(dm);
    return alertManager;
  }
  
  /**
   * Nullifies the StatAlertsManager instance.
   */
  private synchronized static void closeInstance() {
    StatAlertsManager.alertManager = null;
  }

  /**
   * 
   * Update the alert's definition map
   * 
   * @param defns
   *                Alert definitions
   * @param actionCode
   *                Action to be performed like add , remove or update alert's
   *                definition
   * 
   * @see UpdateAlertDefinitionMessage
   */
  public void updateAlertDefinition(StatAlertDefinition[] defns, int actionCode) {
    if (logger.isDebugEnabled()) {
      logger.debug("Entered StatAlertsManager.updateAlertDefinition *****");
    }
    synchronized (alertDefinitionsMap) {
      if (actionCode == UpdateAlertDefinitionMessage.REMOVE_ALERT_DEFINITION) {
        for (int i = 0; i < defns.length; i++) {
          alertDefinitionsMap.remove(Integer.valueOf(defns[i].getId()));
          if (logger.isDebugEnabled()) {
            logger.debug("Removed StatAlertDefinition: {}", defns[i].getName());
          }
        }
      }
      else {
        StatAlertDefinition[] alertDefns = this
            .createMemberStatAlertDefinition(dm, defns);
        StatAlertDefinition defn;
        for (int i = 0; i < alertDefns.length; i++) {
          defn = alertDefns[i];
          alertDefinitionsMap.put(Integer.valueOf(defns[i].getId()), defn);
        }
      }
    } // synchronized

    if (logger.isDebugEnabled()) {
      logger.debug("Exiting StatAlertsManager.updateAlertDefinition *****");
    }
  }

  private synchronized void rescheduleTimer() {
    // cancel the old timer. Although cancelled, old task might execute one last
    // time
    if (timer != null)
      timer.cancel();

    // Get the swarm.  Currently rather UGLY.
    InternalDistributedSystem system = dm.getSystem();
    if (system == null || system.getDistributionManager() != dm) {
      throw new com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException(
          "This manager has been cancelled");
    }
    // start and schedule new timer
    timer = new SystemTimer(system /*swarm*/, true);

    EvaluateAlertDefnsTask task = new EvaluateAlertDefnsTask();
    if (refreshAtFixedRate) {
      timer.scheduleAtFixedRate(task, 0, refreshInterval);
    }
    else {
      timer.schedule(task, 0, refreshInterval);
    }
  }
  
  /**
   * Set refresh time interval also cancel the previous {@link TimerTask} and
   * create new timer task based on ner refresh time interval
   * 
   * @param interval
   *                Refresh time interval
   */
  public synchronized void setRefreshTimeInterval(long interval) {
    refreshInterval = interval;
    rescheduleTimer();
  }

  /**
   * 
   * @return time interval alert generation
   */
  public synchronized long getRefreshTimeInterval() {
    return refreshInterval;
  }

  /**
   * @return true if refresh for timer has to be fixed rate see
   *         scheduleAtFixedRate method of {@link TimerTask}
   */
  public synchronized boolean isRefreshAtFixedRate() {
    return refreshAtFixedRate;
  }

  /**
   * set true if refresh for timer has to be fixed rate see scheduleAtFixedRate
   * method of {@link TimerTask}
   *
   * TODO never called
   * 
   * @param refreshAtFixedRate
   */
  public synchronized void setRefreshAtFixedRate(boolean refreshAtFixedRate) {
    this.refreshAtFixedRate = refreshAtFixedRate;
    rescheduleTimer();
  }

  /**
   * Query all the statistic defined by alert definition and notify alerts
   * aggregator if at least one statistic value crosses the threshold defined in
   * alert definition
   * 
   */
  protected StatAlert[] getAlerts() {
    Set alerts = new HashSet();

    synchronized (alertDefinitionsMap) {
      Set keyset = alertDefinitionsMap.keySet();
      Iterator iter = keyset.iterator();
      StatAlert alert;
      Date now = new Date();
      while (iter.hasNext()) {
        Integer key = (Integer)iter.next();
        StatAlertDefinition defn = (StatAlertDefinition)alertDefinitionsMap
            .get(key);
        alert = defn.evaluateAndAlert();
        if (alert != null) {
          alert.setTime(now);
          alerts.add(alert);
          if (logger.isDebugEnabled()) {
            logger.debug("getAlerts:  found alert {}", alert);
          }
        }
      } // while
    } // synchronized
    return (StatAlert[])alerts.toArray(new StatAlert[alerts.size()]);
  }

  /**
   * Convert {@link StatAlertDefinition }(Created by client like GFMon2.0) with
   * {@link DummyStatisticInfoImpl} to StatAlertDefinition with
   * {@link StatisticInfoImpl}
   */
  private StatAlertDefinition[] createMemberStatAlertDefinition(
      DistributionManager dm, StatAlertDefinition[] defns) {
    dm.getCancelCriterion().checkCancelInProgress(null);

    Statistics[] statistics;
    StatisticsType type;
    StatisticDescriptor desc;
    String textId;
    boolean skipDefinition = false;
    List result = new ArrayList();

    for (int i = 0; i < defns.length; i++) {
      skipDefinition = false;
      StatAlertDefinition defn = defns[i];
      StatisticInfo[] statInfos = defn.getStatisticInfo();
      for (int ii = 0; ii < statInfos.length && !skipDefinition; ii++) {
        textId = statInfos[ii].getStatisticsTextId();

        // TODO If none by TextID, use StatType and getAll.
        statistics = dm.getSystem().findStatisticsByTextId(textId);
        if (statistics.length == 0) {
          logger.error(LocalizedMessage.create(
              LocalizedStrings.StatAlertsManager_STATALERTSMANAGER_CREATEMEMBERSTATALERTDEFINITION_STATISTICS_WITH_GIVEN_TEXTID_0_NOT_FOUND, textId));
          skipDefinition = true;
//          break;
          continue; // To print all errors
        }

        type = statistics[0].getType();
        desc = type.nameToDescriptor(statInfos[ii].getStatisticName());
        // Replace the actual StatInfo object
        statInfos[ii] = new StatisticInfoImpl(statistics[0], desc);
        if (logger.isDebugEnabled()) {
          logger.debug("StatAlertsManager.createMemberStatAlertDefinition: created statInfo {}", statInfos[ii]);
        }
      } // for

      if (!skipDefinition) {
        defn.setStatisticInfo(statInfos);
        result.add(defn);
        if (logger.isDebugEnabled()) {
          logger.debug("StatAlertsManager.createMemberStatAlertDefinition :: {}", defns[i].getStringRepresentation());
        }
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("StatAlertsManager.createMemberStatAlertDefinition :: StatAlertDefinition {} is excluded", defn.getName());
        }
      }
    } // for

    return (StatAlertDefinition[])
        result.toArray(new StatAlertDefinition[result.size()]);
  }
  
  /**
   * Shut down this instance
   */
  protected synchronized void close() {
    // nullify the manager instance first
    closeInstance();

    // cancel the old timer. Although canceled, old task might execute one last
    // time
    if (timer != null) {
      timer.cancel();
    }
    timer = null;
  }

  /**
   * Timer task to send all the alerts raised to {@link StatAlertsAggregator}
   * 
   */
  class EvaluateAlertDefnsTask extends SystemTimerTask {
    /**
     * Collect all the alerts raised and send it to {@link StatAlertsAggregator}
     */
    @Override
    public void run2() {
      final boolean isDebugEnabled = logger.isDebugEnabled();
      
      synchronized (StatAlertsManager.this) {
        if (dm.getCancelCriterion().cancelInProgress() != null) {
          return;
        }
        
        //start alert notification are supposed to send to all the 
        //admin agents exists in the system.
        //For the DS without agent, alert manager should not create 
        //any alert notifications
        Set adminMemberSet = dm.getAdminMemberSet();
        if (adminMemberSet == null || adminMemberSet.isEmpty())
          return;

        if (isDebugEnabled) {
          logger.debug("EvaluateAlertDefnsTask: starting");
        }
        try {
          StatAlert[] alerts = getAlerts();
          if (alerts.length == 0) {
            if (isDebugEnabled) {
              logger.debug("EvaluateAlertsDefnsTask: no alerts");
            }
            return;
          }

          AlertsNotificationMessage request = new AlertsNotificationMessage();
          request.setAlerts(alerts);
          if (isDebugEnabled) {
            Iterator iterator = adminMemberSet.iterator();
            while (iterator.hasNext()) {
              logger.debug("EvaluateAlertDefnsTask: sending {} alerts to {}", alerts.length, iterator.next());
            }
          }
          request.setRecipients(adminMemberSet);
          dm.putOutgoing(request);
        }
        catch (CancelException e) {
          logger.debug("EvaluateAlertDefnsTask: system closed: {}", e.getMessage(), e);
          close();
        }
        catch (Exception e) {
          logger.error(LocalizedMessage.create(LocalizedStrings.StatAlertsManager_EVALUATEALERTDEFNSTASK_FAILED_WITH_AN_EXCEPTION),  e);
          close();
        }
        if (isDebugEnabled) {
          logger.debug("EvaluateAlertDefnsTask: done ");
        }
      }
    } // run
  } // EvaluateAlertDefnsTask
}
