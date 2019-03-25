/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.admin;

import static org.apache.geode.internal.DSFIDFactory.registerDSFID;
import static org.apache.geode.internal.DataSerializableFixedID.ADD_HEALTH_LISTENER_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.ADD_HEALTH_LISTENER_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.ADMIN_CACHE_EVENT_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.ADMIN_CONSOLE_DISCONNECT_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.ADMIN_CONSOLE_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.ADMIN_DESTROY_REGION_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.ALERTS_NOTIF_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.ALERT_LEVEL_CHANGE_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.ALERT_LISTENER_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.APP_CACHE_SNAPSHOT_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.BRIDGE_SERVER_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.BRIDGE_SERVER_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.CACHE_CONFIG_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.CACHE_CONFIG_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.CACHE_INFO_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.CACHE_INFO_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.CANCELLATION_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.CHANGE_REFRESH_INT_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.CLIENT_HEALTH_STATS;
import static org.apache.geode.internal.DataSerializableFixedID.CLIENT_MEMBERSHIP_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.COMPACT_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.COMPACT_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.DESTROY_ENTRY_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.DURABLE_CLIENT_INFO_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.DURABLE_CLIENT_INFO_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_DIST_LOCK_INFO_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_DIST_LOCK_INFO_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_HEALTH_DIAGNOSIS_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_HEALTH_DIAGNOSIS_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_HOST_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_HOST_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_RESOURCE_ATTRIBUTES_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_RESOURCE_ATTRIBUTES_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_STATS_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_STATS_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_SYS_CFG_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.FETCH_SYS_CFG_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.FLUSH_APP_CACHE_SNAPSHOT_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.HEALTH_LISTENER_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.LICENSE_INFO_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.LICENSE_INFO_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.OBJECT_DETAILS_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.OBJECT_DETAILS_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.OBJECT_NAMES_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.OBJECT_NAMES_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REFRESH_MEMBER_SNAP_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REFRESH_MEMBER_SNAP_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_ATTRIBUTES_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_ATTRIBUTES_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_SIZE_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_SIZE_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_STATISTICS_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_STATISTICS_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_SUB_SIZE_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REGION_SUB_SIZE_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.REMOVE_HEALTH_LISTENER_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.REMOVE_HEALTH_LISTENER_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.RESET_HEALTH_STATUS_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.RESET_HEALTH_STATUS_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.ROOT_REGION_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.ROOT_REGION_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.SHUTDOWN_ALL_GATEWAYHUBS_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.SNAPSHOT_RESULT_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.STAT_ALERTS_MGR_ASSIGN_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.STAT_ALERT_DEFN_GAUGE_THRESHOLD;
import static org.apache.geode.internal.DataSerializableFixedID.STAT_ALERT_DEFN_NUM_THRESHOLD;
import static org.apache.geode.internal.DataSerializableFixedID.STAT_ALERT_NOTIFICATION;
import static org.apache.geode.internal.DataSerializableFixedID.STORE_SYS_CFG_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.STORE_SYS_CFG_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.SUB_REGION_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.SUB_REGION_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.TAIL_LOG_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.TAIL_LOG_RESPONSE;
import static org.apache.geode.internal.DataSerializableFixedID.UPDATE_ALERTS_DEFN_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.VERSION_INFO_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.VERSION_INFO_RESPONSE;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

import sun.management.resources.agent;

import org.apache.geode.admin.internal.SystemMemberCacheEventProcessor;
import org.apache.geode.admin.jmx.internal.StatAlertNotification;
import org.apache.geode.distributed.internal.DistributedSystemService;
import org.apache.geode.distributed.internal.HealthMonitor;
import org.apache.geode.distributed.internal.HealthMonitors;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.AddHealthListenerRequest;
import org.apache.geode.internal.admin.remote.AddHealthListenerResponse;
import org.apache.geode.internal.admin.remote.AdminConsoleDisconnectMessage;
import org.apache.geode.internal.admin.remote.AdminConsoleMessage;
import org.apache.geode.internal.admin.remote.AlertLevelChangeMessage;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.admin.remote.AlertsNotificationMessage;
import org.apache.geode.internal.admin.remote.AppCacheSnapshotMessage;
import org.apache.geode.internal.admin.remote.BridgeServerRequest;
import org.apache.geode.internal.admin.remote.BridgeServerResponse;
import org.apache.geode.internal.admin.remote.CacheConfigRequest;
import org.apache.geode.internal.admin.remote.CacheConfigResponse;
import org.apache.geode.internal.admin.remote.CacheInfoRequest;
import org.apache.geode.internal.admin.remote.CacheInfoResponse;
import org.apache.geode.internal.admin.remote.CancellationMessage;
import org.apache.geode.internal.admin.remote.ChangeRefreshIntervalMessage;
import org.apache.geode.internal.admin.remote.ClientHealthStats;
import org.apache.geode.internal.admin.remote.CompactRequest;
import org.apache.geode.internal.admin.remote.CompactResponse;
import org.apache.geode.internal.admin.remote.DestroyEntryMessage;
import org.apache.geode.internal.admin.remote.DestroyRegionMessage;
import org.apache.geode.internal.admin.remote.DurableClientInfoRequest;
import org.apache.geode.internal.admin.remote.DurableClientInfoResponse;
import org.apache.geode.internal.admin.remote.FetchDistLockInfoRequest;
import org.apache.geode.internal.admin.remote.FetchDistLockInfoResponse;
import org.apache.geode.internal.admin.remote.FetchHealthDiagnosisRequest;
import org.apache.geode.internal.admin.remote.FetchHealthDiagnosisResponse;
import org.apache.geode.internal.admin.remote.FetchHostRequest;
import org.apache.geode.internal.admin.remote.FetchHostResponse;
import org.apache.geode.internal.admin.remote.FetchResourceAttributesRequest;
import org.apache.geode.internal.admin.remote.FetchResourceAttributesResponse;
import org.apache.geode.internal.admin.remote.FetchStatsRequest;
import org.apache.geode.internal.admin.remote.FetchStatsResponse;
import org.apache.geode.internal.admin.remote.FetchSysCfgRequest;
import org.apache.geode.internal.admin.remote.FetchSysCfgResponse;
import org.apache.geode.internal.admin.remote.FlushAppCacheSnapshotMessage;
import org.apache.geode.internal.admin.remote.HealthListenerMessage;
import org.apache.geode.internal.admin.remote.LicenseInfoRequest;
import org.apache.geode.internal.admin.remote.LicenseInfoResponse;
import org.apache.geode.internal.admin.remote.ObjectDetailsRequest;
import org.apache.geode.internal.admin.remote.ObjectDetailsResponse;
import org.apache.geode.internal.admin.remote.ObjectNamesRequest;
import org.apache.geode.internal.admin.remote.ObjectNamesResponse;
import org.apache.geode.internal.admin.remote.RefreshMemberSnapshotRequest;
import org.apache.geode.internal.admin.remote.RefreshMemberSnapshotResponse;
import org.apache.geode.internal.admin.remote.RegionAttributesRequest;
import org.apache.geode.internal.admin.remote.RegionAttributesResponse;
import org.apache.geode.internal.admin.remote.RegionRequest;
import org.apache.geode.internal.admin.remote.RegionResponse;
import org.apache.geode.internal.admin.remote.RegionSizeRequest;
import org.apache.geode.internal.admin.remote.RegionSizeResponse;
import org.apache.geode.internal.admin.remote.RegionStatisticsRequest;
import org.apache.geode.internal.admin.remote.RegionStatisticsResponse;
import org.apache.geode.internal.admin.remote.RegionSubRegionSizeRequest;
import org.apache.geode.internal.admin.remote.RegionSubRegionsSizeResponse;
import org.apache.geode.internal.admin.remote.RemoteAlert;
import org.apache.geode.internal.admin.remote.RemoteGemFireVM;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent;
import org.apache.geode.internal.admin.remote.RemoveHealthListenerRequest;
import org.apache.geode.internal.admin.remote.RemoveHealthListenerResponse;
import org.apache.geode.internal.admin.remote.ResetHealthStatusRequest;
import org.apache.geode.internal.admin.remote.ResetHealthStatusResponse;
import org.apache.geode.internal.admin.remote.RootRegionRequest;
import org.apache.geode.internal.admin.remote.RootRegionResponse;
import org.apache.geode.internal.admin.remote.ShutdownAllGatewayHubsRequest;
import org.apache.geode.internal.admin.remote.SnapshotResultMessage;
import org.apache.geode.internal.admin.remote.StatAlertsManagerAssignMessage;
import org.apache.geode.internal.admin.remote.StoreSysCfgRequest;
import org.apache.geode.internal.admin.remote.StoreSysCfgResponse;
import org.apache.geode.internal.admin.remote.SubRegionRequest;
import org.apache.geode.internal.admin.remote.SubRegionResponse;
import org.apache.geode.internal.admin.remote.TailLogRequest;
import org.apache.geode.internal.admin.remote.TailLogResponse;
import org.apache.geode.internal.admin.remote.UpdateAlertDefinitionMessage;
import org.apache.geode.internal.admin.remote.VersionInfoRequest;
import org.apache.geode.internal.admin.remote.VersionInfoResponse;
import org.apache.geode.internal.admin.statalerts.GaugeThresholdDecoratorImpl;
import org.apache.geode.internal.admin.statalerts.NumberThresholdDecoratorImpl;
import org.apache.geode.management.internal.AlertDetails;

public class GeodeAdminDistributedSystemService implements DistributedSystemService {

  /**
   * The administration agent associated with this distribution manager.
   */
  private volatile RemoteGfManagerAgent agent;
  private HealthMonitors healthMonitors;

  @Override
  public void init(InternalDistributedSystem internalDistributedSystem) {
    healthMonitors = new HealthMonitors(internalDistributedSystem);
    registerAlertListener(internalDistributedSystem);
    registerDataSerializables();
  }

  @Override
  public Class getInterface() {
    return getClass();
  }

  @Override
  public Collection<String> getSerializationAcceptlist() throws IOException {
    URL sanctionedSerializables = ClassPathLoader.getLatest().getResource(getClass(),
        "sanctioned-geode-old-admin-serializables.txt");
    return InternalDataSerializer.loadClassNames(sanctionedSerializables);
  }

  private void registerAlertListener(InternalDistributedSystem system) {
    system.addResourceListener(new ResourceEventsListener() {
      @Override
      public void handleEvent(ResourceEvent event, Object resource) {
        if (event == ResourceEvent.SYSTEM_ALERT) {
          AlertDetails details = (AlertDetails) resource;
          if (agent != null) {
            RemoteGemFireVM manager = agent.getMemberById(details.getSender());

            Alert alert =
                new RemoteAlert(manager, details.getAlertLevel(), details.getMsgTime(),
                    details.getConnectionName(), details.getConnectionName(), details.getTid(),
                    details.getMsg(), details.getExceptionText(), details.getSender());

            agent.callAlertListener(alert);
          }
        }
      }
    });
  }

  public void setAgent(RemoteGfManagerAgent agent) {
    this.agent = agent;
  }

  public RemoteGfManagerAgent getAgent() {
    return agent;
  }

  public HealthMonitors getHealthMonitors() {
    return this.healthMonitors;
  }

  public HealthMonitor getHealthMonitor(InternalDistributedMember member) {
    return this.healthMonitors.getHealthMonitor(member);
  }

  public void removeHealthMonitor(InternalDistributedMember member, int id) {
    this.healthMonitors.removeHealthMonitor(member, id);
  }


  private static void registerDataSerializables() {
    registerDSFID(ADMIN_CACHE_EVENT_MESSAGE,
        SystemMemberCacheEventProcessor.SystemMemberCacheMessage.class);
    registerDSFID(ADD_HEALTH_LISTENER_REQUEST, AddHealthListenerRequest.class);
    registerDSFID(ADD_HEALTH_LISTENER_RESPONSE, AddHealthListenerResponse.class);
    registerDSFID(ADMIN_CONSOLE_DISCONNECT_MESSAGE, AdminConsoleDisconnectMessage.class);
    registerDSFID(ADMIN_CONSOLE_MESSAGE, AdminConsoleMessage.class);
    registerDSFID(ALERT_LEVEL_CHANGE_MESSAGE, AlertLevelChangeMessage.class);
    registerDSFID(ALERT_LISTENER_MESSAGE, AlertListenerMessage.class);
    registerDSFID(APP_CACHE_SNAPSHOT_MESSAGE, AppCacheSnapshotMessage.class);
    registerDSFID(BRIDGE_SERVER_REQUEST, BridgeServerRequest.class);
    registerDSFID(BRIDGE_SERVER_RESPONSE, BridgeServerResponse.class);
    registerDSFID(CACHE_CONFIG_REQUEST, CacheConfigRequest.class);
    registerDSFID(CACHE_CONFIG_RESPONSE, CacheConfigResponse.class);
    registerDSFID(CACHE_INFO_REQUEST, CacheInfoRequest.class);
    registerDSFID(CACHE_INFO_RESPONSE, CacheInfoResponse.class);
    registerDSFID(CANCELLATION_MESSAGE, CancellationMessage.class);
    registerDSFID(DESTROY_ENTRY_MESSAGE, DestroyEntryMessage.class);
    registerDSFID(ADMIN_DESTROY_REGION_MESSAGE, DestroyRegionMessage.class);
    registerDSFID(FETCH_DIST_LOCK_INFO_REQUEST, FetchDistLockInfoRequest.class);
    registerDSFID(FETCH_DIST_LOCK_INFO_RESPONSE, FetchDistLockInfoResponse.class);
    registerDSFID(FETCH_HEALTH_DIAGNOSIS_REQUEST, FetchHealthDiagnosisRequest.class);
    registerDSFID(FETCH_HEALTH_DIAGNOSIS_RESPONSE, FetchHealthDiagnosisResponse.class);
    registerDSFID(FETCH_HOST_REQUEST, FetchHostRequest.class);
    registerDSFID(FETCH_HOST_RESPONSE, FetchHostResponse.class);
    registerDSFID(FETCH_RESOURCE_ATTRIBUTES_REQUEST, FetchResourceAttributesRequest.class);
    registerDSFID(FETCH_RESOURCE_ATTRIBUTES_RESPONSE, FetchResourceAttributesResponse.class);
    registerDSFID(FETCH_STATS_REQUEST, FetchStatsRequest.class);
    registerDSFID(FETCH_STATS_RESPONSE, FetchStatsResponse.class);
    registerDSFID(FETCH_SYS_CFG_REQUEST, FetchSysCfgRequest.class);
    registerDSFID(FETCH_SYS_CFG_RESPONSE, FetchSysCfgResponse.class);
    registerDSFID(FLUSH_APP_CACHE_SNAPSHOT_MESSAGE, FlushAppCacheSnapshotMessage.class);
    registerDSFID(HEALTH_LISTENER_MESSAGE, HealthListenerMessage.class);
    registerDSFID(OBJECT_DETAILS_REQUEST, ObjectDetailsRequest.class);
    registerDSFID(OBJECT_DETAILS_RESPONSE, ObjectDetailsResponse.class);
    registerDSFID(OBJECT_NAMES_REQUEST, ObjectNamesRequest.class);
    registerDSFID(LICENSE_INFO_REQUEST, LicenseInfoRequest.class);
    registerDSFID(LICENSE_INFO_RESPONSE, LicenseInfoResponse.class);
    registerDSFID(OBJECT_NAMES_RESPONSE, ObjectNamesResponse.class);
    registerDSFID(REGION_ATTRIBUTES_REQUEST, RegionAttributesRequest.class);
    registerDSFID(REGION_ATTRIBUTES_RESPONSE, RegionAttributesResponse.class);
    registerDSFID(REGION_REQUEST, RegionRequest.class);
    registerDSFID(REGION_RESPONSE, RegionResponse.class);
    registerDSFID(REGION_SIZE_REQUEST, RegionSizeRequest.class);
    registerDSFID(REGION_SIZE_RESPONSE, RegionSizeResponse.class);
    registerDSFID(REGION_STATISTICS_REQUEST, RegionStatisticsRequest.class);
    registerDSFID(REGION_STATISTICS_RESPONSE, RegionStatisticsResponse.class);
    registerDSFID(REMOVE_HEALTH_LISTENER_REQUEST, RemoveHealthListenerRequest.class);
    registerDSFID(REMOVE_HEALTH_LISTENER_RESPONSE, RemoveHealthListenerResponse.class);
    registerDSFID(RESET_HEALTH_STATUS_REQUEST, ResetHealthStatusRequest.class);
    registerDSFID(RESET_HEALTH_STATUS_RESPONSE, ResetHealthStatusResponse.class);
    registerDSFID(ROOT_REGION_REQUEST, RootRegionRequest.class);
    registerDSFID(ROOT_REGION_RESPONSE, RootRegionResponse.class);
    registerDSFID(SNAPSHOT_RESULT_MESSAGE, SnapshotResultMessage.class);
    registerDSFID(STORE_SYS_CFG_REQUEST, StoreSysCfgRequest.class);
    registerDSFID(STORE_SYS_CFG_RESPONSE, StoreSysCfgResponse.class);
    registerDSFID(SUB_REGION_REQUEST, SubRegionRequest.class);
    registerDSFID(SUB_REGION_RESPONSE, SubRegionResponse.class);
    registerDSFID(TAIL_LOG_REQUEST, TailLogRequest.class);
    registerDSFID(TAIL_LOG_RESPONSE, TailLogResponse.class);
    registerDSFID(VERSION_INFO_REQUEST, VersionInfoRequest.class);
    registerDSFID(VERSION_INFO_RESPONSE, VersionInfoResponse.class);
    registerDSFID(STAT_ALERTS_MGR_ASSIGN_MESSAGE, StatAlertsManagerAssignMessage.class);
    registerDSFID(UPDATE_ALERTS_DEFN_MESSAGE, UpdateAlertDefinitionMessage.class);
    registerDSFID(REFRESH_MEMBER_SNAP_REQUEST, RefreshMemberSnapshotRequest.class);
    registerDSFID(REFRESH_MEMBER_SNAP_RESPONSE, RefreshMemberSnapshotResponse.class);
    registerDSFID(REGION_SUB_SIZE_REQUEST, RegionSubRegionSizeRequest.class);
    registerDSFID(REGION_SUB_SIZE_RESPONSE, RegionSubRegionsSizeResponse.class);
    registerDSFID(CHANGE_REFRESH_INT_MESSAGE, ChangeRefreshIntervalMessage.class);
    registerDSFID(ALERTS_NOTIF_MESSAGE, AlertsNotificationMessage.class);
    registerDSFID(DURABLE_CLIENT_INFO_REQUEST, DurableClientInfoRequest.class);
    registerDSFID(DURABLE_CLIENT_INFO_RESPONSE, DurableClientInfoResponse.class);
    registerDSFID(STAT_ALERT_DEFN_NUM_THRESHOLD, NumberThresholdDecoratorImpl.class);
    registerDSFID(STAT_ALERT_DEFN_GAUGE_THRESHOLD, GaugeThresholdDecoratorImpl.class);
    registerDSFID(CLIENT_HEALTH_STATS, ClientHealthStats.class);
    registerDSFID(STAT_ALERT_NOTIFICATION, StatAlertNotification.class);
    registerDSFID(CLIENT_MEMBERSHIP_MESSAGE, ClientMembershipMessage.class);
    registerDSFID(COMPACT_REQUEST, CompactRequest.class);
    registerDSFID(COMPACT_RESPONSE, CompactResponse.class);
    registerDSFID(SHUTDOWN_ALL_GATEWAYHUBS_REQUEST, ShutdownAllGatewayHubsRequest.class);
  }
}
