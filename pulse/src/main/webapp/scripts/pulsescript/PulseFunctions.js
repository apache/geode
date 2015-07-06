/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @name PulseFunctions.js
 * @author Ashutosh Zambare.
 * @version 1.0
 *
 */

/**
  @class A thin JavaScript client that accesses the pulse services. It provides an abstraction layer to communicate with the system and returns json object as a result.

  @constructor

 */
function PulseFunctions() {

}

/**
 @description get pulse version function
 @param responseBack Indicates which function to be called when a response is received. 
 @example
    var pulseVersionBack = function(r, jsonResponse) { ... // handle response };
    pf.pulseVersion(pulseVersionBack, "param1", "param2");
*/
PulseFunctions.prototype.pulseVersion = function(responseBack) {
    var po = new Object();
    po.traceNo = UUIDv4();
    ajaxPost("pulse/pulseVersion", po, responseBack);
};

PulseFunctions.prototype.CluserAlertNotificationFunc = function (funcName, postData) {
  var qp = new Object();
  qp.pageNumber = $('#pageNumber').val();
  postData["SystemAlerts"] = qp;
};

PulseFunctions.prototype.ClusterKeyStatisticsFunc = function (funcName, postData) {
  var qp = new Object();
  //postData[funcName] = qp;
  postData["ClusterKeyStatistics"] = qp;
};

PulseFunctions.prototype.ClusterMembersFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterMembers"] = qp;
};

PulseFunctions.prototype.ClusterJVMPAusesFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterJVMPauses"] = qp;
};

PulseFunctions.prototype.ClusterWanInformationFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterWANInfo"] = qp;
};

PulseFunctions.prototype.ClusterMemoryUsageFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterMemoryUsage"] = qp;
};

PulseFunctions.prototype.ClusterDiskThroughputFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterDiskThroughput"] = qp;
};

PulseFunctions.prototype.PulseVersionDetailsFunc = function (funcName, postData) {
  var qp = new Object();
  postData["PulseVersion"] = qp;

};

PulseFunctions.prototype.CluserBasicDetailsFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterDetails"] = qp;
};


PulseFunctions.prototype.ClusterMembersRGraphFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterMembersRGraph"] = qp;
};

PulseFunctions.prototype.ClusterRegionFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterRegion"] = qp;
};

PulseFunctions.prototype.ClusterRegionsFunc = function (funcName, postData) {
  var qp = new Object();
  postData["ClusterRegions"] = qp;
};

PulseFunctions.prototype.ClearAllAlertsFunc = function (funcName, postData) {
  var qp = new Object();
  qp.alertType = -1;
  postData["ClearAllAlerts"] = qp;
};

PulseFunctions.prototype.MemberGatewayHubFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberGatewayHub"] = qp;
};

PulseFunctions.prototype.MemberAsynchEventQueuesFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberAsynchEventQueues"] = qp;
};

PulseFunctions.prototype.MemberDiskThroughputFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberDiskThroughput"] = qp;
};

PulseFunctions.prototype.MemberHeapUsageFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberHeapUsage"] = qp;
};

PulseFunctions.prototype.MemberClientsFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberClients"] = qp;
};

PulseFunctions.prototype.MemberRegionSummaryFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberRegions"] = qp;
};

PulseFunctions.prototype.MemberGCPausesFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberGCPauses"] = qp;
};

PulseFunctions.prototype.MemberKeyStatisticsFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberKeyStatistics"] = qp;
};

PulseFunctions.prototype.MembersListFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  postData["MembersList"] = qp;
};

PulseFunctions.prototype.MemberDetailsFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.memberId = memberId;
  qp.memberName = memberName;
  postData["MemberDetails"] = qp;
};

PulseFunctions.prototype.QueryStatisticsFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  // later send filter, page etc params here
  //qp.memberId = memberId;
  //qp.memberName = memberName;
  postData["QueryStatistics"] = qp;
};

PulseFunctions.prototype.ClusterSelectedRegionFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.regionFullPath = regionFullPath;
  postData["ClusterSelectedRegion"] = qp;
};

PulseFunctions.prototype.ClusterSelectedRegionsMemberFunc = function (funcName, postData) {
  getRequestParams();
  var qp = new Object();
  qp.regionFullPath = regionFullPath;
  postData["ClusterSelectedRegionsMember"] = qp;
};

/*
PulseFunctions.prototype.ClusterMembersFunc = function (funcName, postData) {
  var qp = new Object();
  qp.param1 = "7777";
  qp.param2 = 1;
  postData["ClusterMembers"] = qp;
};*/
