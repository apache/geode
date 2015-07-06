/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// CONSTANTS
var CONST_BACKEND_PRODUCT_GEMFIRE = "gemfire";
var CONST_BACKEND_PRODUCT_SQLFIRE = "gemfirexd";  // "sqlfire";

var host = '';
var port = '';
var memberId = '';
var memberName = '';
var regionFullPath = '';
var clusteRGraph;
var loadMore = false;
var productname = 'gemfire';
var currentSelectedAlertId = null;
var colorCodeForRegions = "#8c9aab"; // Default color for regions
var colorCodeForSelectedRegion = "#87b025";
var colorCodeForZeroEntryCountRegions = "#848789";

// Global reference for PulseFunctions
var pf = new PulseFunctions();

var infoAlerts = new Array();
var severAlerts = new Array();
var errorAlerts = new Array();
var warningAlerts = new Array();
var expanededNodIds = [];
var functionStartArray = [];

// global vars - for alerts
var numTotalInfoAlerts = 0;
var numTotalSeverAlerts = 0;
var numTotalErrorAlerts = 0;
var numTotalWarningAlerts = 0;

var vMode = document.documentMode;

function changeLocale(language, pagename) {
  var locale_language = language;

  // This will initialize the plugin
  // and show two dialog boxes: one with the text "hello"
  // and other with the text "Good morning John!"
  jQuery.i18n.properties({
    name : pagename,
    path : 'properties/',
    mode : 'both',
    language : locale_language,
    callback : function() {
      $('#I18NTEXT').html(msg_welcome);
    }
  });
}

function customizeUI() {

  productname = getCookie("productname");

  // common call back function for default and selected languages
  var propertiesFileLoadedCallBackFunction = function() {
    // $.holdReady(false);
    $("[data-prod-custom$='custom']").each(function() {
      var grabValue = $(this).attr('data-prod-custom');
      var customDisplayValue = jQuery.i18n.prop(grabValue);
      if ($(this).is("div")) {
        $(this).html(customDisplayValue);
      } else if ($(this).is("img")) {
        $(this).attr('src', customDisplayValue);
      } else if ($(this).is("a")) {
        $(this).attr('href', customDisplayValue);
      } else if ($(this).is("span")) {
        $(this).html(customDisplayValue);
      }
    });

  };

  // TODO : retrieve locale from VM and use it later i.e. returned from server
  var locale_language = 'en';
  jQuery.i18n.properties({
    name : [ productname, 'default' ],
    path : 'properties/',
    mode : 'map',
    cache : true,
    language : locale_language,
    callback : propertiesFileLoadedCallBackFunction
  });
}

// function used for getting list of cookies
function getCookie(c_name) {
  var i, x, y, ARRcookies = document.cookie.split(";");
  for (i = 0; i < ARRcookies.length; i++) {
    x = ARRcookies[i].substr(0, ARRcookies[i].indexOf("="));
    y = ARRcookies[i].substr(ARRcookies[i].indexOf("=") + 1);
    x = x.replace(/^\s+|\s+$/g, "");
    if (x == c_name) {
      return unescape(y);
    }
  }
}

// function used for setting cookies
function setCookie(c_name, value, exdays) {
  var exdate = new Date();
  exdate.setDate(exdate.getDate() + exdays);
  var c_value = escape(value)
      + ((exdays == null) ? "" : "; expires=" + exdate.toUTCString());
  document.cookie = c_name + "=" + c_value;
}

// function used for getting parameter from the url in js file
function GetParam(name) {
  var start = location.search.indexOf("?" + name + "=");
  if (start < 0)
    start = location.search.indexOf("&" + name + "=");
  if (start < 0)
    return '';
  start += name.length + 2;
  var end = location.search.indexOf("&", start) - 1;
  if (end < 0)
    end = location.search.length;
  var result = '';
  for ( var i = start; i <= end; i++) {
    var c = location.search.charAt(i);
    result = result + (c == '+' ? ' ' : c);
  }
  return unescape(result);
}

// function repsonseErrorHandler handles response errors
// It check if its a HTTP Unauthorized access and redirect to Login Page
function repsonseErrorHandler(data) {
  // Check for unauthorized access
  if (data.status == 401) {
    // redirect user on Login Page
    window.location.href = "Login.html?error=HTTP Error 401 : Unauthorized Access..";
  }
};

// Function for removing/clearing all the three types of alerts
function clearAlerts(alertType, flagClearAll) {

  // Hide alert details pop up if displayed
  $("#tooltip").hide();

  // update global vars - for alerts
  var requestData = {
    // "alertType" : -1,
    "clearAll" : flagClearAll
  };

  if ("severe" == alertType.toLowerCase()) { // Severe
    requestData.alertType = 0;
  } else if ("error" == alertType.toLowerCase()) { // Error
    requestData.alertType = 1;
  } else if ("warning" == alertType.toLowerCase()) { // Warning
    requestData.alertType = 2;
  } else { // All
    requestData.alertType = -1;
  }

  $.getJSON("pulse/clearAlerts", requestData, function(data) {

    // call system alerts callback handler
    getSystemAlertsBack(data);

    if (window.location.pathname.toLowerCase().indexOf('clusterdetail.html') != -1) {
      var objForceUpdateForWidgets = {
        "ClusterDetails" : {},
        "ClusterMembers" : {},
        "ClusterMembersRGraph" : {},
        "ClusterKeyStatistics" : {},
        "ClusterJVMPauses" : {},
        "ClusterWANInfo" : {},
        "ClusterMemoryUsage" : {},
        "ClusterDiskThroughput" : {}
      };
      // Call forced pulse update function
      forcePulseDataUpdate(objForceUpdateForWidgets);
    } else if (window.location.pathname.toLowerCase().indexOf('memberdetails.html') != -1) {
      var objForceUpdateForWidgets = {
        "MemberDetails" : {
          "memberId" : memberId,
          "memberName" : memberName
        }
      };
      // Call forced pulse update function
      forcePulseDataUpdate(objForceUpdateForWidgets);
    }
  }).error(repsonseErrorHandler);

  $("#allAlertScrollPane").addClass("hide-scroll-pane");
}

// function used to change the cluster status according to
// member notification alerts
function displayClusterStatus() {
  memberDetailsRefreshFlag = 0;
  if (!(document.getElementById("clusterStatusIcon") == null)) {
    if (numTotalSeverAlerts > 0) { // Severe
      $('#clusterStatusText').html("Severe");
      $("#clusterStatusIcon").addClass("severeStatus");
      if ($("#clusterStatusIcon").hasClass("errorStatus"))
        $("#clusterStatusIcon").removeClass("errorStatus");
      if ($("#clusterStatusIcon").hasClass("warningStatus"))
        $("#clusterStatusIcon").removeClass("warningStatus");
      if ($("#clusterStatusIcon").hasClass("normalStatus"))
        $("#clusterStatusIcon").removeClass("normalStatus");

    } else if (numTotalErrorAlerts > 0) { // Error
      $('#clusterStatusText').html("Error");
      $("#clusterStatusIcon").addClass("errorStatus");
      if ($("#clusterStatusIcon").hasClass("severeStatus"))
        $("#clusterStatusIcon").removeClass("severeStatus");
      if ($("#clusterStatusIcon").hasClass("warningStatus"))
        $("#clusterStatusIcon").removeClass("warningStatus");
      if ($("#clusterStatusIcon").hasClass("normalStatus"))
        $("#clusterStatusIcon").removeClass("normalStatus");
    } else if (numTotalWarningAlerts > 0) { // Warning
      $('#clusterStatusText').html("Warning");
      $("#clusterStatusIcon").addClass("warningStatus");
      if ($("#clusterStatusIcon").hasClass("severeStatus"))
        $("#clusterStatusIcon").removeClass("severeStatus");
      if ($("#clusterStatusIcon").hasClass("errorStatus"))
        $("#clusterStatusIcon").removeClass("errorStatus");
      if ($("#clusterStatusIcon").hasClass("normalStatus"))
        $("#clusterStatusIcon").removeClass("normalStatus");
    } else { // Normal
      $('#clusterStatusText').html("Normal");
      $("#clusterStatusIcon").addClass("normalStatus");
      if ($("#clusterStatusIcon").hasClass("severeStatus"))
        $("#clusterStatusIcon").removeClass("severeStatus");
      if ($("#clusterStatusIcon").hasClass("errorStatus"))
        $("#clusterStatusIcon").removeClass("errorStatus");
      if ($("#clusterStatusIcon").hasClass("warningStatus"))
        $("#clusterStatusIcon").removeClass("warningStatus");
    }

    // updating r graph
    if (flagActiveTab == "MEM_R_GRAPH_DEF") {
      var postData = new Object();
      var qp = new Object();
      postData["ClusterMembersRGraph"] = qp;
      var data = {
        "pulseData" : this.toJSONObj(postData)
      };
      $.post("pulse/pulseUpdate", data, function(data) {
        updateRGraphFlags();
        clusteRGraph.loadJSON(data.clustor);
        clusteRGraph.compute('end');
        if (vMode != 8)
          refreshNodeAccAlerts();
        clusteRGraph.refresh();
      }).error(repsonseErrorHandler);
    }
    // updating tree map
    if (flagActiveTab == "MEM_TREE_MAP_DEF") {
      var postData = new Object();
      var qp = new Object();
      postData["ClusterMembers"] = qp;
      var data = {
        "pulseData" : this.toJSONObj(postData)
      };

      $.post("pulse/pulseUpdate", data, function(data) {
        var members = data.members;
        memberCount = members.length;
        var childerensVal = [];

        for ( var i = 0; i < members.length; i++) {
          var color = "#a0c44a";
          for ( var j = 0; j < warningAlerts.length; j++) {
            if (members[i].name == warningAlerts[j].memberName) {
              color = '#ebbf0f';
              break;
            }
          }
          for ( var j = 0; j < errorAlerts.length; j++) {
            if (members[i].name == errorAlerts[j].memberName) {
              color = '#de5a25';
              break;
            }
          }
          for ( var j = 0; j < severAlerts.length; j++) {
            if (members[i].name == severAlerts[j].memberName) {
              color = '#b82811';
              break;
            }
          }
          var heapSize = members[i].currentHeapUsage;
          if (heapSize == 0)
            heapSize = 1;
          var name = "";
          name = members[i].name;
          var id = "";
          id = members[i].memberId;
          var dataVal = {
            "name" : name,
            "id" : id,
            "$color" : color,
            "$area" : heapSize,
            "cpuUsage" : members[i].cpuUsage,
            "heapUsage" : members[i].currentHeapUsage,
            "loadAvg" : members[i].loadAvg,
            "threads" : members[i].threads,
            "sockets" : members[i].sockets,
            "initial" : false
          };
          var childrenVal = {
            "children" : [],
            "data" : dataVal,
            "id" : id,
            "name" : name
          };
          childerensVal[i] = childrenVal;
        }
        var json = {
          "children" : childerensVal,
          "data" : {},
          "id" : "root",
          "name" : "Members"
        };
        clusterMemberTreeMap.loadJSON(json);
        clusterMemberTreeMap.refresh();
      }).error(repsonseErrorHandler);
    }
  }
}

/*
 * Function to extract filtering criteria/text from html elemnt. 
 * It removes extra leading and trailing white spaces and converts the 
 * criteria/text into lower case version. 
 *    
 */
function extractFilterTextFrom(elementId){
  var filterText = $('#'+elementId).val();
  // Remove leading and trailing space 
  filterText = $.trim(filterText);
  // Convert it to lowercase
  filterText = filterText.toLowerCase();
  
  if("search" == filterText){
    filterText = "";
  }
  
  return filterText;
}


/*
 * Function which applys filter on alerts list based on the filter criteria 
 * and generates html for list of alerts satisfying criteria
 */
function applyFilterOnNotificationsList(alertsType){
  
  var filteredNotifications = new Object();
  
  if('severe' == alertsType.toLowerCase()){
    var filterText = extractFilterTextFrom('filterSevereNotificationsListBox');
    var filteredSevereAlertsList = new Array();
    for(var cntr=0; cntr < severAlerts.length; cntr++){
      if(severAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || severAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredSevereAlertsList.push(severAlerts[cntr]);
      }
    }
    filteredNotifications.info = infoAlerts;
    filteredNotifications.severe = filteredSevereAlertsList;
    filteredNotifications.errors = errorAlerts;
    filteredNotifications.warnings = warningAlerts;    
  }else if('error' == alertsType.toLowerCase()){
    var filterText = extractFilterTextFrom('filterErrorNotificationsListBox');
    var filteredErrorAlertsList = new Array();
    for(var cntr=0; cntr < errorAlerts.length; cntr++){
      if(errorAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || errorAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredErrorAlertsList.push(errorAlerts[cntr]);
      }
    }
    filteredNotifications.info = infoAlerts;
    filteredNotifications.severe = severAlerts;
    filteredNotifications.errors = filteredErrorAlertsList;
    filteredNotifications.warnings = warningAlerts;
  }else if('warning' == alertsType.toLowerCase()){
    var filterText = extractFilterTextFrom('filterWarningNotificationsListBox');
    var filteredWarningAlertsList = new Array();
    for(var cntr=0; cntr < warningAlerts.length; cntr++){
      if(warningAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || warningAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredWarningAlertsList.push(warningAlerts[cntr]);
      }
    }
    filteredNotifications.info = infoAlerts;
    filteredNotifications.severe = severAlerts;
    filteredNotifications.errors = errorAlerts;
    filteredNotifications.warnings = filteredWarningAlertsList;
  }else{ // all alerts
    var filterText = extractFilterTextFrom('filterAllNotificationsListBox');
    var filteredSevereAlertsList = new Array();
    var filteredErrorAlertsList = new Array();
    var filteredWarningAlertsList = new Array();
    var filteredInfoAlertsList = new Array();
    for(var cntr=0; cntr < infoAlerts.length; cntr++){
      if(infoAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || infoAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredInfoAlertsList.push(infoAlerts[cntr]);
      }
    }
    for(var cntr=0; cntr < severAlerts.length; cntr++){
      if(severAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || severAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredSevereAlertsList.push(severAlerts[cntr]);
      }
    }
    for(var cntr=0; cntr < errorAlerts.length; cntr++){
      if(errorAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || errorAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredErrorAlertsList.push(errorAlerts[cntr]);
      }
    }
    for(var cntr=0; cntr < warningAlerts.length; cntr++){
      if(warningAlerts[cntr].description.toLowerCase().indexOf(filterText) > -1
          || warningAlerts[cntr].memberName.toLowerCase().indexOf(filterText) > -1){
        filteredWarningAlertsList.push(warningAlerts[cntr]);
      }
    }
    filteredNotifications.info = filteredInfoAlertsList;
    filteredNotifications.severe = filteredSevereAlertsList;
    filteredNotifications.errors = filteredErrorAlertsList;
    filteredNotifications.warnings = filteredWarningAlertsList;
  }
  
  var recordCount = parseInt($('#pageNumber').val()) * 100;
  generateHTMLForNotificationsList(filteredNotifications, recordCount);
}

/*
 * Function which generates html for list of notification alerts
 */
function generateHTMLForNotificationsList(notificationList, recordCount){

  var errorAlertsList = notificationList.errors;
  var severeAlertsList = notificationList.severe;
  var warningAlertsList = notificationList.warnings;
  var infoAlertsList = notificationList.info;

  var allListHTML = "";
  var allCount = 1;

  if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
    $("#allAlertScrollPane").removeClass("hide-scroll-pane");

  if (severeAlertsList != null && severeAlertsList != undefined) {

    if ($("#severeAlertScrollPane").hasClass("hide-scroll-pane"))
      $("#severeAlertScrollPane").removeClass("hide-scroll-pane");
    document.getElementById("severeTotalCount").innerHTML = severeAlertsList.length
        .toString();
    // update global count
    numTotalSeverAlerts = severeAlertsList.length;
    var severeListHTML = "";
    var severeCount = 1;

    for ( var i = numTotalSeverAlerts - 1; i >= 0; i--) {
      if (severeCount <= recordCount) {
        severeListHTML = severeListHTML
            + generateNotificationAlerts(severeAlertsList[i], "severe");
        severeCount++;
        //severAlerts[severAlerts.length] = severeAlertsList[i];
      }
      if (allCount <= recordCount) {
        allCount++;
        allListHTML = severeListHTML;
      }
    }
    document.getElementById("severeList").innerHTML = severeListHTML;

  } else {
    numTotalSeverAlerts = 0;
    document.getElementById("severeTotalCount").innerHTML = numTotalSeverAlerts;
  }

  if (errorAlertsList != null && errorAlertsList != undefined) {

    if ($("#errorAlertScrollPane").hasClass("hide-scroll-pane"))
      $("#errorAlertScrollPane").removeClass("hide-scroll-pane");
    document.getElementById("errorTotalCount").innerHTML = errorAlertsList.length;
    // update global count
    numTotalErrorAlerts = errorAlertsList.length;
    var errorListHTML = "";
    var errorCount = 1;
    for ( var i = numTotalErrorAlerts - 1; i >= 0; i--) {
      if (errorCount <= recordCount) {
        errorListHTML = errorListHTML
            + generateNotificationAlerts(errorAlertsList[i], "error");
        //errorAlerts[errorAlerts.length] = errorAlertsList[i];
        errorCount++;
      }
      if (allCount <= recordCount) {
        allCount++;
        allListHTML = allListHTML
            + generateNotificationAlerts(errorAlertsList[i], "error");
      }

    }
    document.getElementById("errorList").innerHTML = errorListHTML;

  } else {
    numTotalErrorAlerts = 0;
    document.getElementById("errorTotalCount").innerHTML = numTotalErrorAlerts;
  }

  if (warningAlertsList != null && warningAlertsList != undefined) {

    if ($("#warningAlertScrollPane").hasClass("hide-scroll-pane"))
      $("#warningAlertScrollPane").removeClass("hide-scroll-pane");
    document.getElementById("warningTotalCount").innerHTML = warningAlertsList.length;
    // update global count
    numTotalWarningAlerts = warningAlertsList.length;
    var warningListHTML = "";
    var warningCount = 1;
    for ( var i = numTotalWarningAlerts - 1; i >= 0; i--) {
      if (warningCount <= recordCount) {
        warningListHTML = warningListHTML
            + generateNotificationAlerts(warningAlertsList[i], "warning");
        //warningAlerts[warningAlerts.length] = warningAlertsList[i];
        warningCount++;
      }
      if (allCount <= recordCount) {
        allCount++;
        allListHTML = allListHTML
            + generateNotificationAlerts(warningAlertsList[i], "warning");
      }
    }
    document.getElementById("warningList").innerHTML = warningListHTML;

  } else {
    numTotalWarningAlerts = 0;
    document.getElementById("warningTotalCount").innerHTML = numTotalWarningAlerts;
  }

  if (infoAlertsList != null && infoAlertsList != undefined) {

    // update global count
    numTotalInfoAlerts = infoAlertsList.length;
    var infoCount = 1;
    for ( var i = numTotalInfoAlerts - 1; i >= 0; i--) {
      if (infoCount <= recordCount) {
        infoCount++;
      }
      if (allCount <= recordCount) {
        allCount++;
        allListHTML = allListHTML
            + generateNotificationAlerts(infoAlertsList[i], "info");
      }
    }
  } else {
    numTotalInfoAlerts = 0;
  }

  // Show/hide alert count on top ribbon
  displayAlertCounts();

  // start : for all alerts tab
  var allAlertCount = numTotalSeverAlerts + numTotalErrorAlerts
      + numTotalWarningAlerts + numTotalInfoAlerts;
  document.getElementById("allAlertCount").innerHTML = allAlertCount.toString();
  // Display Load More only when number of alerts > 100
  if (allAlertCount > 100) {
    $('#containerLoadMoreAlertsLink').show();
  } else {
    $('#containerLoadMoreAlertsLink').hide();
  }

  /*var allAlertHTML = "";

  allAlertHTML = allAlertHTML + document.getElementById("severeList").innerHTML;

  allAlertHTML = allAlertHTML + document.getElementById("errorList").innerHTML;

  allAlertHTML = allAlertHTML + document.getElementById("warningList").innerHTML;*/

  document.getElementById("allAlertList").innerHTML = allListHTML;

  // end : for all alerts tab

  if (severAlerts.length > 0) { // Severe
    if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
      $("#allAlertScrollPane").removeClass("hide-scroll-pane");
    $('#clusterStatusText').html("Severe");
    $("#clusterStatusIcon").addClass("severeStatus");
  } else if (errorAlerts.length > 0) { // Error
    if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
      $("#allAlertScrollPane").removeClass("hide-scroll-pane");
    $('#clusterStatusText').html("Error");
    $("#clusterStatusIcon").addClass("errorStatus");
  } else if (warningAlerts.length > 0) { // Warning
    if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
      $("#allAlertScrollPane").removeClass("hide-scroll-pane");
    $('#clusterStatusText').html("Warning");
    $("#clusterStatusIcon").addClass("warningStatus");
  } else { // Normal
    $('#clusterStatusText').html("Normal");
    $("#clusterStatusIcon").addClass("normalStatus");
  }
  $('.scroll-pane').jScrollPane();

  // tooltip
  $("#allAlertScrollPane .jspPane").scroll(function() {

  });
  
  // $(".tip_trigger").moveIntoView();
  $(".tip_trigger").each(function() {
    $(this).hover(function() {
      var tooltip = $(this).find('.tooltip');

      tooltip.show(100); // Show tooltip
      var p = $(this);
      var offset = p.offset();

      var t = offset.top - $(window).scrollTop();
      // var l = position.left;
      $(tooltip).css("top", t);
      $(tooltip).find('.tooltipcontent').css("height", 100);
      $(tooltip).find('.tooltipcontent').jScrollPane();
      // setTimeout($(this).show(), 500);
    }, function() {
      var tooltip = $(this).find('.tooltip');
      tooltip.hide(); // Hide tooltip

    });
  });
}
/*
 * Function show or hide alerts counts on notification ribbon
 */
function displayAlertCounts(){

  if(severAlerts.length == 0){
    $('#severeTotalCount').hide();
  }else{
    $('#severeTotalCount').show();
  }
  
  if(errorAlerts.length == 0){
    $('#errorTotalCount').hide();
  }else{
    $('#errorTotalCount').show();
  }
  
  if(warningAlerts.length == 0){
    $('#warningTotalCount').hide();
  }else{
    $('#warningTotalCount').show();
  }
  
  if(severAlerts.length == 0
      && errorAlerts.length == 0
      && warningAlerts.length == 0
      && infoAlerts.length == 0){
    $('#allAlertCount').hide();
  }else{
    $('#allAlertCount').show();
  }

}

// function used for generating alerts html div
function generateNotificationAlerts(alertsList, type) {
  var alertDiv = "";

  alertDiv = "<div id='tip_trigger_" + alertsList.id + "' class='tip_trigger";
  
  // if alert is selected, highlight entry 
  if(currentSelectedAlertId!= null && currentSelectedAlertId == alertsList.id){
    alertDiv = alertDiv + " active";
  }
  
  alertDiv = alertDiv + "' onclick='showNotificationPopup(event, this, \""
      + type + "\", " + alertsList.id + ")'>";

  alertDiv = alertDiv + "<div class='tabMessageMasterBlock'>";
  alertDiv = alertDiv + "<div class='";

  if (type == "severe") {
    alertDiv = alertDiv + " tabSevere ";
  } else if (type == "error") {
    alertDiv = alertDiv + " tabError ";
  } else if (type == "warning") {
    alertDiv = alertDiv + " tabWarning ";
  } else {
    alertDiv = alertDiv + " tabInfo ";
  }

  if (alertsList.isAcknowledged) {
    alertDiv = alertDiv + " tabMessageHeadingAckBlock ";
  } else {
    alertDiv = alertDiv + " tabMessageHeadingBlock ";
  }

  alertDiv = alertDiv + " defaultCursor' id='alertTitle_" + alertsList.id
      + "'>" + alertsList.memberName + "</div>" + "<p id='alertShortMsg_"
      + alertsList.id + "' class='tabMessageDetailsBlock ";

  if (alertsList.isAcknowledged) {
    alertDiv = alertDiv + " graytickmark ";
  } else {
    alertDiv = alertDiv + " notickmark ";
  }

  var alertDescription = alertsList.description;
  if(alertDescription.length <= 38){
    alertDiv = alertDiv + " '>" + alertDescription + "</p>";
  }else{
    alertDiv = alertDiv + " '>" + alertDescription.substring(0,36) + "..</p>";
  }

  alertDiv = alertDiv + "<div class='tabMessageDateBlock'>" 
      + jQuery.timeago(alertsList.iso8601Ts) + "</div>"
      + "</div>";

  alertDiv = alertDiv + "</div>";

  return alertDiv;
}

/*
 * Function to generate html for Notification panel 
 */
function generateNotificationsPanel(){
  var notificationPanelHtml = "";

  // Notification tabs top ribbon
  notificationPanelHtml += 
      "<div class=\"TabTopPanelInnerBlock\">" 
    + "<a id=\"btnTopTab_All\" class=\"position-relative TopTabLink "
    + "TopTabLinkActive\" onClick=\"tabAll();toggleTab('TopTab_All');\" >" 
    + "<span class=\"tabAll\"  >All</span>"
    + "<span class=\"textbold postical-absolute notification\" id=\"allAlertCount\">0</span>"
    + "</a>"
    + "<a id=\"btnTopTab_Severe\" class=\"position-relative TopTabLink\" "
    + "onClick=\"tabSevere();toggleTab('TopTab_Severe');\" >"
    + "<span class=\"tabSevere\">&nbsp;</span>"
    + "<span class=\"textbold postical-absolute notification\" id=\"severeTotalCount\">0</span>"
    + "</a>"
    + "<a id=\"btnTopTab_Error\" class=\"position-relative TopTabLink\" "
    + "onClick=\"tabError();toggleTab('TopTab_Error');\" >"
    + "<span class=\"tabError\"  >&nbsp;</span>"
    + "<span class=\"textbold postical-absolute notification\" id=\"errorTotalCount\">0</span>"
    + "</a>"
    + "<a id=\"btnTopTab_Warning\" class=\"position-relative TopTabLink\" "
    + "onClick=\"tabWarning();toggleTab('TopTab_Warning');\" >"
    + "<span class=\"tabWarning\"  >&nbsp;</span>"
    + "<span class=\"textbold postical-absolute notification\" id=\"warningTotalCount\">0</span>"
    + "</a>"
    + "</div>";

  // Tab 1 - All
  notificationPanelHtml += 
      "<div class=\"postical-absolute display-block topTabBlock\" id=\"TopTab_All\">"
    + "<!-- Search-->"
    + "<div class=\"left marginL10 marginTop6\">"
    + "<div class=\"searchBlockMaster\">"
    + "<input type=\"button\" class=\"searchButton\">"
    + "<input type=\"text\" placeholder=\"Search\" class=\"searchBox\" "
    + "onkeyup=\"applyFilterOnNotificationsList('all');\" id=\"filterAllNotificationsListBox\">"
    + "</div>"
    + "</div>"
    + "<!-- Clear Button-->"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('all', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('all', false)\">Clear</a>"
    + "</div>"
    + "<div class=\"scroll-pane\" id=\"allAlertScrollPane\" >"
    + "<div id=\"allAlertList\"></div>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('all', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('all', false)\">Clear</a>"
    + "</div>"
    + "<div class=\"loadMoreButton\" id=\"containerLoadMoreAlertsLink\">"
    + "<a href=\"#\" class=\"left linkButton\" onclick=\"loadMoreAlerts()\">Load More</a>"
    + "</div>"
    + "<input type=\"hidden\" name=\"pageNumber\" id=\"pageNumber\" value=\"1\" />"
    + "</div>";

  // Tab 2 - Severe
  notificationPanelHtml +=
      "<div class=\"postical-absolute display-none topTabBlock\" id=\"TopTab_Severe\">" 
    + "<!-- Search-->"
    + "<div class=\"left marginL10 marginTop6\">"
    + "<div class=\"searchBlockMaster\">"
    + "<input type=\"button\" class=\"searchButton\">"
    + "<input type=\"text\" placeholder=\"Search\" class=\"searchBox\" "
    + "onkeyup=\"applyFilterOnNotificationsList('severe');\" id=\"filterSevereNotificationsListBox\">"
    + "</div>"
    + "</div>"
    + "<!-- Clear Button-->"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('severe', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('severe', false)\">Clear</a>"
    + "</div>"
    + "<div class=\"scroll-pane\" id=\"severeAlertScrollPane\">"
    + "<div id=\"severeList\"></div>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('severe', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('severe', false)\">Clear</a>"
    + "</div>"
    + "</div>";

  // Tab 3 - Error
  notificationPanelHtml +=
      "<div class=\"postical-absolute display-none topTabBlock\" id=\"TopTab_Error\" >" 
    + "<!-- Search-->"
    + "<div class=\"left marginL10 marginTop6\">"
    + "<div class=\"searchBlockMaster\">"
    + "<input type=\"button\" class=\"searchButton\">"
    + "<input type=\"text\" placeholder=\"Search\" class=\"searchBox\" "
    + "onkeyup=\"applyFilterOnNotificationsList('error');\" id=\"filterErrorNotificationsListBox\">"
    + "</div>"
    + "</div>"
    + "<!-- Clear Button-->"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('error', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('error', false)\">Clear</a>"
    + "</div>"
    + "<div class=\"scroll-pane\" id=\"errorAlertScrollPane\">"
    + "<div id=\"errorList\"></div>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('error', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('error', false)\">Clear</a>"
    + "</div>"
    + "</div>";

  // Tab 4 - Warning
  notificationPanelHtml += 
      "<div class=\"postical-absolute display-none topTabBlock\" id=\"TopTab_Warning\">"
    + "<!-- Search-->"
    + "<div class=\"left marginL10 marginTop6\">"
    + "<div class=\"searchBlockMaster\">"
    + "<input type=\"button\" class=\"searchButton\">"
    + "<input type=\"text\" placeholder=\"Search\" class=\"searchBox\" "
    + "onkeyup=\"applyFilterOnNotificationsList('warning');\" id=\"filterWarningNotificationsListBox\">"
    + "</div>"
    + "</div>"
    + "<!-- Clear Button-->"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('warning', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">" 
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('warning', false)\">Clear</a>"
    + "</div>"
    + "<div class=\"scroll-pane\" id=\"warningAlertScrollPane\" >"
    + "<div id=\"warningList\"></div>" 
    + "</div>"
    + "<div class=\"clearButton\">"
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('warning', true)\">Clear All</a>"
    + "</div>"
    + "<div class=\"clearButton\">" 
    + "<a href=\"#\" class=\"right linkButton\" onclick=\"clearAlerts('warning', false)\">Clear</a>"
    + "</div>"
    + "</div>";

  // Set html into the notification panel
  $("#notificationsPanel").html(notificationPanelHtml);

}

// function used to give functionality of expanding and collapsing the left
// panel cluster name and member list
function expandCollapse(liDiv, clusterNameDiv) {
  if ($("#" + liDiv).hasClass("collapsable")) {
    $("#" + liDiv).removeClass("collapsable");
    $("#" + liDiv).addClass("expandable");
    $("#" + clusterNameDiv).removeClass("hitarea collapsable-hitarea");
    $("#" + clusterNameDiv).addClass("hitarea expandable-hitarea");
    document.getElementById("clusterA_1").style.display = "block";
  } else {
    $("#" + liDiv).removeClass("expandable");
    $("#" + liDiv).addClass("collapsable");
    $("#" + clusterNameDiv).removeClass("hitarea expandable-hitarea");
    $("#" + clusterNameDiv).addClass("hitarea collapsable-hitarea");
    document.getElementById("clusterA_1").style.display = "none";
  }
}

// function used to give functionality of expanding and collapsing all the three
// alerts
function expandCollapseAlerts(div1Id, div2Id, ulId) {
  if ($("#" + div1Id).hasClass("collapsable")) {
    $("#" + div1Id).removeClass("collapsable");
    $("#" + div1Id).addClass("expandable");

    $("#" + div2Id).removeClass("rightCollapseLabelDropdown");
    $("#" + div2Id).addClass("rightLabelDropdown");

    $("#" + div2Id).removeClass("hitarea collapsable-hitarea");
    $("#" + div2Id).addClass("hitarea expandable-hitarea");
    document.getElementById(ulId).style.display = "block";
  } else {
    $("#" + div1Id).removeClass("expandable");
    $("#" + div1Id).addClass("collapsable");

    $("#" + div2Id).removeClass("rightLabelDropdown");
    $("#" + div2Id).addClass("rightCollapseLabelDropdown");

    $("#" + div2Id).removeClass("hitarea expandable-hitarea");
    $("#" + div2Id).addClass("hitarea collapsable-hitarea");
    document.getElementById(ulId).style.display = "none";
  }
}

// function used for getting request parameters
function getRequestParams() {
  // host
  if (GetParam('host') != null || GetParam('host') != "") {
    host = GetParam('host');
  } else {
    host = '';
  }

  // port
  if (GetParam('port') != null || GetParam('port') != "") {
    port = GetParam('port');
  } else {
    port = '';
  }

  // memberId
  if (GetParam('member') != null || GetParam('member') != "") {
    memberId = GetParam('member');
  } else {
    memberId = '';
  }

  // memberName
  if (GetParam('memberName') != null || GetParam('memberName') != "") {
    memberName = GetParam('memberName');
  } else {
    memberName = '';
  }
  
  //regionFullPath
  if (GetParam('regionFullPath') != null || GetParam('regionFullPath') != "") {
    regionFullPath = GetParam('regionFullPath');
  } else {
    regionFullPath = '';
  }
}

// function used for getting system Alerts details

function getSystemAlerts() {

  var requestData = {
    "pageNumber" : $('#pageNumber').val()
  };

  $.getJSON(
          "GetSystemAlerts",
          requestData,
          function(data) {
            $('#pageNumber').val(data.pageNumber);
            var recordCount = parseInt(data.pageNumber) * 100;
            if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
              $("#allAlertScrollPane").removeClass("hide-scroll-pane");
            var errorAlertsList = data.systemAlerts.errors;
            var severeAlertsList = data.systemAlerts.severe;
            var warningAlertsList = data.systemAlerts.warnings;
            var infoAlertsList = data.systemAlerts.info;

            var allListHTML = "";
            var allCount = 1;

            if (severeAlertsList != null && severeAlertsList != undefined) {

              if ($("#severeAlertScrollPane").hasClass("hide-scroll-pane"))
                $("#severeAlertScrollPane").removeClass("hide-scroll-pane");
              document.getElementById("severeTotalCount").innerHTML = severeAlertsList.length
                  .toString();
              // update global count
              numTotalSeverAlerts = severeAlertsList.length;
              var severeListHTML = "";
              var severeCount = 1;

              for ( var i = numTotalSeverAlerts - 1; i >= 0; i--) {
                if (severeCount <= recordCount) {
                  severeListHTML = severeListHTML
                      + generateNotificationAlerts(severeAlertsList[i],
                          "severe");
                  severeCount++;
                  severAlerts[severAlerts.length] = severeAlertsList[i];
                }
                if (allCount <= recordCount) {
                  allCount++;
                  allListHTML = severeListHTML;
                }
              }
              document.getElementById("severeList").innerHTML = severeListHTML;

            }

            if (errorAlertsList != null && errorAlertsList != undefined) {

              if ($("#errorAlertScrollPane").hasClass("hide-scroll-pane"))
                $("#errorAlertScrollPane").removeClass("hide-scroll-pane");
              document.getElementById("errorTotalCount").innerHTML = errorAlertsList.length;
              // update global count
              numTotalErrorAlerts = errorAlertsList.length;
              var errorListHTML = "";
              var errorCount = 1;
              for ( var i = numTotalErrorAlerts - 1; i >= 0; i--) {
                if (errorCount <= recordCount) {
                  errorListHTML = errorListHTML
                      + generateNotificationAlerts(errorAlertsList[i], "error");
                  errorAlerts[errorAlerts.length] = errorAlertsList[i];
                  errorCount++;
                }
                if (allCount <= recordCount) {
                  allCount++;
                  allListHTML = allListHTML
                      + generateNotificationAlerts(errorAlertsList[i], "error");
                }

              }
              document.getElementById("errorList").innerHTML = errorListHTML;

            }

            if (warningAlertsList != null && warningAlertsList != undefined) {

              if ($("#warningAlertScrollPane").hasClass("hide-scroll-pane"))
                $("#warningAlertScrollPane").removeClass("hide-scroll-pane");
              document.getElementById("warningTotalCount").innerHTML = warningAlertsList.length;
              // update global count
              numTotalWarningAlerts = warningAlertsList.length;
              var warningListHTML = "";
              var warningCount = 1;
              for ( var i = numTotalWarningAlerts - 1; i >= 0; i--) {
                if (warningCount <= recordCount) {
                  warningListHTML = warningListHTML
                      + generateNotificationAlerts(warningAlertsList[i],
                          "warning");
                  warningAlerts[warningAlerts.length] = warningAlertsList[i];
                  warningCount++;
                }
                if (allCount <= recordCount) {
                  allCount++;
                  allListHTML = allListHTML
                      + generateNotificationAlerts(warningAlertsList[i],
                          "warning");
                }
              }
              document.getElementById("warningList").innerHTML = warningListHTML;

            }

            if (infoAlertsList != null && infoAlertsList != undefined) {
              
              // update global count
              numTotalInfoAlerts = infoAlertsList.length;
              var infoCount = 1;
              for ( var i = numTotalInfoAlerts - 1; i >= 0; i--) {
                if (infoCount <= recordCount) {
                  infoAlerts[infoAlerts.length] = infoAlertsList[i];
                  infoCount++;
                }
                if (allCount <= recordCount) {
                  allCount++;
                  allListHTML = allListHTML
                      + generateNotificationAlerts(infoAlertsList[i], "info");
                }
              }

            }

            // start : for all alerts tab
            var allAlertCount = numTotalSeverAlerts + numTotalErrorAlerts
                + numTotalWarningAlerts + numTotalInfoAlerts;
            document.getElementById("allAlertCount").innerHTML = allAlertCount
                .toString();
            // Display Load More only when number of alerts > 100
            if (allAlertCount > 100) {
              $('#containerLoadMoreAlertsLink').show();
            } else {
              $('#containerLoadMoreAlertsLink').hide();
            }

            /*var allAlertHTML = "";

            allAlertHTML = allAlertHTML
                + document.getElementById("severeList").innerHTML;

            allAlertHTML = allAlertHTML
                + document.getElementById("errorList").innerHTML;

            allAlertHTML = allAlertHTML
                + document.getElementById("warningList").innerHTML;*/

            document.getElementById("allAlertList").innerHTML = allListHTML;

            // end : for all alerts tab

            if (severAlerts.length > 0) { // Severe
              if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
                $("#allAlertScrollPane").removeClass("hide-scroll-pane");
              $('#clusterStatusText').html("Severe");
              $("#clusterStatusIcon").addClass("severeStatus");
            } else if (errorAlerts.length > 0) { // Error
              if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
                $("#allAlertScrollPane").removeClass("hide-scroll-pane");
              $('#clusterStatusText').html("Error");
              $("#clusterStatusIcon").addClass("errorStatus");
            } else if (warningAlerts.length > 0) { // Warning
              if ($("#allAlertScrollPane").hasClass("hide-scroll-pane"))
                $("#allAlertScrollPane").removeClass("hide-scroll-pane");
              $('#clusterStatusText').html("Warning");
              $("#clusterStatusIcon").addClass("warningStatus");
            } else { // Normal
              $('#clusterStatusText').html("Normal");
              $("#clusterStatusIcon").addClass("normalStatus");
            }
            $('.scroll-pane').jScrollPane();
          }).error(repsonseErrorHandler);

  if (!loadMore)
    setTimeout("getSystemAlerts()", 10000); // refreshing cluster disk storage
  else
    loadMore = false;
  // widget in every 5 sec
}

// function to copy alert message to clipboard
function copyAlertTextToClipboard(alertText) {

  console.log("copyAlertTextToClipboard called");

}

// function used for acknowleding a system alert
function acknowledgeAlert(divId) {
  requestData = {
    "alertId" : divId
  };
  $.getJSON(
      "pulse/acknowledgeAlert",
      requestData,
      function(data) {
        // Change color of alert title
        $("#alertTitle_" + divId).addClass(
            "tabMessageHeadingAckBlock defaultCursor");
        $("#alertTitle_" + divId).removeClass("tabMessageHeadingBlock");

        // Add Acknowledged Tick
        $("#alertShortMsg_" + divId).addClass("graytickmark");
        $("#alertShortMsg_" + divId).removeClass("notickmark");

      }).error(repsonseErrorHandler);
}

function loadMoreAlerts() {
  $('#pageNumber').val(parseInt($('#pageNumber').val()) + 1);
  loadMore = true;
  getSystemAlerts();
}

/*
 * Displays Notification Alert Popup
 */
function showNotificationPopup(e, element, alertType, alertId) {
  // set selected alert id
  currentSelectedAlertId = alertId;
  
  var alertSelected = null;
  
  // create pop up content
  if (alertType == "severe") {
    for ( var i = 0; i < severAlerts.length; i++) {
      if (alertId == severAlerts[i].id) {
        alertSelected = severAlerts[i];
      }
    }
  } else if (alertType == "error") {
    for ( var i = 0; i < errorAlerts.length; i++) {
      if (alertId == errorAlerts[i].id) {
        alertSelected = errorAlerts[i];
      }
    }
  } else if (alertType == "warning") {
    for ( var i = 0; i < warningAlerts.length; i++) {
      if (alertId == warningAlerts[i].id) {
        alertSelected = warningAlerts[i];
      }
    }
  } else {// info
    for ( var i = 0; i < infoAlerts.length; i++) {
      if (alertId == infoAlerts[i].id) {
        alertSelected = infoAlerts[i];
      }
    }
  }

  // content html
  var alertDiv = "<div class='content'>" + "<h3 class='";

  if (alertType == "severe") {
    alertDiv = alertDiv + " tabSevere ";
  } else if (alertType == "error") {
    alertDiv = alertDiv + " tabError ";
  } else if (alertType == "warning") {
    alertDiv = alertDiv + " tabWarning ";
  } else {
    alertDiv = alertDiv + " tabInfo ";
  }

  alertDiv = alertDiv + "'>" + alertSelected.memberName + "</h3>"
      + "<div class='tooltipcontent'><p>" + alertSelected.description
      + "</p></div>" + "<div class='notifications'>" + "<ul>"
      + "<li><div class='tabMessageDateBlock'>" + alertSelected.timestamp
      + "</div></li>" + "<li class='right'><a href='#.' ";
  
  // Add call to function handler only if alert is not acknowledged previously
  if (!alertSelected.isAcknowledged) {
    alertDiv = alertDiv + " onClick = 'javascript:acknowledgeAlert("
        + alertSelected.id + ")' ";
  }

  alertDiv = alertDiv
      + " >"
      + "<span class='acknowledgedTick'></span></a></li>"
      + "<!-- <li class='right'><a href='#.' onClick = 'javascript:copyAlertTextToClipboard(\""
      + alertSelected.id + "\")' >"
      + "<span class='copyNotification'></span></a></li> -->" 
      + "</ul>"
      + "</div>" + "</div>" 
      + "<span class='bubble-arrow'></span>"
      + "<a class='closePopup' href='javascript:hideNotificationPopup("
      + alertSelected.id + ");'></a>";

  $("#tooltip").html(alertDiv);

  // Logic to display pop up
  $(".tip_trigger").removeClass("active");
  var parentOffset = $(element).offset();
  $(element).addClass("active");

  $("#tooltip").find('.tooltipcontent').css("height", 100);
  $("#tooltip").find('.tooltipcontent').css("width", 277);
  $("#tooltip").find('.content').css("width", 277);
  $("#tooltip").css({
    width : '277px'
  });
  var relX = parentOffset.left - $("#tooltip").width();
  var relY = parentOffset.top - 160;

  $("#tooltip").css({
    top : relY,
    left : relX
  });
  $("#tooltip").show();
  $("#tooltip").find('.tooltipcontent').jScrollPane();

}

/*
 * Hides Notification Alert Popup
 */
function hideNotificationPopup(divId) {
  // Hide Pop up
  $("#tooltip").hide();

  // rmeove selected alert's highlighting
  $('#tip_trigger_' + divId).removeClass('active');
}

function ajaxPost(pulseUrl, pulseData, pulseCallBackName) {
  $.ajax({
    url : pulseUrl,
    type : "POST",
    dataType : "json",
    data : {
      "pulseData" : this.toJSONObj(pulseData)
    },
    // data : pulseData,
    // callback handler that will be called on success
    success : function(data) {
      pulseCallBackName(data);
    },
    // callback handler that will be called on error
    error : function(jqXHR, textStatus, errorThrown) {
      // log the error to the console
      console.log("The following error occured: " + textStatus, errorThrown);
      $('#connectionStatusDiv').show();
      $('#connectionErrorMsgDiv').html("Pulse server is not connected");
    },
    // callback handler that will be called on completion
    // which means, either on success or error
    complete : function() {
      // enable the inputs
      // $inputs.removeAttr("disabled");
    }
  });

}

function toJSONObj(object) {
  var type = typeof object;
  switch (type) {
  case 'undefined':
  case 'function':
  case 'unknown':
    return;
  case 'object':
    break;
  default:
    return '"' + object.toString() + '"';
  }
  if (object === null)
    return 'null';
  if (object.ownerDocument === document)
    return;

  var results = [];
  if (object.length) { // array
    for ( var i = 0; i < object.length; i++) {
      var value = this.toJSON(object[i]);
      if (value !== undefined)
        results.push(value);
    }
    return '[' + results.join(',') + ']';
  } else { // object
    for ( var property in object) {
      var value = this.toJSONObj(object[property]);
      if (value !== undefined) {
        property = (property.split('_')[0] == "pseudo") ? '@'
            + property.split('_')[1] : property;
        results.push('"' + property + '"' + ':' + value);
      }
    }
    return '{' + results.join(',') + '}';
  }
}

UUIDv4 = function b(a) {
  return a ? (a ^ Math.random() * 16 >> a / 4).toString(16) : ([ 1e7 ] + -1e3
      + -4e3 + -8e3 + -1e11).replace(/[018]/g, b);
};

function scanPageForWidgets() {
  var listOfActiveWidgets = $("[data-active ='yes']");
  var functionTimingList = new Array();
  for ( var i = 0; i < listOfActiveWidgets.length; i++) {
    // alert( listOfActiveWidgets[i].dataset.widgetid);
    widgetAlreadyPresent = false;
    for ( var j = 0; j < functionTimingList.length; j++) {
      if ((functionTimingList[j].indexOf(listOfActiveWidgets[i]
          .getAttribute("data-timeline"), 0)) == 0) {
        functionTimingList[j] = functionTimingList[j] + ","
            + listOfActiveWidgets[i].getAttribute("data-widgetid") + "Func";
        widgetAlreadyPresent = true;
      }
    }

    if (!widgetAlreadyPresent) {
      functionTimingList
          .push(listOfActiveWidgets[i].getAttribute("data-timeline") + ":"
              + listOfActiveWidgets[i].getAttribute("data-widgetid") + "Func");
    }
  }
  prepareDataAndPost(functionTimingList);
}

function prepareDataAndPost(functionTimingList) {
  var functionArray = [];
  var timing;
  for ( var i = 0; i < functionTimingList.length; i++) {
    var colon = functionTimingList[i].indexOf(":");
    timing = functionTimingList[i].substr(0, colon);
    var functionList = functionTimingList[i].substr(colon + 1,
        functionTimingList[i].length);
    var func = "setInterval(function() {\n"
        + "var postData = new Object();\n"
        + "var listOfPulseFunction= '"
        + functionList.toString()
        + "'.split(\",\");\n"
        + "for(var j=0; j< listOfPulseFunction.length; j++){\n"
        + "var functionName = listOfPulseFunction[j];\n"
        + "var funcCall =  'pf.'+functionName + \"('\" +functionName+\"',postData);\";\n"
        + "eval(funcCall);\n"
        + "}\n"
        + "ajaxPost(\"pulse/pulseUpdate\", postData, responseCallbackHandler);\n"
        + "postData = null;\n" + "postData = new Object();\n" + "}," + timing
        + ");";
    var loadOnStart = "setTimeout(function() {\n"
        + "var postData = new Object();\n"
        + "var listOfPulseFunction= '"
        + functionList.toString()
        + "'.split(\",\");\n"
        + "for(var j=0; j< listOfPulseFunction.length; j++){\n"
        + "var functionName = listOfPulseFunction[j];\n"
        + "var funcCall =  'pf.'+functionName + \"('\" +functionName+\"',postData);\";\n"
        + "eval(funcCall);\n"
        + "}\n"
        + "ajaxPost(\"pulse/pulseUpdate\", postData, responseCallbackHandler);\n"
        + "postData = null;\n" + "postData = new Object();\n" + "},0);";
    functionArray[i] = func;
    functionStartArray[i] = loadOnStart;
  }
  for ( var k = 0; k < functionStartArray.length; k++) {
    eval(functionStartArray[k]);
  }
  for ( var k = 0; k < functionArray.length; k++) {
    eval(functionArray[k]);
  }

}
/*
 * function prepareDataAndPost(functionTimingList) { for ( var i = 0; i <
 * functionTimingList.length; i++) { var postData = new Object(); var funcObj =
 * new Array(); var colon = functionTimingList[i].indexOf(":"); var timing =
 * functionTimingList[i].substr(0, colon); var functionList =
 * functionTimingList[i].substr(colon + 1, functionTimingList[i].length);
 * funcObj[i] = setInterval(function() { var listOfPulseFunction =
 * functionList.split(","); for ( var j = 0; j < listOfPulseFunction.length;
 * j++) { var functionName = listOfPulseFunction[j]; //Create the function call
 * from function name and parameter. var funcCall = 'pf.'+functionName + "('" +
 * functionName + "',postData);"; //Call the function eval(funcCall); }
 * ajaxPost("pulse/pulseUpdate", postData, responseCallbackHandler); postData =
 * null; postData = new Object(); }, timing); } }
 */

var responseCallbackHandler = function(data) {
  for ( var widgetCallbackHandlerName in data) {
    try {
      eval('get' + widgetCallbackHandlerName
          + 'Back(data[widgetCallbackHandlerName])');
    } catch (err) {
      console.log('Error in get' + widgetCallbackHandlerName + 'Back : ' + err);
    }
  }

};
/*
* This function sends ajax request with passed post data.
* Use this function to force periodic update of pulse data.
*/
function forcePulseDataUpdate(postData){
  ajaxPost("pulse/pulseUpdate", postData, responseCallbackHandler);
}

// function used for getting pulse version
function getPulseVersion() {

  $.getJSON("pulse/pulseVersion", function(data) {

    var pulseVersion = data.pulseVersion;
    $('#pulseVersion').html(pulseVersion);
    $('#pulseVer').html(data.pulseVersion);
    $('#buildId').html(data.buildId);
    $('#buildDate').html(data.buildDate);
    $('#sourceDate').html(data.sourceDate);
    $('#sourceRevision').html(data.sourceRevision);
    $('#sourceRepository').html(data.sourceRepository);

    if (data.pulseVersion != undefined && data.pulseVersion != "") {
      // Display version details link
      $('#pulseVersionDetailsLink').toggle();
    }
  });
}

/*
 * String utility function to check whether string is empty or whitespace only
 * or null or undefined
 * 
 */
function isEmpty(str) {

  // Remove extra spaces
  str = str.replace(/\s+/g, ' ');

  switch (str) {
  case "":
  case " ":
  case null:
  case false:
  case typeof this == "undefined":
  case (/^\s*$/).test(str):
    return true;
  default:
    return false;
  }
}

/*
 * Utility function to check whether value is -1, 
 * return true if -1 else false
 * 
 */
function isNotApplicable(value) {
  
  if(!isNaN(value)){
    // if number, convert to string
    value = value.toString();
  }else{
    // Remove extra spaces
    value = value.replace(/\s+/g, ' ');
  }

  

  switch (value) {
  case "-1":
  case "-1.0":
  case "-1.00":
    return true;
  default:
    return false;
  }
}

/*
 * Utility function to apply Not Applicable constraint on value, 
 * returns "NA" if isNotApplicable(value) returns true 
 * else value itself
 * 
 */
function applyNotApplicableCheck(value){
  if(isNotApplicable(value)){
    return "NA";
  }else{
    return value;
  }
}

/*
 * Utility function to convert given value in Bytes to KB or MB or GB
 * 
 */
function convertBytesToMBorGB(value){
  // UNITS VALUES IN BYTES
  var ONE_KB = 1024;
  var ONE_MB = 1024 * 1024;
  var ONE_GB = 1024 * 1024 * 1024;
  
  var convertedValue = new Array();
  var valueInMBorGB = value;
  var isBorKBorMBorGB = "B";
  
  if (valueInMBorGB > ONE_KB && valueInMBorGB < ONE_MB) {
    // Convert to KBs
    valueInMBorGB = (valueInMBorGB / ONE_KB);
    isBorKBorMBorGB = "KB";
  }else if(valueInMBorGB > ONE_MB && valueInMBorGB < ONE_GB){
    // Convert to MBs
    valueInMBorGB = (valueInMBorGB / ONE_MB);
    isBorKBorMBorGB = "MB";
  }else if(valueInMBorGB > ONE_GB ){
    // Convert to GBs
    valueInMBorGB = (valueInMBorGB / ONE_GB);
    isBorKBorMBorGB = "GB";
  }

  // converted value
  convertedValue.push(valueInMBorGB.toFixed(2));
  // B or KB or MB or GB
  convertedValue.push(isBorKBorMBorGB);
  
  return convertedValue;
}

// Function to escape html entities
function escapeHTML(htmlContent) {
  return escapedHTML = htmlContent
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#x27;')
      .replace(/`/g, '&#x60;');
}

//Function to unescape html entities
function unescapeHTML(htmlContent) {
  return unescapedHTML = htmlContent
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#x27;/g, "'")
      .replace(/&#x60;/g, '`');
}
