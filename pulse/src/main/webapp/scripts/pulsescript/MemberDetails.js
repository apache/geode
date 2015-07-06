/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// MemberDetails.js
var memberRegions;
var membersList = null;
var isMemberListFilterHandlerBound = false;
var gatewaysenderEndpoints = null;
var asyncEventQueueData = null;

// initializeMemberDetailsPage: function that initializes all widgets present on
// Member Details Page
$(document).ready(function() {

  // Load Notification HTML
  generateNotificationsPanel();

  // loadResourceBundles();
  // modify UI text as per requirement
  customizeUI();

  if (CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()) {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_SQLFIRE);
  } else {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_GEMFIRE);
  }

  scanPageForWidgets();
  createMemberRegionsTreeMap();
  createMemberRegionsGrid();
  createMemberClientsGrid(); // creating empty member client grid
  $.ajaxSetup({
    cache : false
  });
  
  $('#MemberGatewayHubWidget').show();
});

/*
 * Displays Gateway sender Popup
 */
function showGatewaySenderPopup(element) {
  
  hideAsyncEventQueuePopup();
  
  // content html
  var alertDiv = "<div class='content'><span class='left marginL10 canvasHeading'>Gateway Sender Details</span>"
    + "<div class='canvasInnerBlock tooltipcontent'><div class='gridBlocksPanel left' id='gateway_gridBlocks_Panel'>"
    + "<div class='left widthfull-100per'><table id='idGatewaySenderGrid'></table></div></div></div>" 
    + "<div class='marginB10'>&nbsp;&nbsp;</div></div><a class='closePopup' href='javascript:hideGatewaySenderPopup();'></a>";

  $("#tooltip").html(alertDiv);

  // Logic to display pop up
  $(".tip_trigger").removeClass("active");
  var parentOffset = $(element).offset();
  $(element).addClass("active");

  $("#tooltip").find('.tooltipcontent').css("height", 200);
  $("#tooltip").find('.tooltipcontent').css("width", 500);
  $("#tooltip").find('.content').css("width", 500);
  $("#tooltip").css({
    width : '500px'
  });
  var relX = parentOffset.left - ($("#tooltip").width() / 2);
  var relY = parentOffset.top - 255;

  $("#tooltip").css({
    top : relY,
    left : relX
  });
  
  // put grid into pop-up
  if (!($("#idGatewaySenderGrid")[0].grid)) {
    // if grid is not initialized then create
    createGatewaySenderGrid();
  } else{
    destroyScrollPane('gview_idGatewaySenderGrid') ;
  }
  
  //clear grid and load data from global variable updated periodically
  $('#idGatewaySenderGrid').jqGrid('clearGridData');
  $('#idGatewaySenderGrid').jqGrid("getGridParam").data = gatewaysenderEndpoints;
  var gridUserData = jQuery("#idGatewaySenderGrid").getGridParam('userData');
  // Apply sort order ans sort columns on updated jqgrid data
  jQuery("#idGatewaySenderGrid").jqGrid('setGridParam', {
    sortname : gridUserData.sortColName,
    sortorder : gridUserData.sortOrder
  });
  // Reload jqgrid
  jQuery("#idGatewaySenderGrid").trigger("reloadGrid");
  
  $("#tooltip").show();
  $("#tooltip").find('.ui-jqgrid-bdiv').jScrollPane({maintainPosition : true, stickToRight : true});
}

/*
 * Hides Notification Alert Popup
 */
function hideGatewaySenderPopup() {
  // Hide Pop up
  $("#tooltip").hide();

  destroyScrollPane('gview_idGatewaySenderGrid') ;
}


function showAsyncEventQueuePopup(element) {
  
  hideGatewaySenderPopup();
  
  // content html
  var alertDiv = "<div class='content'><span class='left marginL10 canvasHeading'>Asynchronous Event Queue Details</span>"
    + "<div class='canvasInnerBlock tooltipcontent'><div class='gridBlocksPanel left' id='asynch_event_queue_gridBlocks_Panel'>"
    + "<div class='left widthfull-100per'><table id='idAsynchEventQueueGrid'></table></div></div></div>" 
    + "<div class='marginB10'>&nbsp;&nbsp;</div></div><a class='closePopup' href='javascript:hideAsyncEventQueuePopup();'></a>";

  $("#tooltip").html(alertDiv);

  // Logic to display pop up
  $(".tip_trigger").removeClass("active");
  var parentOffset = $(element).offset();
  $(element).addClass("active");

  $("#tooltip").find('.tooltipcontent').css("height", 200);
  $("#tooltip").find('.tooltipcontent').css("width", 500);
  $("#tooltip").find('.content').css("width", 500);
  $("#tooltip").css({
    width : '500px'
  });
  var relX = parentOffset.left - ($("#tooltip").width() / 2);
  var relY = parentOffset.top - 255;

  $("#tooltip").css({
    top : relY,
    left : relX
  });
  
  // put grid into pop-up
  if (!($("#idAsynchEventQueueGrid")[0].grid)) {
    // if grid is not initialized then create
    createAsynchEventQueueGrid();
  } else{
    destroyScrollPane('gview_idAsynchEventQueueGrid') ;
  }

  //clear grid and load data from global variable updated periodically
  $('#idAsynchEventQueueGrid').jqGrid('clearGridData');
  $('#idAsynchEventQueueGrid').jqGrid("getGridParam").data = asyncEventQueueData;
  var gridUserData = jQuery("#idAsynchEventQueueGrid").getGridParam('userData');
  
  // Apply sort order ans sort columns on updated jqgrid data
  jQuery("#idAsynchEventQueueGrid").jqGrid('setGridParam', {
    sortname : gridUserData.sortColName,
    sortorder : gridUserData.sortOrder
  });
  
  // Reload jqgrid
  jQuery("#idAsynchEventQueueGrid").trigger("reloadGrid");
  $("#tooltip").show();
  $("#tooltip").find('.ui-jqgrid-bdiv').jScrollPane({maintainPosition : true, stickToRight : true});
}

/*
 * Hides Notification Alert Popup
 */
function hideAsyncEventQueuePopup() {

  // Hide Pop up
  $("#tooltip").hide();

  destroyScrollPane('gview_idAsynchEventQueueGrid') ;
}

/*
 * Function to show and hide html elements/components based upon whether product
 * is sqlfire or gemfire
 */
function alterHtmlContainer(prodname) {
  if (CONST_BACKEND_PRODUCT_SQLFIRE == prodname.toLowerCase()) {
    // Hide HTML for following

    // Show HTML for following
    $('#subTabQueryStatistics').show();
  } else {
    // Hide HTML for following
    $('#subTabQueryStatistics').hide();

    // Show HTML for following

  }

}

// Function to generate HTML for Members list drop down
function generateMemberListHTML(membersList) {
  var htmlMemberList = '';
  for ( var i = 0; i < membersList.length; i++) {
    htmlMemberList += '<div class="resultItemFilter">'
        + '<a href="MemberDetails.html?member=' + membersList[i].memberId
        + '&memberName=' + membersList[i].name + '">' + membersList[i].name
        + '</a></div>';
  }
  return htmlMemberList;
}

// Handler to filter members list drop down based on user's criteria
var applyFilterOnMembersListDropDown = function(e) {
  var searchKeyword = extractFilterTextFrom('filterMembersBox');
  var filteredMembersList = new Array();
  if (searchKeyword != "") {
    // generate filtered members list
    for ( var i = 0; i < membersList.length; i++) {
      if (membersList[i].name.toLowerCase().indexOf(searchKeyword) !== -1) {
        filteredMembersList.push(membersList[i]);
      }
    }

    // Set list height
    if(filteredMembersList.length <= 5){
      $("#clusterMembersList").height(filteredMembersList.length * 26);
    }else{
      $("#clusterMembersList").height(5 * 26);
    }

    var htmlMemberListWithFilter = generateMemberListHTML(filteredMembersList);
    e.preventDefault();
    // $("div#setting").toggle();
    $('#clusterMembersContainer').html(htmlMemberListWithFilter);
    // $("div#setting").toggle();
    $('.jsonSuggestScrollFilter').jScrollPane();
  } else {

    // Set list height
    if(membersList.length <= 5){
      $("#clusterMembersList").height(membersList.length * 26);
    }else{
      $("#clusterMembersList").height(5 * 26);
    }

    var htmlMemberList = generateMemberListHTML(membersList);
    e.preventDefault();
    // $("div#setting").toggle();
    $('#clusterMembersContainer').html(htmlMemberList);
    // $("div#setting").toggle();
    $('.jsonSuggestScrollFilter').jScrollPane();
  }

};


function createAsynchEventQueueGrid() {
  jQuery("#idAsynchEventQueueGrid").jqGrid(
      {
        datatype : "local",
        height : 150,
        width : 500,
        shrinkToFit : false,
        rowNum : 20,
        colNames : [ 'Id', 'Primary', 'Parallel', 'Batch Size', 'Batch Time Interval', 'Batch Conflation Enabled', 'Async Event Listener', 'Event Queue Size' ],
        colModel : [
            {
              name : 'id',
              index : 'id',
              width : 80,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'primary',
              index : 'primary',
              width : 60,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'senderType',
              index : 'senderType',
              width : 60,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'batchSize',
              index : 'batchSize',
              width : 80,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "long"
            },
            {
              name : 'batchTimeInterval',
              index : 'batchTimeInterval',
              width : 95,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'batchConflationEnabled',
              index : 'batchConflationEnabled',
              width : 125,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'asyncEventListener',
              index : 'asyncEventListener',
              align : 'right',
              width : 145,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "long"
            },
            {
              name : 'queueSize',
              index : 'queueSize',
              width : 125,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formAsynchEventQueueGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "id"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#idAsynchEventQueueGrid").getGridParam(
              'userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        resizeStop : function(width, index) {
          
          var memberRegionsList = $('#gview_idAsynchEventQueueGrid');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          var api = memberRegionsListChild.data('jsp');
          api.reinitialise();
          
          memberRegionsList = $('#gview_idAsynchEventQueueGrid');
          memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_idAsynchEventQueueGrid');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
          
          this.gridComplete();
          $('#asynch_event_queue_gridBlocks_Panel').toggle();
          refreshTheGridByToggle('#asynch_event_queue_gridBlocks_Panel');
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_idAsynchEventQueueGrid');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_idAsynchEventQueueGrid');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
        }
      }
  );
}

// Tool tip for Asynch Event Queue in grid
function formAsynchEventQueueGridToolTip(rawObject) {
  return 'title="Queue ' + rawObject.id + ' , Primary ' + rawObject.primary
      + ' , Parallel ' + rawObject.senderType + ' , Batch size '
      + rawObject.batchSize + ' , Batch Time Interval '
      + rawObject.batchTimeInterval + ' , Batch Conflation Enabled '
      + rawObject.batchConflationEnabled + ' , Async Event Listener '
      + rawObject.asyncEventListener + ' , Queue size ' + rawObject.queueSize
      + '"';
}

function createGatewaySenderGrid() {
  jQuery("#idGatewaySenderGrid").jqGrid(
      {
        datatype : "local",
        height : 150,
        width : 500,
        shrinkToFit : false,
        rowNum : 20,
        colNames : [ 'Sender Id', 'Primary', 'Parallel', 'Remote DS Id', 'Connected Status', 'Batch Size', 'Event Queue Size',
            'Threshold Alert Count' ],
        colModel : [
            {
              name : 'id',
              index : 'id',
              width : 80,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'primary',
              index : 'primary',
              width : 60,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'senderType',
              index : 'senderType',
              width : 60,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'remoteDSId',
              index : 'remoteDSId',
              width : 95,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'status',
              index : 'status',
              width : 125,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'batchSize',
              index : 'batchSize',
              width : 80,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "long"
            },
            {
              name : 'queueSize',
              index : 'queueSize',
              width : 125,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'eventsExceedingAlertThreshold',
              index : 'eventsExceedingAlertThreshold',
              align : 'right',
              width : 145,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formGatewaySendersGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "long"
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "id"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#idGatewaySenderGrid").getGridParam(
              'userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        resizeStop : function(width, index) {
          
          var memberRegionsList = $('#gview_idGatewaySenderGrid');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          var api = memberRegionsListChild.data('jsp');
          api.reinitialise();
          
          memberRegionsList = $('#gview_idGatewaySenderGrid');
          memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_idGatewaySenderGrid');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
          
          this.gridComplete();
          $('#gateway_gridBlocks_Panel').toggle();
          refreshTheGridByToggle('#gateway_gridBlocks_Panel');
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_idGatewaySenderGrid');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_idGatewaySenderGrid');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
        }
      }
  );
}

// Tool tip for Gateway Senders in grid
function formGatewaySendersGridToolTip(rawObject) {
  return 'title="Sender ' + rawObject.id + ' , Primary ' + rawObject.primary
      + ' , Parallel ' + rawObject.senderType + '"';
}

function createMemberClientsGrid() {
  jQuery("#memberClientsList").jqGrid(
      {
        datatype : "local",
        height : 190,
        width : 745,
        rowNum : 100,
        shrinkToFit : false,
        colNames : [ 'Id', 'Name', 'Host', 'Connected','Subscription Enabled', 'Queue Size','Client CQ Count', 'CPU Usage', 'Uptime',
            'Threads', 'Gets', 'Puts' ],
        colModel : [
            {
              name : 'clientId',
              index : 'clientId',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'name',
              index : 'name',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'host',
              index : 'host',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'isConnected',
              index : 'isConnected',
              width : 75,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'isSubscriptionEnabled',
              index : 'isSubscriptionEnabled',
              width : 140,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                  return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'queueSize',
              index : 'queueSize',
              width : 80,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'clientCQCount',
              index : 'clientCQCount',
              width : 110,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                  return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'cpuUsage',
              index : 'cpuUsage',
              width : 80,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'uptime',
              index : 'uptime',
              width : 75,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'threads',
              index : 'threads',
              align : 'right',
              width : 70,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'gets',
              index : 'gets',
              width : 50,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'puts',
              index : 'puts',
              width : 50,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formClientsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "name"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#memberClientsList").getGridParam(
              'userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        resizeStop : function(width, index) {
          $('#LargeBlock_2').hide();
          destroyScrollPane('gview_memberClientsList');
          $('#LargeBlock_2').show();
          $('.ui-jqgrid-bdiv').each(function(index) {
            var tempName = $(this).parent().attr('id');
            if (tempName == 'gview_memberClientsList') {
              $(this).jScrollPane({
                maintainPosition : true,
                stickToRight : true
              });
            }
          });
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_memberClientsList');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberClientsList');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
        }
      });
}

// Tool tip for clients in grid
function formClientsGridToolTip(rawObject) {
  return 'title="Name ' + rawObject.name + ' , Host ' + rawObject.host
      + ' , Queue Size ' + rawObject.queueSize + ' , CPU Usage '
      + rawObject.cpuUsage + ' , Threads ' + rawObject.threads + '"';
}

// function used for creating empty member region tree map
function createMemberRegionsTreeMap() {

  var dataVal = {
    "$area" : 1
  };
  var json = {
    "children" : {},
    "data" : dataVal,
    "id" : "root",
    "name" : "Regions"
  };

  memberRegionsTreeMap = new $jit.TM.Squarified(
      {

        injectInto : 'memberRegionSummary',
        levelsToShow : 1,
        titleHeight : 0,
        background : '#8c9aab',
        offset : 2,
        Label : {
          type : 'HTML',
          size : 1
        },
        Node : {
          CanvasStyles : {
            shadowBlur : 0
          }
        },
        Events : {
          enable : true,
          onMouseEnter : function(node, eventInfo) {
            if (node) {
              node.setCanvasStyle('shadowBlur', 7);
              node.setData('border', '#ffffff');
              selecetdClusterTMNodeId = "";
              memberRegionsTreeMap.fx.plotNode(node,
                  memberRegionsTreeMap.canvas);
              memberRegionsTreeMap.labels.plotLabel(
                  memberRegionsTreeMap.canvas, node);
            }
          },
          onMouseLeave : function(node) {
            if (node) {
              node.removeData('border', '#ffffff');
              node.removeCanvasStyle('shadowBlur');
              memberRegionsTreeMap.plot();
            }
          }
        },

        Tips : {
          enable : true,
          offsetX : 20,
          offsetY : 20,
          onShow : function(tip, node, isLeaf, domElement) {

            var data = node.data;
            var html = "";
            if (data.regionType) {
              html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
                  + node.id
                  + "</div>"
                  + "<div class='popupFirstRow'><div class='popupRowBorder borderBottomZero'>"
                  + "<div class='labeltext left display-block width-45'><span class='left'>"
                  + "Type</span></div><div class='right width-55'>"
                  + "<div class='color-d2d5d7 font-size14 popInnerBlockEllipsis'>"
                  + data.regionType
                  + "</div>"
                  + "</div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-45'>"
                  + "<span class='left'>"
                  + jQuery.i18n.prop('pulse-entrycount-custom')
                  + "</span></div><div class='right width-55'>"
                  + "<div class='color-d2d5d7 font-size14'>"
                  + data.entryCount
                  + "</div>"
                  + "</div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-45'>"
                  + "<span class='left'>"
                  + jQuery.i18n.prop('pulse-entrysize-custom')
                  + "</span></div><div class='right width-55'>"
                  + "<div class='color-d2d5d7 font-size14'>" + data.entrySize
                  + "</div></div></div></div></div>" + "</div>";
            } else {
              html = "<div class=\"tip-title\"><div><div class='popupHeading'>No "
                  + jQuery.i18n.prop('pulse-regiontabletooltip-custom')
                  + " Found</div>";
            }
            tip.innerHTML = html;
          }
        },
        onCreateLabel : function(domElement, node) {
          domElement.style.opacity = 0.01;
        }
      });
  memberRegionsTreeMap.loadJSON(json);
  memberRegionsTreeMap.refresh();
}

// function used for creating blank grids for member's region list
// for member details screen
function createMemberRegionsGrid() {
  jQuery("#memberRegionsList").jqGrid(
      {
        datatype : "local",
        height : 250,
        width : 745,
        rowNum : 1000,
        shrinkToFit : false,
        colNames : [ 'Name', 'Type', 'Entry Count', 'Entry Size', 'Scope',
            'Disk Store Name', 'Disk Synchronous', 'Gateway Enabled' ],
        colModel : [
            {
              name : 'name',
              index : 'name',
              width : 150,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'type',
              index : 'type',
              width : 150,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'entryCount',
              index : 'entryCount',
              width : 100,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'entrySize',
              index : 'entrySize',
              width : 100,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'scope',
              index : 'scope',
              width : 150,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'diskStoreName',
              index : 'diskStoreName',
              width : 120,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'diskSynchronous',
              index : 'diskSynchronous',
              width : 120,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'gatewayEnabled',
              index : 'gatewayEnabled',
              width : 120,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formRegionsGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "name"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#memberRegionsList").getGridParam(
              'userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        resizeStop : function(width, index) {
          $('#btngridIcon').click();
          refreshTheGrid($('#btngridIcon'));
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_memberRegionsList');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberRegionsList');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });

          // change col names depend on product
          if (CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()) {
            jQuery("#memberRegionsList").jqGrid('setLabel', 'entryCount',
                jQuery.i18n.prop('pulse-entrycount-custom'));
            jQuery("#memberRegionsList").jqGrid('setLabel', 'entrySize',
                jQuery.i18n.prop('pulse-entrysize-custom'));
          }
        }
      });
}

// Tool tip for regions in grid
function formRegionsGridToolTip(rawObject) {
  return 'title="Name ' + rawObject.name + ' , Type ' + rawObject.type + ' , '
      + jQuery.i18n.prop('pulse-entrycount-custom') + ' '
      + rawObject.entryCount + ' , Scope ' + rawObject.scope
      + ' , Disk Store Name ' + rawObject.diskStoreName
      + ' , Disk Synchronous ' + rawObject.diskSynchronous + ' , '
      + jQuery.i18n.prop('pulse-entrysize-custom') + ' ' + rawObject.entrySize
      + '"';
}

function refreshTheGrid(gridDiv) {
  setTimeout(function() {
    gridDiv.click();
  }, 300);
}

function refreshTheGridByToggle(gridDiv) {
  setTimeout(function() {
    gridDiv.toggle();
  }, 300);
}
