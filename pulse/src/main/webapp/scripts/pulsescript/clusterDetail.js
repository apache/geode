/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

var selectedPerspectiveView = "member";
var selectedMemberViewOption = "default";
var selectedViewTypeDefault = ""; //"rgraph";
var selectedViewTypeSG = "treemap";
var selectedViewTypeRZ = "treemap";
var selectedViewTypeData = "treemap";

var gblClusterMembers = null;

/**
 * This JS File is used for Cluster Details screen
 * 
 */

// This function is the initialization function for Cluster Details screen. It
// is
// making call to functions defined for different widgets used on this screen
$(document).ready(function() {

  // Load Notification HTML  
  generateNotificationsPanel();

  // modify UI text as per requirement
  customizeUI();

  if (CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()) {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_SQLFIRE);

    // "ClusterDetails" service callback handler
    getClusterDetailsBack = getClusterDetailsSQLfireBack;

    // "ClusterKeyStatistics" service callback handler
    getClusterKeyStatisticsBack = getClusterKeyStatisticsSQLfireBack;

  } else {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_GEMFIRE);
    
    // "ClusterDetails" service callback handler
    getClusterDetailsBack = getClusterDetailsGemfireBack;

    // "ClusterKeyStatistics" service callback handler
    getClusterKeyStatisticsBack = getClusterKeyStatisticsGemfireBack;
  }

  // Add hostspot attributes
  hotspotAttributes = new Array();
  hotspotAttributes.push({"id":"currentHeapUsage", "name":"Heap Usage"});
  hotspotAttributes.push({"id":"cpuUsage", "name":"CPU Usage"});

  // Initialize set up for hotspot
  initHotspotDropDown();

  scanPageForWidgets();
  // Default view - creating blank cluster members tree map 
  createMembersTreeMapDefault();
  $('#default_treemap_block').hide();
  // Default view - creating blank cluster members grid
  createMemberGridDefault();
  // Default view - creating blank R Graph for all members associated with defined cluster
  createClusteRGraph();

  // Server Groups view - creating blank cluster members tree map 
  createMembersTreeMapSG();
  $('#servergroups_treemap_block').hide();
  // Server Groups view - creating blank cluster members grid
  createMemberGridSG();

  // Redundancy Zones view - creating blank cluster members tree map  
  createMembersTreeMapRZ();
  $('#redundancyzones_treemap_block').hide();
  // Redundancy Zones view - creating blank cluster members grid
  createMemberGridRZ();

  // Data view - creating blank cluster data/regions tree map 
  createRegionsTreeMapDefault();
  $('#data_treemap_block').hide();
  // Data view - creating blank cluster data/regions grid
  createRegionsGridDefault();

  $.ajaxSetup({
    cache : false
  });
});

/*
 * Function to show and hide html elements/components based upon whether product
 * is sqlfire or gemfire 
 */
function alterHtmlContainer(prodname){
  if(CONST_BACKEND_PRODUCT_SQLFIRE == prodname.toLowerCase()){
    // Hide HTML for following
    $('#clusterUniqueCQsContainer').hide();
    $('#SubscriptionsContainer').hide();
    $('#queriesPerSecContainer').hide();
    
    // Show HTML for following
    $('#subTabQueryStatistics').show();
    $('#TxnCommittedContainer').show();
    $('#TxnRollbackContainer').show(); 
  }else{
    // Hide HTML for following
    $('#subTabQueryStatistics').hide();
    $('#TxnCommittedContainer').hide();
    $('#TxnRollbackContainer').hide();

    // Show HTML for following
    $('#clusterUniqueCQsContainer').show();
    $('#SubscriptionsContainer').show();
    $('#queriesPerSecContainer').show();
  }
  
}

// Function called when Hotspot is changed 
function applyHotspot(){
  var data = new Object();
  data.members = gblClusterMembers;
  if (flagActiveTab == "MEM_TREE_MAP_DEF") {
    updateClusterMembersTreeMapDefault(data);
  } else if (flagActiveTab == "MEM_TREE_MAP_SG") {
    updateClusterMembersTreeMapSG(data);
  } else if (flagActiveTab == "MEM_TREE_MAP_RZ") {
    updateClusterMembersTreeMapRZ(data);
  }
}

function translateGetClusterMemberBack(data) {
  getClusterMembersBack(data.ClusterMembers);
}

function translateGetClusterMemberRGraphBack(data) {
  getClusterMembersRGraphBack(data.ClusterMembersRGraph);
}

function translateGetClusterRegionsBack(data) {
  getClusterRegionsBack(data.ClusterRegions);
}

// function used for creating blank TreeMap for member's on Cluster Details
// Screen
function createMembersTreeMapDefault() {
  var dataVal = {
    "$area" : 1,
    "initial" : true
  };
  var json = {
    "children" : {},
    "data" : dataVal,
    "id" : "root",
    "name" : "Members"
  };

  clusterMemberTreeMap = new $jit.TM.Squarified(
      {

        injectInto : 'GraphTreeMap',
        levelsToShow : 1,
        titleHeight : 0,
        background : '#a0c44a',
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

              clusterMemberTreeMap.fx.plotNode(node,
                  clusterMemberTreeMap.canvas);
              clusterMemberTreeMap.labels.plotLabel(
                  clusterMemberTreeMap.canvas, node);
            }
          },
          onMouseLeave : function(node) {
            if (node) {
              node.removeData('border', '#ffffff');
              node.removeCanvasStyle('shadowBlur');
              clusterMemberTreeMap.plot();
            }
          },
          onClick : function(node) {
            if (!node.data.initial) {
              location.href = 'MemberDetails.html?member=' + node.id
                  + '&memberName=' + node.name;
            }
          }
        },

        Tips : {
          enable : true,
          offsetX : 5,
          offsetY : 5,
          onShow : function(tip, node, isLeaf, domElement) {
            var html = "";
            var data = node.data;
            if (!data.initial) {
              html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
                  + node.name
                  + "</div>"
                  + "<div class='popupFirstRow'><div class='popupRowBorder borderBottomZero'>"
                  + "<div class='labeltext left display-block width-70'><span class='left'>CPU Usage</span></div><div class='right width-30'>"
                  + "<div class='color-d2d5d7 font-size14'>"
                  + data.cpuUsage
                  + "<span class='fontSize15'>%</span></span></div>"
                  + "</div></div>"
                  + "<div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                  + "<span class='left'>Memory Usage</span></div><div class='right width-30'>"
                  + "<div class='color-d2d5d7 font-size14'>"
                  + data.heapUsage
                  + "<span class='font-size15 paddingL5'>MB</span>"
                  + "</div></div></div>"
                  + "<div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                  + "<span class='left'>Load Avg.</span></div><div class='right width-30'>"
                  + "<div class='color-d2d5d7 font-size14'>"
                  + applyNotApplicableCheck(data.loadAvg)
                  + "</div></div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                  + "<span class='left'>Threads</span></div><div class='right width-30'>"
                  + "<div class='color-d2d5d7 font-size14'>"
                  + data.threads
                  + "</div></div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                  + "<span class='left'>Sockets</span></div><div class='right width-30'>"
                  + "<div class='color-d2d5d7 font-size14'>"
                  + applyNotApplicableCheck(data.sockets)
                  + "</div></div></div><div class='popupRowBorder borderBottomZero'>"
                  + "<div class='labeltext left display-block width-70'><span class='left'>GemFire Version</span>"
                  + "</div><div class='right width-30'><div class='color-d2d5d7 font-size14'>"
                  + data.gemfireVersion + "</div></div>" + "</div>" + ""
                  + "</div></div>" + "</div>";
            } else {
              html = "<div class=\"tip-title\"><div><div class='popupHeading'>Loading</div>";
            }
            tip.innerHTML = html;
          }
        },
        onCreateLabel : function(domElement, node) {
          //domElement.style.opacity = 0.01;

          if(node.id == "root"){
            domElement.innerHTML = "<div class='treemapRootNodeLabeltext'>&nbsp;</div>";
          }else{
            domElement.innerHTML = "<div class='treemapNodeLabeltext'>"+node.name+"</div>";
            var style = domElement.style;
            style.display = '';
            style.border = '1px solid transparent';

            domElement.onmouseover = function() {
              style.border = '1px solid #ffffff';
            };

            domElement.onmouseout = function() {
              style.border = '1px solid transparent';
            };
          }

        }
      });
  clusterMemberTreeMap.loadJSON(json);
  clusterMemberTreeMap.refresh();
}

//function used for creating blank TreeMap for member's on Servre Groups View
function createMembersTreeMapSG(){
  var dataVal = {
      "$area" : 1,
      "initial" : true
    };
    var json = {
      "children" : {},
      "data" : dataVal,
      "id" : "root",
      "name" : "sgMembers"
    };

    clusterSGMemberTreeMap = new $jit.TM.Squarified(
        {

          injectInto : 'GraphTreeMapSG',
          //levelsToShow : 1,
          titleHeight : 15,
          background : '#a0c44a',
          offset : 1,
          userData : {
            isNodeEntered : false
          },
          Label : {
            type : 'HTML',
            size : 9
          },
          Node : {
            CanvasStyles : {
              shadowBlur : 0
            }
          },
          Events : {
            enable : true,
            onMouseEnter : function(node, eventInfo) {
              if (node && node.id != "root") {
                node.setCanvasStyle('shadowBlur', 7);
                node.setData('border', '#ffffff');

                clusterSGMemberTreeMap.fx.plotNode(node,
                    clusterSGMemberTreeMap.canvas);
                clusterSGMemberTreeMap.labels.plotLabel(
                    clusterSGMemberTreeMap.canvas, node);
              }
            },
            onMouseLeave : function(node) {
              if (node && node.id != "root") {
                node.removeData('border', '#ffffff');
                node.removeCanvasStyle('shadowBlur');
                clusterSGMemberTreeMap.plot();
              }
            },
            onClick : function(node) {
              if (node) {
                if(node._depth == 1){
                  var isSGNodeEntered = clusterSGMemberTreeMap.config.userData.isNodeEntered;
                  if(!isSGNodeEntered){
                    clusterSGMemberTreeMap.config.userData.isNodeEntered = true;
                    clusterSGMemberTreeMap.enter(node);
                  }else{
                    clusterSGMemberTreeMap.config.userData.isNodeEntered = false;
                    clusterSGMemberTreeMap.out();
                  }
                } else if(node._depth == 2 && (!node.data.initial)){
                  location.href = 'MemberDetails.html?member=' + node.data.id
                  + '&memberName=' + node.data.name;
                }
              }
            },
            onRightClick: function() {
              clusterSGMemberTreeMap.out();
            }
          },

          Tips : {
            enable : true,
            offsetX : 5,
            offsetY : 5,
            onShow : function(tip, node, isLeaf, domElement) {
              var html = "";
              if(node && node.id != "root"){
                var data = node.data;
                if (!data.initial) {
                  if(node._depth == 1){
                    html = "<div class=\"tip-title\"><div><div class='popupHeading'>" + node.name + "</div>";
                  }else if(node._depth == 2){
                    html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
                        + node.name
                        + "</div>"
                        + "<div class='popupFirstRow'><div class='popupRowBorder borderBottomZero'>"
                        + "<div class='labeltext left display-block width-70'><span class='left'>CPU Usage</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + data.cpuUsage
                        + "<span class='fontSize15'>%</span></span></div>"
                        + "</div></div>"
                        + "<div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Memory Usage</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + data.heapUsage
                        + "<span class='font-size15 paddingL5'>MB</span>"
                        + "</div></div></div>"
                        + "<div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Load Avg.</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + applyNotApplicableCheck(data.loadAvg)
                        + "</div></div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Threads</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + data.threads
                        + "</div></div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Sockets</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + applyNotApplicableCheck(data.sockets)
                        + "</div></div></div><div class='popupRowBorder borderBottomZero'>"
                        + "<div class='labeltext left display-block width-70'><span class='left'>GemFire Version</span>"
                        + "</div><div class='right width-30'><div class='color-d2d5d7 font-size14'>"
                        + data.gemfireVersion + "</div></div>" + "</div>" + ""
                        + "</div></div>" + "</div>";
                  }
                }else{
                  html = "<div class=\"tip-title\"><div><div class='popupHeading'>Loading Server Groups and Members</div>";
                }
              }else{
                html = "<div class=\"tip-title\"><div><div class='popupHeading'>Cluster's Server Groups and Members</div>";
              }
              tip.innerHTML = html;
            }
          },
          onBeforePlotNode: function(node) {
            if(node._depth == 0) {
              // Hide root node title bar
              node.setData('color', '#132634');  
            } else if(node._depth == 1) {
              node.setData('color', '#506D94');
            }
          },
          onCreateLabel : function(domElement, node) {
            //domElement.style.opacity = 0.01;
            if(node._depth != 0){
              domElement.innerHTML = "<div class='treemapNodeLabeltext'>"+node.name+"</div>";
              var style = domElement.style;
              style.display = '';
              style.border = '1px solid transparent';

              domElement.onmouseover = function() {
                style.border = '1px solid #ffffff';
                // Highlight this member in all server groups
                if(this.id.indexOf("(!)") != -1){
                  var currSGMemberId = this.id.substring(this.id.indexOf("(!)")+3);
                  var sgMembersDOM = $("div[id$='"+currSGMemberId+"']");
                  for(var cntr=0; cntr<sgMembersDOM.length; cntr++){
                    sgMembersDOM[cntr].style.border = '1px solid #ffffff';
                  }
                }
              };

              domElement.onmouseout = function() {
                style.border = '1px solid transparent';
                // Remove Highlighting of this member in all server groups
                if(this.id.indexOf("(!)") != -1){
                  var currSGMemberId = this.id.substring(this.id.indexOf("(!)")+3);
                  var sgMembersDOM = $("div[id$='"+currSGMemberId+"']");
                  for(var cntr=0; cntr<sgMembersDOM.length; cntr++){
                    sgMembersDOM[cntr].style.border = '1px solid transparent';
                  }
                }
              };
            }
          }
        });
    clusterSGMemberTreeMap.loadJSON(json);
    clusterSGMemberTreeMap.refresh();
}

//function used for creating blank TreeMap for member's on Redundancy Zones View
function createMembersTreeMapRZ(){
  var dataVal = {
      "$area" : 1,
      "initial" : true
    };
    var json = {
      "children" : {},
      "data" : dataVal,
      "id" : "root",
      "name" : "rzMembers"
    };

    clusterRZMemberTreeMap = new $jit.TM.Squarified(
        {

          injectInto : 'GraphTreeMapRZ',
          //levelsToShow : 1,
          titleHeight : 15,
          background : '#a0c44a',
          offset : 1,
          userData : {
            isNodeEntered : false
          },
          Label : {
            type : 'HTML',
            size : 9
          },
          Node : {
            CanvasStyles : {
              shadowBlur : 0
            }
          },
          Events : {
            enable : true,
            onMouseEnter : function(node, eventInfo) {
              if (node && node.id != "root") {
                node.setCanvasStyle('shadowBlur', 7);
                node.setData('border', '#ffffff');

                clusterRZMemberTreeMap.fx.plotNode(node,
                    clusterRZMemberTreeMap.canvas);
                clusterRZMemberTreeMap.labels.plotLabel(
                    clusterRZMemberTreeMap.canvas, node);
              }
            },
            onMouseLeave : function(node) {
              if (node && node.id != "root") {
                node.removeData('border', '#ffffff');
                node.removeCanvasStyle('shadowBlur');
                clusterRZMemberTreeMap.plot();
              }
            },
            onClick : function(node) {
              if (node) {
                if(node._depth == 1){
                  var isRZNodeEntered = clusterRZMemberTreeMap.config.userData.isNodeEntered;
                  if(!isRZNodeEntered){
                    clusterRZMemberTreeMap.config.userData.isNodeEntered = true;
                    clusterRZMemberTreeMap.enter(node);
                  }else{
                    clusterRZMemberTreeMap.config.userData.isNodeEntered = false;
                    clusterRZMemberTreeMap.out();
                  }
                } else if(node._depth == 2 && (!node.data.initial)){
                  location.href = 'MemberDetails.html?member=' + node.data.id
                  + '&memberName=' + node.data.name;
                }
              }
            },
            onRightClick: function() {
              clusterRZMemberTreeMap.out();
            }
          },

          Tips : {
            enable : true,
            offsetX : 5,
            offsetY : 5,
            onShow : function(tip, node, isLeaf, domElement) {
              var html = "";
              if(node && node.id != "root"){
                var data = node.data;
                if (!data.initial) {
                  if(node._depth == 1){
                    html = "<div class=\"tip-title\"><div><div class='popupHeading'>" + node.name + "</div>";
                  }else if(node._depth == 2){
                    html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
                        + node.name
                        + "</div>"
                        + "<div class='popupFirstRow'><div class='popupRowBorder borderBottomZero'>"
                        + "<div class='labeltext left display-block width-70'><span class='left'>CPU Usage</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + data.cpuUsage
                        + "<span class='fontSize15'>%</span></span></div>"
                        + "</div></div>"
                        + "<div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Memory Usage</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + data.heapUsage
                        + "<span class='font-size15 paddingL5'>MB</span>"
                        + "</div></div></div>"
                        + "<div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Load Avg.</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + applyNotApplicableCheck(data.loadAvg)
                        + "</div></div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Threads</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + data.threads
                        + "</div></div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-70'>"
                        + "<span class='left'>Sockets</span></div><div class='right width-30'>"
                        + "<div class='color-d2d5d7 font-size14'>"
                        + applyNotApplicableCheck(data.sockets)
                        + "</div></div></div><div class='popupRowBorder borderBottomZero'>"
                        + "<div class='labeltext left display-block width-70'><span class='left'>GemFire Version</span>"
                        + "</div><div class='right width-30'><div class='color-d2d5d7 font-size14'>"
                        + data.gemfireVersion + "</div></div>" + "</div>" + ""
                        + "</div></div>" + "</div>";
                  }
                }else{
                  html = "<div class=\"tip-title\"><div><div class='popupHeading'>Loading Server Groups and Members</div>";
                }
              }else{
                html = "<div class=\"tip-title\"><div><div class='popupHeading'>Cluster's Server Groups and Members</div>";
              }
              tip.innerHTML = html;
            }
          },
          onBeforePlotNode: function(node) {  
            if(node._depth == 0) {
              // Hide root node title bar 
              node.setData('color', '#132634');  
            } else if(node._depth == 1) {
              node.setData('color', '#506D94');
            }
          },
          onCreateLabel : function(domElement, node) {
            //domElement.style.opacity = 0.01;
            if(node._depth != 0){
              domElement.innerHTML = "<div class='treemapNodeLabeltext'>"+node.name+"</div>";
              var style = domElement.style;
              style.display = '';
              style.border = '1px solid transparent';

              domElement.onmouseover = function() {
                style.border = '1px solid #ffffff';
                // Highlight this member in all redundancy zones
                if(this.id.indexOf("(!)") != -1){
                  var currRZMemberId = this.id.substring(this.id.indexOf("(!)")+3);
                  var rzMembersDOM = $("div[id$='"+currRZMemberId+"']");
                  for(var cntr=0; cntr<rzMembersDOM.length; cntr++){
                    rzMembersDOM[cntr].style.border = '1px solid #ffffff';
                  }
                }
              };

              domElement.onmouseout = function() {
                style.border = '1px solid transparent';
                // Remove Highlighting of this member in all redundancy zones
                if(this.id.indexOf("(!)") != -1){
                  var currRZMemberId = this.id.substring(this.id.indexOf("(!)")+3);
                  var rzMembersDOM = $("div[id$='"+currRZMemberId+"']");
                  for(var cntr=0; cntr<rzMembersDOM.length; cntr++){
                    rzMembersDOM[cntr].style.border = '1px solid transparent';
                  }
                }
              };
            }
          }
        });
    clusterRZMemberTreeMap.loadJSON(json);
    clusterRZMemberTreeMap.refresh();
}

//function used for creating blank TreeMap for cluster's regions on Data View
function createRegionsTreeMapDefault(){
  var dataVal = {
      "$area" : 1
    };
    var json = {
      "children" : {},
      "data" : dataVal,
      "id" : "root",
      "name" : "Regions"
    };

    clusterRegionsTreeMap = new $jit.TM.Squarified(
        {

          injectInto : 'GraphTreeMapClusterData',
          levelsToShow : 1,
          titleHeight : 0,
          background : '#8c9aab',
          offset : 2,
          Label : {
            type : 'Native',//HTML
            size : 9
          },
          Node : {
            CanvasStyles : {
              shadowBlur : 0
            }
          },
          Events : {
            enable : true,
            onMouseEnter : function(node, eventInfo) {
              if (node && node.id != "root") {
                node.setCanvasStyle('shadowBlur', 7);
                node.setData('border', '#ffffff');
                clusterRegionsTreeMap.fx.plotNode(node, clusterRegionsTreeMap.canvas);
                clusterRegionsTreeMap.labels.plotLabel(clusterRegionsTreeMap.canvas, node);
              }
            },
            onMouseLeave : function(node) {
              if (node && node.id != "root") {
                node.removeData('border', '#ffffff');
                node.removeCanvasStyle('shadowBlur');
                clusterRegionsTreeMap.plot();
              }
            },
            onClick : function(node) {
              if (node.id != "root") {
                location.href ='regionDetail.html?regionFullPath='+node.data.regionPath;
              }
            }
          },

          Tips : {
            enable : true,
            offsetX : 5,
            offsetY : 5,
            onShow : function(tip, node, isLeaf, domElement) {

              var data = node.data;
              var html = "";
              if (data.type) {
                html = "<div class=\"tip-title\"><div><div class='popupHeading'>"
                    + node.id
                    + "</div>"
                    + "<div class='popupFirstRow'><div class='popupRowBorder borderBottomZero'>"
                    + "<div class='labeltext left display-block width-45'><span class='left'>"
                    + "Type</span></div><div class='right width-55'>"
                    + "<div class='color-d2d5d7 font-size14 popInnerBlockEllipsis'>"
                    + data.type
                    + "</div>"
                    + "</div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-45'>"
                    + "<span class='left'>" + jQuery.i18n.prop('pulse-entrycount-custom') + "</span></div><div class='right width-55'>"
                    + "<div class='color-d2d5d7 font-size14'>"
                    + data.systemRegionEntryCount
                    + "</div>"
                    + "</div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-45'>"
                    + "<span class='left'>" + jQuery.i18n.prop('pulse-entrysize-custom') + "</span></div><div class='right width-55'>"
                    + "<div class='color-d2d5d7 font-size14'>" 
                    + data.entrySize
                    + "</div>"
                    /*+ "</div></div><div class='popupRowBorder borderBottomZero'><div class='labeltext left display-block width-45'>"
                    + "<span class='left'>Compression Codec</span></div><div class='right width-55'>"
                    + "<div class='color-d2d5d7 font-size14'>"
                    + data.compressionCodec
                    + "</div>"*/
                    + "</div></div></div></div>" + "</div>";
              } else {
                html = "<div class=\"tip-title\"><div><div class='popupHeading'>No " + jQuery.i18n.prop('pulse-regiontabletooltip-custom') + " Found</div>";
              }

              tip.innerHTML = html;
            }
          },
          onCreateLabel : function(domElement, node) {
            domElement.innerHTML = node.name;
            var style = domElement.style;
            style.cursor = 'default';
            style.border = '1px solid';

            style.background = 'none repeat scroll 0 0 #606060';
            domElement.onmouseover = function() {
              style.border = '1px solid #9FD4FF';
              style.background = 'none repeat scroll 0 0 #9FD4FF';
            };
            domElement.onmouseout = function() {
              style.border = '1px solid';
              style.background = 'none repeat scroll 0 0 #606060';
            };

          }
        });
    clusterRegionsTreeMap.loadJSON(json);
    clusterRegionsTreeMap.refresh();
}

// function used for creating blank grids for member list for Cluster Details
// Screen

function createMemberGridDefault() {
  
  jQuery("#memberList").jqGrid(
      {
        datatype : "local",
        height : 360,
        width : 740,
        rowNum : 200,
        shrinkToFit : false,
        colNames : [ 'ID', 'Name', 'Host', 'Heap Usage (MB)', 'CPU Usage (%)',
            'Uptime', 'Clients', 'CurrentHeapSize', 'Load Avg', 'Threads',
            'Sockets'],
        colModel : [
            {
              name : 'memberId',
              index : 'memberId',
              width : 170,
              sorttype : "string",
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true
            },
            {
              name : 'name',
              index : 'name',
              width : 150,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'host',
              index : 'host',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'currentHeapUsage',
              index : 'currentHeapUsage',
              width : 110,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "float"
            },
            {
              name : 'cpuUsage',
              index : 'cpuUsage',
              align : 'right',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "float"
            },
            {
              name : 'uptime',
              index : 'uptime',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'clients',
              index : 'clients',
              width : 100,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            }, {
              name : 'currentHeapUsage',
              index : 'currentHeapUsage',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'loadAvg',
              index : 'loadAvg',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'threads',
              index : 'threads',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'sockets',
              index : 'sockets',
              align : 'center',
              width : 0,
              hidden : true
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "name"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#memberList").getGridParam('userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        onSelectRow : function(rowid, status, event) {
          if (!event || event.which == 1) { // mouse left button click
            var member = rowid.split("&");
            location.href = 'MemberDetails.html?member=' + member[0]
                + '&memberName=' + member[1];
          }
        },
        resizeStop : function(width, index) {

          var memberRegionsList = $('#gview_memberList');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          var api = memberRegionsListChild.data('jsp');
          api.reinitialise();

          memberRegionsList = $('#gview_memberList');
          memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberList');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
          
          $('#default_grid_button').click();
          refreshTheGrid($('#default_grid_button'));
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_memberList');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberList');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
        }
      });
}

function createMemberGridSG() {
  
  jQuery("#memberListSG").jqGrid(
      {
        datatype : "local",
        height : 360,
        width : 740,
        rowNum : 200,
        shrinkToFit : false,
        colNames : [ 'Server Group','ID', 'Name', 'Host', 'Heap Usage (MB)', 'CPU Usage (%)',
            'Uptime', 'Clients', 'CurrentHeapSize', 'Load Avg', 'Threads',
            'Sockets'],
        colModel : [
            {
              name : 'serverGroup',
              index : 'serverGroup',
              width : 170,
              sorttype : "string",
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true
            },
            {
              name : 'memberId',
              index : 'memberId',
              width : 170,
              sorttype : "string",
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true
            },
            {
              name : 'name',
              index : 'name',
              width : 150,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'host',
              index : 'host',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'currentHeapUsage',
              index : 'currentHeapUsage',
              width : 110,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "float"
            },
            {
              name : 'cpuUsage',
              index : 'cpuUsage',
              align : 'right',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "float"
            },
            {
              name : 'uptime',
              index : 'uptime',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'clients',
              index : 'clients',
              width : 100,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            }, {
              name : 'currentHeapUsage',
              index : 'currentHeapUsage',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'loadAvg',
              index : 'loadAvg',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'threads',
              index : 'threads',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'sockets',
              index : 'sockets',
              align : 'center',
              width : 0,
              hidden : true
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "serverGroup"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#memberListSG").getGridParam('userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        onSelectRow : function(rowid, status, event) {
          if (!event || event.which == 1) { // mouse left button click
            var member = rowid.split("&");
            location.href = 'MemberDetails.html?member=' + member[0]
                + '&memberName=' + member[1];
          }
        },
        resizeStop : function(width, index) {

          var memberRegionsList = $('#gview_memberListSG');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          var api = memberRegionsListChild.data('jsp');
          api.reinitialise();

          memberRegionsList = $('#gview_memberListSG');
          memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberListSG');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
          
          $('#servergroups_grid_button').click();
          refreshTheGrid($('#servergroups_grid_button'));
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_memberListSG');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberListSG');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
        }
      });
}

function createMemberGridRZ() {
  
  jQuery("#memberListRZ").jqGrid(
      {
        datatype : "local",
        height : 360,
        width : 740,
        rowNum : 200,
        shrinkToFit : false,
        colNames : [ 'Redundancy Zone','ID', 'Name', 'Host', 'Heap Usage (MB)', 'CPU Usage (%)',
            'Uptime', 'Clients', 'CurrentHeapSize', 'Load Avg', 'Threads',
            'Sockets'],
        colModel : [
            {
              name : 'redundancyZone',
              index : 'redundancyZone',
              width : 170,
              sorttype : "string",
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true
            },
            {
              name : 'memberId',
              index : 'memberId',
              width : 170,
              sorttype : "string",
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true
            },
            {
              name : 'name',
              index : 'name',
              width : 150,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'host',
              index : 'host',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "string"
            },
            {
              name : 'currentHeapUsage',
              index : 'currentHeapUsage',
              width : 110,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "float"
            },
            {
              name : 'cpuUsage',
              index : 'cpuUsage',
              align : 'right',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "float"
            },
            {
              name : 'uptime',
              index : 'uptime',
              width : 100,
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            },
            {
              name : 'clients',
              index : 'clients',
              width : 100,
              align : 'right',
              cellattr : function(rowId, val, rawObject, cm, rdata) {
                return formMemberGridToolTip(rawObject);
              },
              sortable : true,
              sorttype : "int"
            }, {
              name : 'currentHeapUsage',
              index : 'currentHeapUsage',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'loadAvg',
              index : 'loadAvg',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'threads',
              index : 'threads',
              align : 'center',
              width : 0,
              hidden : true
            }, {
              name : 'sockets',
              index : 'sockets',
              align : 'center',
              width : 0,
              hidden : true
            } ],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "redundancyZone"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#memberListRZ").getGridParam('userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        onSelectRow : function(rowid, status, event) {
          if (!event || event.which == 1) { // mouse left button click
            var member = rowid.split("&");
            location.href = 'MemberDetails.html?member=' + member[0]
                + '&memberName=' + member[1];
          }
        },
        resizeStop : function(width, index) {

          var memberRegionsList = $('#gview_memberListRZ');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          var api = memberRegionsListChild.data('jsp');
          api.reinitialise();

          memberRegionsList = $('#gview_memberListRZ');
          memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberListRZ');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
          
          $('#redundancyzones_grid_button').click();
          refreshTheGrid($('#redundancyzones_grid_button'));
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_memberListRZ');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_memberListRZ');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
        }
      });
}

function createRegionsGridDefault() {
  
  jQuery("#regionsList").jqGrid(
      {
        datatype : "local",
        height : 360,
        width : 740,
        rowNum : 200,
        shrinkToFit : false,
        colNames : [ 'Region Name', 'Type', 'Entry Count', 'Entry Size',
                     'Region Path', 'Member Count', 'Read Rates', 'Write Rates',
                     'Persistence', 'Entry Count', 'Empty Nodes', 'Data Usage',
                     'Total Data Usage', 'Memory Usage', 'Total Memory',
                     'Member Names', 'Writes', 'Reads','Off Heap Enabled',
                     'Compression Codec','HDFS Write Only' ],
        colModel : [ {
          name : 'name',
          index : 'name',
          width : 120,
          sortable : true,
          sorttype : "string"
        }, {
          name : 'type',
          index : 'type',
          width : 130,
          sortable : true,
          sorttype : "string"
        }, {
          name : 'systemRegionEntryCount',
          index : 'systemRegionEntryCount',
          width : 100,
          align : 'right',
          sortable : true,
          sorttype : "int"
        }, {
          name : 'entrySize',
          index : 'entrySize',
          width : 100,
          align : 'right',
          sortable : true,
          sorttype : "int"
        }, {
          name : 'regionPath',
          index : 'regionPath',
          //hidden : true,
          width : 130,
          sortable : true,
          sorttype : "string"
        }, {
          name : 'memberCount',
          index : 'memberCount',
          hidden : true
        }, {
          name : 'getsRate',
          index : 'getsRate',
          hidden : true
        }, {
          name : 'putsRate',
          index : 'putsRate',
          hidden : true
        }, {
          name : 'persistence',
          index : 'persistence',
          //hidden : true,
          width : 100,
          sortable : true,
          sorttype : "string"
        }, {
          name : 'systemRegionEntryCount',
          index : 'systemRegionEntryCount',
          hidden : true
        }, {
          name : 'emptyNodes',
          index : 'emptyNodes',
          hidden : true
        }, {
          name : 'dataUsage',
          index : 'dataUsage',
          hidden : true
        }, {
          name : 'totalDataUsage',
          index : 'totalDataUsage',
          hidden : true
        }, {
          name : 'memoryUsage',
          index : 'memoryUsage',
          hidden : true
        }, {
          name : 'totalMemory',
          index : 'totalMemory',
          hidden : true
        }, {
          name : 'memberNames',
          index : 'memberNames',
          hidden : true
        }, {
          name : 'writes',
          index : 'writes',
          hidden : true
        }, {
          name : 'reads',
          index : 'reads',
          hidden : true
        }, {
          name : 'isEnableOffHeapMemory',
          index : 'isEnableOffHeapMemory',
          hidden : true,
          width : 120,
          sortable : true,
          sorttype : "string"
        }, {
          name : 'compressionCodec',
          index : 'compressionCodec',
          hidden : true
        }, {
          name : 'isHDFSWriteOnly',
          index : 'isHDFSWriteOnly',
          hidden : true
        }],
        userData : {
          "sortOrder" : "asc",
          "sortColName" : "name"
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#regionsList").getGridParam('userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
        },
        onSelectRow : function(rowid, status, event) {
          if (!event || event.which == 1) { // mouse left button click
            var region = rowid.split("&");
            location.href = 'regionDetail.html?regionFullPath=' + region[0];
          }
        },
        resizeStop : function(width, index) {

          var clusterRegionsList = $('#gview_regionsList');
          var clusterRegionsListChild = clusterRegionsList
              .children('.ui-jqgrid-bdiv');
          var api = clusterRegionsListChild.data('jsp');
          api.reinitialise();

          /*memberRegionsList = $('#gview_regionsList');
          memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');
          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_regionsList');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });*/
          
          $('#data_grid_button').click();
          refreshTheGrid($('#data_grid_button'));
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });

          var memberRegionsList = $('#gview_regionsList');
          var memberRegionsListChild = memberRegionsList
              .children('.ui-jqgrid-bdiv');

          memberRegionsListChild.unbind('jsp-scroll-x');
          memberRegionsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var mRList = $('#gview_regionsList');
            var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            mRLC.css("position", "relative");
            mRLC.css('right', scrollPositionX);
          });
          
          // change col names depend on product
          if(CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()){
            jQuery("#regionsList").jqGrid('setLabel', 'name', jQuery.i18n.prop('pulse-regiontableName-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'regionPath', jQuery.i18n.prop('pulse-regiontablePathColName-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'getsRate', jQuery.i18n.prop('pulse-readsRate-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'putsRate', jQuery.i18n.prop('pulse-writesRate-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'writes', jQuery.i18n.prop('pulse-writes-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'reads', jQuery.i18n.prop('pulse-reads-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'systemRegionEntryCount', jQuery.i18n.prop('pulse-entrycount-custom'));
            jQuery("#regionsList").jqGrid('setLabel', 'entrySize', jQuery.i18n.prop('pulse-entrysize-custom'));
          }
        }
      });
}


/* builds and returns json from given members details sent by server */
function buildDefaultMembersTreeMapData(members) {

  var childerensVal = [];
  for ( var i = 0; i < members.length; i++) {

    var color = "#a0c44a";
    // setting node color according to the status of member
    // like if member has severe notification then the node color will be
    // '#ebbf0f'
    for ( var j = 0; j < warningAlerts.length; j++) {
      if (members[i].name == warningAlerts[j].memberName) {
        color = '#ebbf0f';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#de5a25'
    for ( var j = 0; j < errorAlerts.length; j++) {
      if (members[i].name == errorAlerts[j].memberName) {
        color = '#de5a25';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#b82811'
    for ( var j = 0; j < severAlerts.length; j++) {
      if (members[i].name == severAlerts[j].memberName) {
        color = '#b82811';
        break;
      }
    }
    var heapSize = members[i][currentHotspotAttribiute];
    // if (heapSize == 0)
    // heapSize = 1;
    var name = "";
    name = members[i].name;
    var id = "";
    id = members[i].memberId;
    // passing all the required information of member to tooltip
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
      "gemfireVersion" : members[i].gemfireVersion,
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
  var localjson = {
    "children" : childerensVal,
    "data" : {},
    "id" : "root",
    "name" : "Members"
  };
  return localjson;
}

/* builds treemap json for Server Groups  */
function buildSGMembersTreeMapData(members) {
  
  // Server Groups
  var serverGroups = new Array();
  var sgChildren = [];

  for ( var i = 0; i < members.length; i++) {
    // add Server Group if not present
    var memServerGroups = members[i].serverGroups;

    for ( var cntr = 0; cntr < memServerGroups.length; cntr++) {
      if ($.inArray(memServerGroups[cntr], serverGroups) == -1) {
        serverGroups.push(memServerGroups[cntr]);
        var sgChild = {
            "children" : new Array(),
            "data" : {
              "$area" : 1,
              "initial" : false
             },
            "id" : memServerGroups[cntr],
            "name" : memServerGroups[cntr]
          };
        sgChildren.push(sgChild);
      }
    }

  }

  var localjson = {
    "children" : sgChildren,
    "data" : {},
    "id" : "root",
    "name" : "sgMembers"
  };


  for ( var i = 0; i < members.length; i++) {

    var color = "#a0c44a";
    // setting node color according to the status of member
    // like if member has severe notification then the node color will be
    // '#ebbf0f'
    for ( var j = 0; j < warningAlerts.length; j++) {
      if (members[i].name == warningAlerts[j].memberName) {
        color = '#ebbf0f';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#de5a25'
    for ( var j = 0; j < errorAlerts.length; j++) {
      if (members[i].name == errorAlerts[j].memberName) {
        color = '#de5a25';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#b82811'
    for ( var j = 0; j < severAlerts.length; j++) {
      if (members[i].name == severAlerts[j].memberName) {
        color = '#b82811';
        break;
      }
    }
    var heapSize = members[i][currentHotspotAttribiute];
    // if (heapSize == 0)
    // heapSize = 1;
    var name = "";
    name = members[i].name;
    var id = "";
    id = members[i].memberId;
    // passing all the required information of member to tooltip
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
      "gemfireVersion" : members[i].gemfireVersion,
      "initial" : false
    };
    var childrenVal = {
      "children" : [],
      "data" : dataVal,
      "id" : id,
      "name" : name
    };

    var memServerGroups = members[i].serverGroups;
    
    for(var k=0; k<memServerGroups.length; k++){
      var localJsonChildren = localjson.children;
      for(var l=0; l<localJsonChildren.length; l++){
        if(localJsonChildren[l].name == memServerGroups[k]){
          // create copy of member treemap child for the current server group
          var copyOFChildren = jQuery.extend(true, {}, childrenVal);
          copyOFChildren.id = memServerGroups[k]+"(!)"+childrenVal.id;
          // Add member child in server group treemap object
          localJsonChildren[l].children.push(copyOFChildren);
        }
      }
    }
  }
  
  return localjson;
}

/* builds treemap json for Redundancy Zones  */
function buildRZMembersTreeMapData(members) {
  
  // Redundancy Zones
  var redundancyZones = new Array();
  var rzChildren = [];

  for ( var i = 0; i < members.length; i++) {
    // add redundancy zones if not present
    var memRedundancyZones = members[i].redundancyZones;

    for ( var cntr = 0; cntr < memRedundancyZones.length; cntr++) {
      if ($.inArray(memRedundancyZones[cntr], redundancyZones) == -1) {
        redundancyZones.push(memRedundancyZones[cntr]);
        var rzChild = {
            "children" : new Array(),
            "data" : {
              "$area" : 1,
              "initial" : false
             },
            "id" : memRedundancyZones[cntr],
            "name" : memRedundancyZones[cntr]
          };
        rzChildren.push(rzChild);
      }
    }

  }

  var localjson = {
    "children" : rzChildren,
    "data" : {},
    "id" : "root",
    "name" : "rzMembers"
  };


  for ( var i = 0; i < members.length; i++) {

    var color = "#a0c44a";
    // setting node color according to the status of member
    // like if member has severe notification then the node color will be
    // '#ebbf0f'
    for ( var j = 0; j < warningAlerts.length; j++) {
      if (members[i].name == warningAlerts[j].memberName) {
        color = '#ebbf0f';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#de5a25'
    for ( var j = 0; j < errorAlerts.length; j++) {
      if (members[i].name == errorAlerts[j].memberName) {
        color = '#de5a25';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#b82811'
    for ( var j = 0; j < severAlerts.length; j++) {
      if (members[i].name == severAlerts[j].memberName) {
        color = '#b82811';
        break;
      }
    }
    var heapSize = members[i][currentHotspotAttribiute];
    // if (heapSize == 0)
    // heapSize = 1;
    var name = "";
    name = members[i].name;
    var id = "";
    id = members[i].memberId;
    // passing all the required information of member to tooltip
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
      "gemfireVersion" : members[i].gemfireVersion,
      "initial" : false
    };
    var childrenVal = {
      "children" : [],
      "data" : dataVal,
      "id" : id,
      "name" : name
    };

    var memRedundancyZones = members[i].redundancyZones;
    for(var k=0; k<memRedundancyZones.length; k++){
      var localJsonChildren = localjson.children;
      for(var l=0; l<localJsonChildren.length; l++){
        if(localJsonChildren[l].name == memRedundancyZones[k]){
          // create copy of member treemap child for the current redundancy zone
          var copyOFChildren = jQuery.extend(true, {}, childrenVal);
          copyOFChildren.id = memRedundancyZones[k]+"(!)"+childrenVal.id;
          // Add member child in reduadancy zone treemap object
          localJsonChildren[l].children.push(copyOFChildren);
        }
      }
    }
  }
  
  return localjson;
}

/* builds treemap json for Cluster Regions  */
function buildRegionsTreeMapData(clusterRegions) {

  // variable to store value of total of entry counts of all regions
  var totalOfEntryCounts = 0;
  // flag to determine if all regions are having entry count = 0
  var flagSetEntryCountsToZero = false;

  // Calculate the total of all regions entry counts
  for ( var i = 0; i < clusterRegions.length; i++) {
    totalOfEntryCounts += clusterRegions[i].entryCount;
  }

  // If totalOfEntryCounts is zero and at least one region is present
  // then set flagSetEntryCountsToZero to avoid displaying circles
  // in treemap as all valid regions are zero area regions and also display
  // all regions with evenly placement of blocks
  if (totalOfEntryCounts == 0 && clusterRegions.length > 0) {
    flagSetEntryCountsToZero = true;
  }

  var childerensVal = [];  

  for ( var i = 0; i < clusterRegions.length; i++) {

    var entryCount = clusterRegions[i].systemRegionEntryCount;
    // If flagSetEntryCountsToZero is true then set entry count to
    // display all
    // regions with evenly placement of blocks
    if (flagSetEntryCountsToZero && entryCount == 0) {
      entryCount = 1;
    }
    var color = colorCodeForRegions;
    if(clusterRegions[i].systemRegionEntryCount == 0){
      color = colorCodeForZeroEntryCountRegions;
    }

    var wanEnabled = clusterRegions[i].wanEnabled;
    var wanEnabledtxt = "";
    if (wanEnabled == true)
      wanEnabledtxt = "WAN Enabled";
    // if (entryCount == 0)
    // entryCount = 1;
    var dataVal = {
      "name" : clusterRegions[i].name,
      "id" : clusterRegions[i].regionPath,
      "$color" : color,
      "$area" : entryCount,
      "systemRegionEntryCount" : clusterRegions[i].systemRegionEntryCount,
      "type" : clusterRegions[i].type,
      "regionPath" : clusterRegions[i].regionPath,
      "entrySize" : clusterRegions[i].entrySize,
      "memberCount" : clusterRegions[i].memberCount,
      "writes" : clusterRegions[i].putsRate,
      "reads" : clusterRegions[i].getsRate,
      "emptyNodes" : clusterRegions[i].emptyNodes,
      "persistence" : clusterRegions[i].persistence,
      "isEnableOffHeapMemory" : clusterRegions[i].isEnableOffHeapMemory,
      "compressionCodec" : clusterRegions[i].compressionCodec,
      "isHDFSWriteOnly" : clusterRegions[i].isHDFSWriteOnly,
      "memberNames" : clusterRegions[i].memberNames,
      "memoryWritesTrend" : clusterRegions[i].memoryWritesTrend,
      "memoryReadsTrend" : clusterRegions[i].memoryReadsTrend,
      "diskWritesTrend" : clusterRegions[i].diskWritesTrend,
      "diskReadsTrend" : clusterRegions[i].diskReadsTrend,
      "memoryUsage" : clusterRegions[i].memoryUsage,
      "dataUsage" : clusterRegions[i].dataUsage,
      "totalDataUsage" : clusterRegions[i].totalDataUsage,
      "totalMemory" : clusterRegions[i].totalMemory
    };
    var childrenVal = {
      "children" : [],
      "data" : dataVal,
      "id" : clusterRegions[i].regionPath,
      "name" : wanEnabledtxt
    };
    
    childerensVal[i] = childrenVal;
  }

   

  var json = {
    "children" : childerensVal,
    "data" : {},
    "id" : "root",
    "name" : "Regions"
  };

  return json;

}

/* builds grid data for Server Groups  */
function buildSGMembersGridData(members) {
  var sgMembersGridData = new Array();
  // Server Groups
  var serverGroups = new Array();

  for ( var i = 0; i < members.length; i++) {
    // add Server Group if not present
    var memServerGroups = members[i].serverGroups;

    for ( var cntr = 0; cntr < memServerGroups.length; cntr++) {
      if ($.inArray(memServerGroups[cntr], serverGroups) == -1) {
        serverGroups.push(memServerGroups[cntr]);
      }
    }

  }
  
  for(var i=0; i<serverGroups.length; i++){
    var serverGroup = serverGroups[i];
    for(var j=0; j<members.length; j++){
      if ($.inArray(serverGroup, members[j].serverGroups) > -1) {
        var memberObjectNew = jQuery.extend({}, members[j]);
        memberObjectNew.serverGroup = serverGroup;
        sgMembersGridData.push(memberObjectNew);
      }
    }
  }
  
  return sgMembersGridData;
}

/* builds grid data for Redundancy Zones  */
function buildRZMembersGridData(members) {
  var rzMembersGridData = new Array();
  // Server Groups
  var redundancyZones = new Array();

  for ( var i = 0; i < members.length; i++) {
    // add Redundancy Zones if not present
    var memRedundancyZones = members[i].redundancyZones;

    for ( var cntr = 0; cntr < memRedundancyZones.length; cntr++) {
      if ($.inArray(memRedundancyZones[cntr], redundancyZones) == -1) {
        redundancyZones.push(memRedundancyZones[cntr]);
      }
    }

  }
  
  for(var i=0; i<redundancyZones.length; i++){
    var redundancyZone = redundancyZones[i];
    for(var j=0; j<members.length; j++){
      if ($.inArray(redundancyZone, members[j].redundancyZones) > -1) {
        var memberObjectNew = jQuery.extend({}, members[j]);
        memberObjectNew.redundancyZone = redundancyZone;
        rzMembersGridData.push(memberObjectNew);
      }
    }
  }
  
  return rzMembersGridData;
}

// Tool tip for members in grid
function formMemberGridToolTip(rawObject) {
  return 'title="Name: ' + rawObject.name + ' , CPU Usage: '
  + rawObject.cpuUsage + '% , Heap Usage: '
  + rawObject.currentHeapUsage + 'MB , Load Avg.: '
  + rawObject.loadAvg + ' , Threads: ' + rawObject.threads
  + ' , Sockets: ' + rawObject.sockets + '"';
}

function refreshTheGrid(gridDiv) {
  setTimeout(function(){gridDiv.click();}, 300);
}

/*
 * Show/Hide panels in Widget for Cluster Member's and Data
 * Function to be called on click of visualization tab icons 
 */
// function openTabViewPanel(dataPerspective, viewOption, viewType)
function openTabViewPanel(thisitem){
     //alert(thisitem.id+" : "+thisitem.dataset.perspective +" | "+thisitem.dataset.view+" | "+thisitem.dataset.viewoption);
     var viewPanelBlock = thisitem.dataset.view+"_"+thisitem.dataset.viewoption+"_block";
     $('#'+viewPanelBlock).siblings().hide();
     $('#'+viewPanelBlock).show();
     $(thisitem).siblings().removeClass('active');
     $(thisitem).addClass('active');
}

//Start: Widget for Cluster Members and Data



//Show Members Default/Topology R-Graph View and hide rest
function showMembersDefaultRgraphPanel() {

  flagActiveTab = "MEM_R_GRAPH_DEF";
  updateRGraphFlags();

  // populateMemberRGraph using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembersRGraph = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberRGraphBack);

}

//Show Members Default/Topology Treemap View and hide rest
function showMembersDefaultTreemapPanel() {

  flagActiveTab = "MEM_TREE_MAP_DEF";

  // populate Member TreeMap using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembers = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberBack);


}

//Show Members Default/Topology Grid View and hide rest
function showMembersDefaultGridPanel() {

  flagActiveTab = "MEM_GRID_DEF";

  // populate Member Grid using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembers = ""; // getClusterMembersBack
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberBack);

  $('#default_grid_block').hide();
  destroyScrollPane('gview_memberList');
  $('#default_grid_block').show();
  
  /* Custom scroll */

  $('.ui-jqgrid-bdiv').each(function(index) {
    var tempName = $(this).parent().attr('id');
    if (tempName == 'gview_memberList') {
      $(this).jScrollPane({maintainPosition : true, stickToRight : true});  
    }
  });

}

//Show Members Server Groups Treemap View and hide rest
function showMembersSGTreemapPanel() {

  flagActiveTab = "MEM_TREE_MAP_SG";

  // populate Member TreeMap using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembers = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberBack);

}

//Show Members Server Groups Grid View and hide rest
function showMembersSGGridPanel() {

  flagActiveTab = "MEM_GRID_SG";

  // populate Member Grid using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembers = ""; // getClusterMembersBack
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberBack);

  $('#servergroups_grid_block').hide();
  destroyScrollPane('gview_memberListSG');
  $('#servergroups_grid_block').show();
  
  /* Custom scroll */

  $('.ui-jqgrid-bdiv').each(function(index) {
    var tempName = $(this).parent().attr('id');
    if (tempName == 'gview_memberListSG') {
      $(this).jScrollPane({maintainPosition : true, stickToRight : true});  
    }
  });

}

//Show Members Redundancy Zones Treemap View and hide rest
function showMembersRZTreemapPanel() {
  
  flagActiveTab = "MEM_TREE_MAP_RZ";

  // populate Member TreeMap using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembers = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberBack);
}

//Show Members Redundancy Zones Grid View and hide rest
function showMembersRZGridPanel() {

  flagActiveTab = "MEM_GRID_RZ";

  // populate Member Grid using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterMembers = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterMemberBack);

  $('#redundancyzones_grid_block').hide();
  destroyScrollPane('gview_memberListRZ');
  $('#redundancyzones_grid_block').show();
  
  /* Custom scroll */

  $('.ui-jqgrid-bdiv').each(function(index) {
    var tempName = $(this).parent().attr('id');
    if (tempName == 'gview_memberListRZ') {
      $(this).jScrollPane({maintainPosition : true, stickToRight : true});  
    }
  });

}

//Show Data Treemap View and hide rest
function showDataTreemapPanel() {
  
  flagActiveTab = "DATA_TREE_MAP_DEF";

  // populate Region TreeMap using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterRegions = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterRegionsBack);

}

//Show Data Grid View and hide rest
function showDataGridPanel() {

  flagActiveTab = "DATA_GRID_DEF";

  // populate Regions Grid using pulseUpdate
  var pulseData = new Object();
  pulseData.ClusterRegions = "";
  ajaxPost("pulse/pulseUpdate", pulseData, translateGetClusterRegionsBack);

  $('#data_grid_block').hide();
  destroyScrollPane('gview_regionsList');
  $('#data_grid_block').show();
  
  /* Custom scroll */

  $('.ui-jqgrid-bdiv').each(function(index) {
    var tempName = $(this).parent().attr('id');
    if (tempName == 'gview_regionsList') {
      $(this).jScrollPane({maintainPosition : true, stickToRight : true});  
    }
  });

}

// Function to be called on click of visualization tab icons
function openViewPanel(dataPerspective, dataView, dataViewOption){
  // Display Loading/Busy symbol
  $("#loadingSymbol").show();

  selectedPerspectiveView = dataPerspective;

  var viewPanelBlock = dataView+"_"+dataViewOption+"_block";
  var viewPanelButton = dataView+"_"+dataViewOption+"_button";
  // Display tab panel
  $('#'+viewPanelBlock).siblings().hide();
  $('#'+viewPanelBlock).show();
  
  // activate tab button
  var thisitem = $('#'+viewPanelButton);
  $(thisitem).siblings().removeClass('active');
  $(thisitem).addClass('active');
  
  if('member' == dataPerspective){
    selectedMemberViewOption = dataView;
    if('servergroups' == dataView){ // Server Groups view
      selectedViewTypeSG = dataViewOption;
      if('treemap' == dataViewOption){ // Treemap
        showMembersSGTreemapPanel();
      }else { // Grid
        showMembersSGGridPanel();
      }
    }else if('redundancyzones' == dataView){ // Redundancy Zones view
      selectedViewTypeRZ = dataViewOption;
      if('treemap' == dataViewOption){ // Treemap
        showMembersRZTreemapPanel();
      }else { // Grid
        showMembersRZGridPanel();
      }
    }else { // default view
      selectedViewTypeDefault = dataViewOption;
      if('treemap' == dataViewOption){ // Treemap
        showMembersDefaultTreemapPanel();
      }else if('grid' == dataViewOption){ // Grid
        showMembersDefaultGridPanel();
      }else{ // R-Graph
        showMembersDefaultRgraphPanel();
      }
    }

    // Show/hide hotspot dropdown
    if('treemap' == dataViewOption){
      $("#hotspotParentContainer").show();
    }else{
      $("#hotspotParentContainer").hide();
    }

  }else{ // 'data' == dataPerspective
    selectedViewTypeData = dataViewOption;
    if('treemap' == dataViewOption){ // Treemap
      showDataTreemapPanel();
    }else if('grid' == dataViewOption){ // Grid
      showDataGridPanel();
    }
  }
  // Hide Loading/Busy symbol
  $("#loadingSymbol").hide();
}

//Function to be called on click of cluster perspective drop down list
function onChangeClusterPerspective(n) {
  for(var i=1;i<=$(".members_data ul li").length;i++){
    $("#members_data"+i).removeClass("selected");
  }
  $("#members_data"+n).addClass("selected");

  if (n == 1) { // Members Perspective
    $("#member_view_options").show();
    $("#icons_data_view").hide();
    $("#icons_member_view_option_default").hide();
    $("#icons_member_view_option_servergroups").hide();
    $("#icons_member_view_option_redundancyzones").hide();

    // Check last selected member view option
    for ( var i = 0; i < $("#member_view_options ul li").length; i++) {
      var idname = $('#member_view_options ul li').get(i).id;
      if ($("#" + idname).children("label").hasClass("r_on")) {
        var iconsbox = "icons_"+idname;
        $("#" + iconsbox).show();
      }
    }

    selectedPerspectiveView = "member";

  } else if (n == 2) { // Data Perspective
	$("#hotspotParentContainer").hide();
    $("#member_view_options").hide();
    $("#icons_data_view").show();
    $("#icons_member_view_option_default").hide();
    $("#icons_member_view_option_servergroups").hide();
    $("#icons_member_view_option_redundancyzones").hide();

    selectedPerspectiveView = "data";
  }

  // display visualization
  if("member" == selectedPerspectiveView){
    // Member perspective
    if("default" == selectedMemberViewOption){
      if("rgraph" == selectedViewTypeDefault){
        openViewPanel('member', 'default','rgraph');
      }else if("treemap" == selectedViewTypeDefault){
        openViewPanel('member', 'default','treemap');
      }else{
        openViewPanel('member', 'default','grid');
      }
    }else if("servergroups" == selectedMemberViewOption){
      if("treemap" == selectedViewTypeSG){
        openViewPanel('member', 'servergroups','treemap');
      }else{
        openViewPanel('member', 'servergroups','grid');
      }
    }else{ // redundancyzones
      if("treemap" == selectedViewTypeRZ){
        openViewPanel('member', 'redundancyzones','treemap');
      }else{
        openViewPanel('member', 'redundancyzones','grid');
      }
    }
  }else{
    // Data perspective
    if("treemap" == selectedViewTypeData){
      openViewPanel('data', 'data', 'treemap');
    }else{
      openViewPanel('data', 'data', 'grid');
    }
  }

}

function displayClusterPerspective(perspView){
  
}

$(document).ready(function(e) {
  // Attach click event on outside of the drop down list control.
  addEventsForMembersOptions();

});

/* 
 Function to attach click event on outside of the drop down list control.
*/
function addEventsForMembersOptions() {

  // Attach click event for radio butons on Memebrs perspective view
  $(".label_radio input").unbind("click").click(memberOptionSelectionHandler);
  
}

// handler to be called on selection of radio options of Member Views
var memberOptionSelectionHandler = function(){

  $(".label_radio").removeClass("r_on");
  if ($(this).is(":checked")) {
    $(this).parent('label').addClass("r_on");
  } else {
    $(this).parent('label').removeClass("r_on");
  }
  
  var viewOptionId;
  if('radio-servergroups' == this.id){
    viewOptionId = 'member_view_option_servergroups';
    selectedMemberViewOption = "servergroups";
    if("treemap" == selectedViewTypeSG){
      openViewPanel('member', 'servergroups','treemap');
    }else{
      openViewPanel('member', 'servergroups','grid');
    }
  }else if('radio-redundancyzones' == this.id){
    viewOptionId = 'member_view_option_redundancyzones';
    selectedMemberViewOption = "redundancyzones";
    if("treemap" == selectedViewTypeRZ){
      openViewPanel('member', 'redundancyzones','treemap');
    }else{
      openViewPanel('member', 'redundancyzones','grid');
    }
  }else{//radio-default
    viewOptionId = 'member_view_option_default';
    selectedMemberViewOption = "default";
    if("rgraph" == selectedViewTypeDefault){
      openViewPanel('member', 'default','rgraph');
    }else if("treemap" == selectedViewTypeDefault){
      openViewPanel('member', 'default','treemap');
    }else{
      openViewPanel('member', 'default','grid');
    }
  }
  
  // set radio selection
  $('#'+viewOptionId).siblings().removeClass('selected');
  $('#'+viewOptionId).addClass("selected");
  //selectedsubview = id;
  // Show/Hide Tab buttons 
  $('#'+'icons_'+viewOptionId).show();
  $('#'+'icons_'+viewOptionId).siblings().hide();

};

var selectedsubview;
var viewtypegridtreemap;

/************ on Click Out Side **************************/
(function(jQuery) {
   jQuery.fn.clickoutside = function(callback) {
      var outside = 1, self = $(this);
      self.cb = callback;
      this.click(function() {
         outside = 0;
      });
      $(document).click(function() {
         outside && self.cb();
         outside = 1;
      });
      return $(this);
   };
})(jQuery);

// End: Widget for Cluster Members and Data
