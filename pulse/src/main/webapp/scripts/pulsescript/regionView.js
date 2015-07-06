/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/* Region Details javascript with pop-up with spark-lines opening from each tree-map cell showing member level region information.
 * This page opens from Data view on cluster page and links to member details page by clicking on tree map cell.
 *
 * @author Riya Bhandekar
 * @since version 7.5 Cedar 2014-03-01
 *
 */


// required global variables
var regionMemberTreeMap;
var tipObj = null;
var theTooltip = null;
var memberList = null;
var memberOnRegionJson = null;
var clusterDataViewRegions;
var regionMembers = null;

/**
 * This JS File is used for Cluster Details screen
 * 
 */
$(document).ready(function() {

  // Load Notification HTML  
  generateNotificationsPanel();

  if (CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()) {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_SQLFIRE);
  } else {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_GEMFIRE);
  }

  createMemberTreeMap();
  
  createMemberGrid();
  
	// scan page for widgets
  scanPageForWidgets();

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
    // Show HTML for following
    $('#subTabQueryStatistics').show();
  }else{
    // Hide HTML for following
    $('#subTabQueryStatistics').hide();
  }  
}

//function used for applying filter of member names in data view screen
var applyFilterOnMembersList = function() {
  var searchKeyword = extractFilterTextFrom('filterMembersListBox');
  var htmlMemberListWithFilter = '';
  if (searchKeyword != "") {
    for ( var i = 0; i < memberList.length; i++) {
      // filtered list
      if (memberList[i].name.toLowerCase().indexOf(searchKeyword) !== -1) {
        var divId = memberList[i].memberId + "&" + memberList[i].name;
        htmlMemberListWithFilter += "<div class='pointDetailsPadding' title='"
            + memberList[i].name + "' id='" + divId
            + "' onClick = 'javascript:openMemberDetails(this.id)'>"
            + memberList[i].name + "</div>";
      }
    }
  } else {
    for ( var i = 0; i < memberList.length; i++) {
      // non filtered list
      var divId = memberList[i].memberId + "&" + memberList[i].name;
      htmlMemberListWithFilter += "<div class='pointDetailsPadding' title='"
          + memberList[i].name + "' id='" + divId
          + "' onClick = 'javascript:openMemberDetails(this.id)'>"
          + memberList[i].name + "</div>";
    }
  }
  document.getElementById("memberNames").innerHTML = htmlMemberListWithFilter;
  $('.regionMembersSearchBlock').jScrollPane();
};


//function used for applying filter of region's members treemap and grid
var applyFilterOnRegionMembers = function() {
  //console.log("applyFilterOnRegionMembers called");
  var searchKeyword = extractFilterTextFrom("filterRegionMembersBox");

  if (searchKeyword != "") {
    var filteredRegionMembers = {};
    for (var regMemKey in regionMembers) {
      var regionMember = regionMembers[regMemKey];
      // filtered list
      if (regionMember.memberName.toLowerCase().indexOf(searchKeyword) !== -1) {
        filteredRegionMembers[regionMember.memberName] = regionMember;
      }
    }
    updateClusterSelectedRegionMembers(filteredRegionMembers);
  } else {
    updateClusterSelectedRegionMembers(regionMembers);
  }
};


function initializeSparklinesToZero(spo){
  var arrZero = [0,0,0,0,0,0,0];
  tipObj.find('#memberMemoryReadsTrend').sparkline(arrZero,spo);
  tipObj.find('#memberDiskReadsTrend').sparkline(arrZero,spo);
  spo.lineColor = '#2e84bb';
  tipObj.find('#memberMemoryWritesTrend').sparkline(arrZero,spo);
  tipObj.find('#memberDiskWritesTrend').sparkline(arrZero,spo);
}


function displaySelectedRegionDetails(regionName, data){
  
  $('#regionPath').html(data.selectedRegion.path);
  $('#regionType').html(data.selectedRegion.type);
  $('#regionMembersText').html(data.selectedRegion.memberCount);
  $('#regionNameText').html(data.selectedRegion.name);
  $('#regionEntryCountText').html(data.selectedRegion.entryCount);
  $('#regionEntrySizeText').html(data.selectedRegion.entrySize);
  
  $('#regionPersistence').html(data.selectedRegion.persistence);
  $('#regionDiskUsage').html(data.selectedRegion.dataUsage);
  //$('#regionIsEnableOffHeapMemory').html(data.selectedRegion.isEnableOffHeapMemory);
  //$('#regionIsHdfsWriteOnly').html(data.selectedRegion.isHDFSWriteOnly);
  $('#regionEmptyNodes').html(data.selectedRegion.emptyNodes);
  
  //memberList = data.selectedRegion.members;
  //applyFilterOnMembersList();
  
  var memoryUsagePer = (data.selectedRegion.memoryUsage / data.selectedRegion.totalMemory) * 100;
  memoryUsagePer = isNaN(memoryUsagePer) ? 0 : memoryUsagePer;
  var memPer = memoryUsagePer + "%";
  document.getElementById("memoryUsage").style.width = memPer;

  memoryUsagePer = parseFloat(memoryUsagePer);
  $('#memoryUsageVal').html(memoryUsagePer.toFixed(4));

  if (data.selectedRegion.memoryUsage == 0) {
    document.getElementById("memoryUsed").innerHTML = "-";
    document.getElementById("memoryUsedMBSpan").innerHTML = "";
  } else
    document.getElementById("memoryUsed").innerHTML = data.selectedRegion.memoryUsage;
  document.getElementById("totalMemory").innerHTML = data.selectedRegion.totalMemory;

  //theTooltip
  if(theTooltip != null){
    if(theTooltip.isShown(false)){
      theTooltip.hide();
      $('#region_tooltip').css('display','none');
    }
  }
  
  // delete tooltips here
  $('#region_tooltip').remove();
  
  var sparklineOptions = {
      width : '217px',
      height : '72px',
      lineColor : '#FAB948',
      fillColor : false,
      spotRadius : 2.5,
      labelPosition : 'left',
      spotColor : false,
      minSpotColor : false,
      maxSpotColor : false,
      lineWidth : 2
  };
  
  var reads = data.selectedRegion.memoryReadsTrend;
  var diskReads = data.selectedRegion.diskReadsTrend;
  var writes = data.selectedRegion.memoryWritesTrend;
  var diskWrites = data.selectedRegion.diskWritesTrend;
  
  // Reads trends  
  $('#readsPerSecTrend').sparkline(reads, sparklineOptions);
  $('#diskReadsPerSecTrend').sparkline(diskReads, sparklineOptions);
  
  // Writes trends
  sparklineOptions.lineColor = '#2e84bb';
  $('#writesPerSecTrend').sparkline(writes, sparklineOptions);
  $('#diskWritesPerSecTrend').sparkline(diskWrites, sparklineOptions);

  var sumReads = 0;
  var avgReads = 0;
  if (reads.length > 0) {
    for ( var i = 0; i < reads.length; i++) {
      sumReads += parseFloat(reads[i]);
    }
    avgReads = sumReads / reads.length;
  }
  $('#currentReadsPerSec').html(applyNotApplicableCheck(avgReads.toFixed(2)));  

  var sumDiskReads = 0;
  var avgDiskReads = 0;
  if (diskReads.length > 0) {
    for ( var i = 0; i < diskReads.length; i++) {
      sumDiskReads += parseFloat(diskReads[i]);
    }
    avgDiskReads = sumDiskReads / diskReads.length;
  }
  $('#currentDiskReadsPerSec').html(
      applyNotApplicableCheck(avgDiskReads.toFixed(2)));
  
  var sumWrites = 0;
  var avgWrites = 0;
  if (writes.length > 0) {
    for ( var i = 0; i < writes.length; i++) {
      sumWrites += parseFloat(writes[i]);
    }
    avgWrites = sumWrites / writes.length;
  }
  $('#currentWritesPerSec').html(applyNotApplicableCheck(avgWrites.toFixed(2)));

  
  var sumDiskWrites = 0;
  var avgDiskWrites = 0;
  if (diskWrites.length > 0) {
    for ( var i = 0; i < diskWrites.length; i++) {
      sumDiskWrites += parseFloat(diskWrites[i]);
    }
    avgDiskWrites = sumDiskWrites / diskWrites.length;
  }
  $('#currentDiskWritesPerSec').html(
      applyNotApplicableCheck(avgDiskWrites.toFixed(2)));
  
  var popupDiv = $('#popupDiv').html();
  $(".node").attr('title',popupDiv);
  $(".node").tooltip({
    
    onBeforeShow: function() {
      if(theTooltip != null){
        if(theTooltip.isShown(false)){
          theTooltip.hide();
        }
      }
    },
    onShow: function() {
      tipObj = this.getTip(); //global store
      theTooltip = this;

      var nodeId = this.getTrigger().attr('id');
      var memberName = nodeId;
      var spo = {
          width : '110px',
          height : '50px',
          lineColor : '#FAB948',
          fillColor : '#0F1C25',
          spotRadius : 2.5,
          labelPosition : 'left',
          spotColor : false,
          minSpotColor : false,
          maxSpotColor : false,
          lineWidth : 2
      };
      
      initializeSparklinesToZero(spo); 
      spo.lineColor='#FAB948';
      $.sparkline_display_visible();
    
      //region member specific statistics 
      tipObj.find('#idMemberName').html(memberName);
      
      var key = 'memberOnRegionJson.' + memberName + '.entryCount';
      tipObj.find('#regionMemberEntryCount').html(eval(key));
      key = 'memberOnRegionJson.' + memberName + '.entrySize';
      tipObj.find('#regionMemberEntrySize').html(eval(key));
      key = 'memberOnRegionJson.' + memberName + '.accessor';
      //tipObj.find('#regionMemberAccessor').html('False');
      tipObj.find('#regionMemberAccessor').html(eval(key));
      
      key = 'memberOnRegionJson.' + memberName + '.memoryReadsTrend';
      tipObj.find('#memberMemoryReadsTrend').sparkline(eval(key),spo);
      var reads = eval(key); // store
      key = 'memberOnRegionJson.' + memberName + '.diskReadsTrend';
      tipObj.find('#memberDiskReadsTrend').sparkline(eval(key),spo);
      var diskReads = eval(key); // store
      
      // Writes trends
      spo.lineColor = '#2e84bb';
      key = 'memberOnRegionJson.' + memberName + '.memoryWritesTrend';
      tipObj.find('#memberMemoryWritesTrend').sparkline(eval(key),spo);
      var writes = eval(key); // store
      key = 'memberOnRegionJson.' + memberName + '.diskWritesTrend';
      tipObj.find('#memberDiskWritesTrend').sparkline(eval(key),spo);
      var diskWrites = eval(key); // store
            
      $.sparkline_display_visible();
      
      var sumReads = 0;
      var avgReads = 0;
      if (reads.length > 0) {
        for ( var i = 0; i < reads.length; i++) {
          sumReads += parseFloat(reads[i]);
        }
        avgReads = sumReads / reads.length;
      }
      $('#memberMemoryReadsThroughput').html(applyNotApplicableCheck(avgReads.toFixed(2)));  

      var sumDiskReads = 0;
      var avgDiskReads = 0;
      if (diskReads.length > 0) {
        for ( var i = 0; i < diskReads.length; i++) {
          sumDiskReads += parseFloat(diskReads[i]);
        }
        avgDiskReads = sumDiskReads / diskReads.length;
      }
      $('#memberDiskReadsThroughput').html(
          applyNotApplicableCheck(avgDiskReads.toFixed(2)));
      
      var sumWrites = 0;
      var avgWrites = 0;
      if (writes.length > 0) {
        for ( var i = 0; i < writes.length; i++) {
          sumWrites += parseFloat(writes[i]);
        }
        avgWrites = sumWrites / writes.length;
      }
      $('#memberMemoryWritesThroughput').html(applyNotApplicableCheck(avgWrites.toFixed(2)));

      
      var sumDiskWrites = 0;
      var avgDiskWrites = 0;
      if (diskWrites.length > 0) {
        for ( var i = 0; i < diskWrites.length; i++) {
          sumDiskWrites += parseFloat(diskWrites[i]);
        }
        avgDiskWrites = sumDiskWrites / diskWrites.length;
      }
      $('#memberDiskWritesThroughput').html(applyNotApplicableCheck(avgDiskWrites.toFixed(2)));
    },
    onHide: function(){
      $('#region_tooltip').css('display','none');
      tipObj = null; // reset

      theTooltip = null;
    },
     // custom positioning
    position: 'bottom right',
    
    relative: false,
      
    // move tooltip a little bit to the right
    offset: [-350, 10]});
}

function updateSelectedRegionMembersGrid(regMembers){

  // loads grid
  $('#memberList').jqGrid('clearGridData');

  // Add memebers in grid
  for (var key in regMembers){
    var regMemData = regMembers[key];
    $('#memberList').jqGrid('addRowData',
        regMemData.memberId + "&" + regMemData.memberName, regMemData);
  }

  var gridUserData = jQuery("#memberList").getGridParam('userData');

  // Apply sort order ans sort columns on updated jqgrid data
  jQuery("#memberList").jqGrid('setGridParam', {
    sortname : gridUserData.sortColName,
    sortorder : gridUserData.sortOrder
  });
  // Reload jqgrid
  jQuery("#memberList").trigger("reloadGrid");

  // apply scroll if grid container block is not minimized
  if ($("#LargeBlock_1").css("display") != "none") {
    $('.ui-jqgrid-bdiv').each(function(index) {
      var tempName = $(this).parent().attr('id');
      if (tempName == 'gview_memberList') {
        $(this).jScrollPane({maintainPosition : true, stickToRight : true});
      }
    });

    var memberRegionsList = $('#gview_memberList');
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
  }
}

//function used for creating blank TreeMap for member's on Cluster Details
//Screen
function createMemberTreeMap() {
    
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
    
    regionMemberTreeMap = new $jit.TM.Squarified(
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

                regionMemberTreeMap.fx.plotNode(node,
                    regionMemberTreeMap.canvas);
                regionMemberTreeMap.labels.plotLabel(
                    regionMemberTreeMap.canvas, node);
              }
            },
            onMouseLeave : function(node) {
              if (node) {
                node.removeData('border', '#ffffff');
                node.removeCanvasStyle('shadowBlur');
                regionMemberTreeMap.plot();
              }
            },
            onClick : function(node) {
              if (node.id != "root") {
                location.href ='MemberDetails.html?member=' + node.id + "&memberName=" + node.name;
              }
            }
          },

          Tips : {
            enable : false,
            offsetX : 5,
            offsetY : 5,
            onShow : function(tip, node, isLeaf, domElement) {
              var html = "<div class=\"tip-title\"><div><div class='popupHeading'>This region does not exist.</div>";
              tip.innerHTML = html;
            }
          },
          onCreateLabel : function(domElement, node) {
            domElement.style.opacity = 0.01;

          }
        });
    regionMemberTreeMap.loadJSON(json);
    regionMemberTreeMap.refresh();
    $(".node").attr('title',"<div class=\"tip-title\"><div><div class='popupHeading'>This region does not exist.</div></div></div>");
    $(".node").tooltip({
      
      data : $(this).tagName,
      onShow: function() {
        tipObj = this.getTip(); //global store 
      },
      onHide: function(){
        tipObj = null; // reset
      },
       // custom positioning
      position: 'bottom right',
      
      relative: false,
        
      // move tooltip a little bit to the right
      offset: [-350, 10]});
}

/* builds and returns json from given members details sent by server */
function buildMemberTreeMap(members) {

  var childerensVal = [];
  var indexOfChild = 0;
  for(var memKey in members){

    var regMember = members[memKey];

    var color = "#a0c44a";
    // setting node color according to the status of member
    // like if member has severe notification then the node color will be
    // '#ebbf0f'
    for ( var j = 0; j < warningAlerts.length; j++) {
      if (regMember.memberName == warningAlerts[j].memberName) {
        color = '#ebbf0f';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#de5a25'
    for ( var j = 0; j < errorAlerts.length; j++) {
      if (regMember.memberName == errorAlerts[j].memberName) {
        color = '#de5a25';
        break;
      }
    }
    // if member has severe notification then the node color will be
    // '#b82811'
    for ( var j = 0; j < severAlerts.length; j++) {
      if (regMember.memberName == severAlerts[j].memberName) {
        color = '#b82811';
        break;
      }
    }
    
    var areaSize = regMember.entryCount;
    if (areaSize == 0)
      areaSize = 1;
    var name = "";
    name = regMember.memberName;
    var id = "";
    id = regMember.memberName;
    // passing all the required information of member to tooltip
    var dataVal = {
      "name" : name,
      "id" : id,
      "$color" : color,
      "$area" : areaSize,
      "entryCount" : regMember.entryCount,
      "entrySize" : regMember.entrySize,
      "accessor" : regMember.accessor,
      "initial" : false
    };
    var childrenVal = {
      "children" : [],
      "data" : dataVal,
      "id" : id,
      "name" : name
    };
    childerensVal[indexOfChild++] = childrenVal;
  }
  
  var localjson = {
    "children" : childerensVal,
    "data" : {},
    "id" : "root",
    "name" : "Members"
  };
  return localjson;
}

function tabClusterGrid() {
  flagActiveTab = "MEM_GRID"; // MEM_TREE_MAP, MEM_GRID
  
  $('#btngridIcon').addClass('gridIconActive');
  $('#btngridIcon').removeClass('gridIcon');

  $('#btnchartIcon').addClass('chartIcon');
  $('#btnchartIcon').removeClass('chartIconActive');

  $('#chartBlocks_Panel').hide();
  destroyScrollPane('gview_memberList');
  $('.ui-jqgrid-bdiv').each(function(index) {
      var tempName = $(this).parent().attr('id');
      if (tempName == 'gview_memberList') {
        $(this).jScrollPane({maintainPosition : true, stickToRight : true});
      }
  });
  $('#gridBlocks_Panel').show();
  jQuery("#memberList").trigger("reloadGrid");
}

function tabTreeMap() {
  flagActiveTab = "MEM_TREE_MAP"; // MEM_TREE_MAP, MEM_GRID

  $('#gridBlocks_Panel').hide();
  $('#chartBlocks_Panel').show();

  $('#btngridIcon').addClass('gridIcon');
  $('#btngridIcon').removeClass('gridIconActive');

  $('#btnchartIcon').addClass('chartIconActive');
  $('#btnchartIcon').removeClass('chartIcon');

  regionMemberTreeMap.loadJSON(globalJson);
  regionMemberTreeMap.refresh();
}

//function used for creating blank grids for member list for Cluster Details
//Screen
function createMemberGrid() {
jQuery("#memberList").jqGrid(
 {
   datatype : "local",
   height : 560,
   width : 410,
   rowNum : 200,
   shrinkToFit : false,
   colNames : [ 'Member', 'Entry Count', 'Entry Size', 'Accessor'],
   colModel : [
       {
         name : 'memberName',
         index : 'memberName',
         width : 120,
         cellattr : function(rowId, val, rawObject, cm, rdata) {
           return 'title="Member: ' + rawObject.memberName + ' , Entry Count: '
               + rawObject.entryCount + ' , Entry Size: '
               + rawObject.entrySize + ' , Accessor: '
               + rawObject.accessor + '"';
         },
         sortable : true,
         sorttype : "string"
       },
       {
         name : 'entryCount',
         index : 'entryCount',
         width : 90,
         align : 'right',
         cellattr : function(rowId, val, rawObject, cm, rdata) {
           return 'title="Member: ' + rawObject.memberName + ' , Entry Count: '
           + rawObject.entryCount + ' , Entry Size: '
           + rawObject.entrySize + ' , Accessor: '
           + rawObject.accessor + '"';
         },
         sortable : true,
         sorttype : "string"
       },
       {
         name : 'entrySize',
         index : 'entrySize',
         width : 90,
         align : 'right',
         cellattr : function(rowId, val, rawObject, cm, rdata) {
           return 'title="Member: ' + rawObject.memberName + ' , Entry Count: '
           + rawObject.entryCount + ' , Entry Size: '
           + rawObject.entrySize + ' , Accessor: '
           + rawObject.accessor + '"';
         },
         sortable : true,
         sorttype : "float"
       },
       {
         name : 'accessor',
         index : 'accessor',
         width : 90,
         align : 'center',
         cellattr : function(rowId, val, rawObject, cm, rdata) {
           return 'title="Member: ' + rawObject.memberName + ' , Entry Count: '
           + rawObject.entryCount + ' , Entry Size: '
           + rawObject.entrySize + ' , Accessor: '
           + rawObject.accessor + '"';
         },
         sortable : true,
         sorttype : "boolean"
       } ],
   userData : {
     "sortOrder" : "desc",
     "sortColName" : "entryCount"
   },
   onSortCol : function(columnName, columnIndex, sortorder) {
     // Set sort order and sort column in user variables so that
     // periodical updates can maintain the same
     var gridUserData = jQuery("#memberList").getGridParam('userData');
     gridUserData.sortColName = columnName;
     gridUserData.sortOrder = sortorder;
   },
   onSelectRow : function(rowid) {
     var member = rowid.split("&");
     location.href = 'MemberDetails.html?member=' + member[0]
     + '&memberName=' + member[1];
   },
   resizeStop : function(width, index) {
     
     var memberRegionsList = $('#gview_memberList');
     var memberRegionsListChild = memberRegionsList
         .children('.ui-jqgrid-bdiv');
     var api = memberRegionsListChild.data('jsp');
     api.reinitialise();
     
     memberRegionsList = $('#gview_memberList');
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
     
     this.gridComplete();
     $('#btngridIcon').click();
     refreshTheGrid($('#btngridIcon'));
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

function refreshTheGrid(gridDiv) {
  setTimeout(function(){gridDiv.click();}, 300);
}
