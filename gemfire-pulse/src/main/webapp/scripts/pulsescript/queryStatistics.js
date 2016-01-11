/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * This JS File is used for Cluster Details screen
 * 
 */
var grid = null;
var gridFilterReset = true;

$(document).ready(function() {

  // Load Notification HTML  
  generateNotificationsPanel();

  // modify UI text as per requirement
  customizeUI();

  if (CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()) {
    // Do nothing
  } else {
    // Redirect to Cluster details page
    openClusterDetail();
  }
  scanPageForWidgets();

  // creating blank query statistics grid
  createQueryStatisticsGrid();

  // setup the options with check boxes and show hide functionality using
  // library
  setupColumnSelection();

  $.ajaxSetup({
    cache : false
  });
});

/*
 * Function to sort integer column which will show text NA
 */
var customSortFunction = function(cell, rowObject) {
  if (cell == 'NA') {
    return -1;
  } else {
    return parseInt(cell);
  }
};

function createQueryStatisticsGrid() {

  $.ajax({
    url : 'pulse/getQueryStatisticsGridModel',
    type : 'GET',
    dataType : 'json',
    async : false,
    success : function(data) {

      // set cluster name in tab
      $('#clusterName').html(data.clusterName);
      $('#userName').html(data.userName);

      // add column selection options
      for ( var i = 1; i < data.columnNames.length; ++i) {
        $('#columnsSelect').append(
            '<option value = "' + data.columnModels[i].name + '">'
                + data.columnNames[i] + '</option>');
      }

      // one-time setup for column sorting for integer columns here
      for ( var i = 0; i < data.columnModels.length; ++i) {
        if (data.columnModels[i].sorttype == 'integer') {
          data.columnModels[i].sorttype = customSortFunction;
        }
      }

      grid = $("#queryStatisticsList");
      grid.jqGrid({
        datatype : "local",
        height : 710,
        width : 1000,
        rowNum : 50,
        sortname : data.columnModels[1].name,
        sortorder: "desc",
        shrinkToFit : false,
        ignoreCase: true,
        colNames : data.columnNames,
        colModel : data.columnModels,
        userData : {
          "sortOrder" : "desc",
          "sortColName" : data.columnModels[1].name
        },
        onSortCol : function(columnName, columnIndex, sortorder) {
          // Set sort order and sort column in user variables so that
          // periodical updates can maintain the same
          var gridUserData = jQuery("#queryStatisticsList").getGridParam(
              'userData');
          gridUserData.sortColName = columnName;
          gridUserData.sortOrder = sortorder;
          
          jQuery("#queryStatisticsList").trigger("reloadGrid");
          var queryStatisticsList = $('#gview_queryStatisticsList');
          var queryStatisticsListChild = queryStatisticsList
              .children('.ui-jqgrid-bdiv');
          var api = queryStatisticsListChild.data('jsp');
          api.reinitialise();

          $('#queryStatisticsList').toggle();
          refreshTheGrid($('#queryStatisticsList'));
        },
        resizeStop : function(width, index) {
          var queryStatisticsList = $('#gview_queryStatisticsList');
          var queryStatisticsListChild = queryStatisticsList
              .children('.ui-jqgrid-bdiv');
          var api = queryStatisticsListChild.data('jsp');
          api.reinitialise();

          $('#queryStatisticsList').toggle();
          refreshTheGrid($('#queryStatisticsList'));
        },
        gridComplete : function() {
          $(".jqgrow").css({
            cursor : 'default'
          });
          
          // wrap contents of query statistics table cells only - not other grids
          $(".jqgrow td").css('word-wrap', 'break-word');
          $(".jqgrow td").css('white-space', 'pre-wrap');
          $(".jqgrow td").css('white-space', '-pre-wrap');
          $(".jqgrow td").css('white-space', '-o-pre-wrap');
          $(".jqgrow td").css('white-space', 'normal !important');
          $(".jqgrow td").css('height', 'auto');
          $(".jqgrow td").css('vertical-align', 'text-top');
          $(".jqgrow td").css('padding-top', '2px');
          $(".jqgrow td").css('padding-bottom', '3px');

          var queryStatisticsList = $('#gview_queryStatisticsList');
          var queryStatisticsListChild = queryStatisticsList
              .children('.ui-jqgrid-bdiv');

          queryStatisticsListChild.unbind('jsp-scroll-x');
          queryStatisticsListChild.bind('jsp-scroll-x', function(event,
              scrollPositionX, isAtLeft, isAtRight) {
            var qsList = $('#gview_queryStatisticsList');
            var qsLC = qsList.children('.ui-jqgrid-hdiv').children(
                '.ui-jqgrid-hbox');
            qsLC.css("position", "relative");
            qsLC.css('right', scrollPositionX);
          });
        }
      });

      $('.ui-jqgrid-bdiv').each(function(index) {
        var tempName = $(this).parent().attr('id');
        if (tempName == 'gview_queryStatisticsList') {
          $(this).jScrollPane();
        }
      });
    }
  });
}

function setupColumnSelection() {

  $("select").multiselect(
      {
        click : function(event, ui) {
          // hide selected column
          if (ui.checked) {
            grid.jqGrid('hideCol', ui.value);
          } else {
            grid.jqGrid('showCol', ui.value);
          }

          grid.trigger("reloadGrid", [ {
            page : 1
          } ]);
          destroyScrollPane('gview_queryStatisticsList');
          $('.ui-jqgrid-bdiv').each(function(index) {
            var tempName = $(this).parent().attr('id');
            if (tempName == 'gview_queryStatisticsList') {
              $(this).jScrollPane();
            }
          });
        },
        checkAll : function() {
          // hide all columns
          var colModels = grid.jqGrid('getGridParam', 'colModel');
          for ( var i = 1; i < colModels.length; i++) {
            grid.jqGrid('hideCol', colModels[i].name);
          }

          grid.trigger("reloadGrid", [ {
            page : 1
          } ]);
          destroyScrollPane('gview_queryStatisticsList');
          $('.ui-jqgrid-bdiv').each(function(index) {
            var tempName = $(this).parent().attr('id');
            if (tempName == 'gview_queryStatisticsList') {
              $(this).jScrollPane();
            }
          });  
        },
        uncheckAll : function() {
          // show all columns
          var colModels = grid.jqGrid('getGridParam', 'colModel');
          for ( var i = 1; i < colModels.length; i++) {
            grid.jqGrid('showCol', colModels[i].name);
          }
          grid.trigger("reloadGrid", [ {
            page : 1
          } ]);

          destroyScrollPane('gview_queryStatisticsList');
          $('.ui-jqgrid-bdiv').each(function(index) {
            var tempName = $(this).parent().attr('id');
            if (tempName == 'gview_queryStatisticsList') {
              $(this).jScrollPane();
            }
          });
        }
      });
}

var applyFilterOnQueryStatistics = function() {
  
  var searchKeyword = $('#filterQueryStatisticsBox').val();
  if(searchKeyword.length < 4){
    
    if(! gridFilterReset){
      gridFilterReset = true; // do not filter grid till reset

      ///filter only after string length 4 else reset filter
      grid[0].p.search = false;
      grid.trigger("reloadGrid", [ {
        page : 1
      } ]);
     
      // trigger check for scroll pane to see if scroll bars need to be shown
      var queryStatisticsList = $('#gview_queryStatisticsList');
      var queryStatisticsListChild = queryStatisticsList
          .children('.ui-jqgrid-bdiv');
      var api = queryStatisticsListChild.data('jsp');
      api.reinitialise();
  
      // scroll to top of grid to ensure all records are displayed in the view port
      api.scrollToY(0, false);
  
      $('#queryStatisticsList').toggle();
      refreshTheGrid($('#queryStatisticsList'));
    }
    
    return;
  }
  
  gridFilterReset = false; // set to start filtering grid
  if ((searchKeyword != "Search") && (searchKeyword != "")) {
    var myfilter = {
      groupOp : "AND",
      rules : [ {
        field : "Query",
        op : "cn",
        data : searchKeyword
      } ]
    };

    grid[0].p.search = myfilter.rules.length > 0;
    $.extend(grid[0].p.postData, {
      filters : JSON.stringify(myfilter)
    });
  } else {
    grid[0].p.search = false;
  }

  grid.trigger("reloadGrid", [ {
    page : 1
  } ]);
 
  // trigger check for scroll pane to see if scroll bars need to be shown
  var queryStatisticsList = $('#gview_queryStatisticsList');
  var queryStatisticsListChild = queryStatisticsList
      .children('.ui-jqgrid-bdiv');
  var api = queryStatisticsListChild.data('jsp');
  api.reinitialise();

  // scroll to top of grid to ensure all records are displayed in the view port
  api.scrollToY(0, false);

  $('#queryStatisticsList').toggle();
  refreshTheGrid($('#queryStatisticsList'));
};

function showQueryStatistics(data) {
  getQueryStatisticsBack(data.QueryStatistics);
}

function refreshTheGrid(gridDiv) {
  setTimeout(function(){gridDiv.toggle();}, 500);
}
