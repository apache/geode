var isThisGoodResolution;
var oldSelectedTab;
/* flagActiveTab valid values 
 * MEM_R_GRAPH_DEF, 
 * MEM_TREE_MAP_DEF, MEM_TREE_MAP_SG, MEM_TREE_MAP_RZ, DATA_TREE_MAP_DEF, 
 * MEM_GRID_DEF, MEM_GRID_SG, MEM_GRID_RZ, DATA_GRID_DEF
 */
var flagActiveTab = "";

var currentActiveNotificationTab = "ALL"; // valid values 'ALL', 'SEVERE', 'ERROR', 'WARNING'
var currentActivePerspective = "MEMBER"; // valid values 'MEMBER', 'DATA'
var hotspotAttributes = new Array(); // For Hotspot
var currentHotspotAttribiute = null; // For Hotspot

function checkMedia()

{
	$('.scroll-pane').jScrollPane();/*
																	 * custom scroll bar on body load for 1st tab
																	 */
	$('.scroll-pane_1').jScrollPane();
	$('.pointGridData').jScrollPane();
	$('.regionMembersSearchBlock').jScrollPane();

	$('.ui-jqgrid-bdiv').jScrollPane();

	if (document.getElementById('canvasWidth') != null) {
		var winW, winH;
		if (document.body && document.body.offsetWidth) {
			winW = document.body.offsetWidth;
			winH = document.body.offsetHeight;

		}
		if (document.compatMode == 'CSS1Compat' && document.documentElement
		    && document.documentElement.offsetWidth) {
			winW = document.documentElement.offsetWidth;
			winH = document.documentElement.offsetHeight;
		}
		if (window.innerWidth && window.innerHeight) {
			winW = window.innerWidth;
			winH = window.innerHeight;
		}

		document.getElementById('canvasWidth').style.width = "1258px";
		// alert('Weight: ' + winW );
		// alert('Height: ' + winH );
		if (winW <= 1024) {

			document.getElementById('canvasWidth').style.width = "1002px";
			// document.getElementById("overLapLinkBlock").style.display =
			// 'none';
			/* onload hide first top tab block */
			$('#TopTab_All').hide();
			$('#btnTopTab_All').removeClass('TopTabLinkActive');
			isThisGoodResolution = false;
		} else {

			document.getElementById('canvasWidth').style.width = "1258px";
			isThisGoodResolution = true;
			// document.getElementById("overLapLinkBlock").style.display =
			// 'block';
		}
	}

}

/* Version Details Popup handler */
$(document).ready(function() {

  // popHandler handles the display of pop up window on click event on link
  function popupHandler(e) {
    // Cancel the link behavior
    e.preventDefault();

    // Get the A tag
    var id = $(this).attr('href');

    // Get the screen height and width
    var maskHeight = $(document).height();
    var maskWidth = $(window).width();

    // Set height and width to mask to fill up the whole screen
    $('#mask').css({
      'width' : maskWidth,
      'height' : maskHeight
    });

    // transition effect
    $('#mask').fadeIn(1000);
    $('#mask').fadeTo("fast", 0.8);

    // Get the window height and width
    var winH = $(window).height();
    var winW = $(window).width();

    // Set the popup window to center
    $(id).css('top', winH / 2 - $(id).height() / 2);
    $(id).css('left', winW / 2 - $(id).width() / 2);

    // transition effect
    $(id).fadeIn(1500);

  };    // end of popupHandler

  // Add popupHandler on click of version details link   
  $('[id=pulseVersionDetailsLink]').click(popupHandler);

  // if close button is clicked
  $('.window .closePopup').click(function(e) {
    // Cancel the link behavior
    e.preventDefault();

    $('#mask').hide();
    $('.window').hide();
  });
  // if input close button is clicked
  $('.window .closePopupInputButton').click(function(e) {
    // Cancel the link behavior
    e.preventDefault();

    $('#mask').hide();
    $('.window').hide();
  });

  // if mask is clicked
  $('#mask').click(function() {
    $(this).hide();
    $('.window').hide();
  });

});

/* About Dropdown */
$(document).ready(function() {
	$(".aboutClicked-Off").click(function(e) {
		e.preventDefault();
		$("div#detailsAbout").toggle();
		$(".aboutClicked-Off").toggleClass("aboutClicked-On");
	});

	$("div#detailsAbout").mouseup(function() {
		return false;
	});
	$(document).mouseup(function(e) {
		if ($(e.target).parent("a.aboutClicked-Off").length == 0) {
			$(".aboutClicked-Off").removeClass("aboutClicked-On");
			$("div#detailsAbout").hide();
		}
	});

});
/* Members name Dropdown */
$(document).ready(function() {
	$(".memberClicked-Off").click(function(e) {
		e.preventDefault();
		$("div#setting").toggle();
		$(".memberClicked-Off").toggleClass("memberClicked-On");
		$('.jsonSuggestScrollFilter').jScrollPane();
	});

	$("div#setting").mouseup(function() {
		return false;
	});
	$(document).mouseup(function(e) {
		if ($(e.target).parent("a.memberClicked-Off").length == 0) {
			$(".memberClicked-Off").removeClass("memberClicked-On");
			$("div#setting").hide();
		}
	});

	/* on off switch */
	$('#membersButton').addClass('switchActive');
	$('#switchLinks').show();

	$("#membersButton").click(function(e) {
		$('#membersButton').addClass('switchActive');
		$('#dataButton').removeClass('switchActive');
		$('#switchLinks').show();
	});

	$("#dataButton").click(function(e) {
		$('#membersButton').removeClass('switchActive');
		$('#dataButton').addClass('switchActive');
		$('#switchLinks').hide();
	});

});
/* show block function */
function showDiv(divSelected) {
	$('#' + divSelected).show();
}

/* hide block function */
function hideDiv(divSelected) {
	$('#' + divSelected).hide();
}
/* Toggle Top Tab */
function toggleTab(divSelected) {
	/*
	 * $(document).mouseup(function(e) { $('#'+divSelected).hide(); });
	 */

	if (!isThisGoodResolution) {
		$('#' + divSelected).toggle();
		$('.scroll-pane').jScrollPane();
		if (oldSelectedTab == divSelected) {
			$('#' + 'btn' + oldSelectedTab).removeClass('TopTabLinkActive');
			oldSelectedTab = "";

		} else {
			oldSelectedTab = divSelected;
		}

	}

}

/* toggle block function */
function toggleDiv(divSelected) {
	$('#' + divSelected).toggle();
	if ($('#' + 'btn' + divSelected).hasClass('minusIcon')) {
		$('#' + 'btn' + divSelected).addClass('plusIcon');
		$('#' + 'btn' + divSelected).removeClass('minusIcon');
	} else {
		$('#' + 'btn' + divSelected).addClass('minusIcon');
		$('#' + 'btn' + divSelected).removeClass('plusIcon');
	}

}

/*---Accordion-----*/
function accordion() {

	$('.accordion .heading').prepend('<span class="spriteArrow"></span>');
	$('.accordion .heading').click(
	    function() {
		    var accordionId = $(this).parent().parent().attr('id');
		    accordionId = ('#' + accordionId);

		    if ($(this).is('.inactive')) {
			    $(accordionId + ' .active').toggleClass('active').toggleClass(
			        'inactive').next().slideToggle();
			    $(this).toggleClass('active').toggleClass('inactive');
			    $(this).next().slideToggle();
			    $(this).next().find('.n-heading').removeClass('n-active');
			    $(this).next().find('.n-heading').addClass('n-inactive');
			    $(this).next().find('.n-accordion-content').hide();
		    }

		    else {
			    $(this).toggleClass('active').toggleClass('inactive');
			    $(this).next().slideToggle();

		    }
		    /* custom scroll bar */
		    $('.ScrollPaneBlock').jScrollPane();
	    });
	// onload keep open
        $('.accordion .heading').not('.active').addClass('inactive');
        $('.accordion .heading.active').next().show();
}

/*---Accordion Nested-----*/
function accordionNested() {

	$('.accordionNested .n-heading').prepend(
	    '<span class="n-spriteArrow"></span>');
	$('.accordionNested .n-heading').click(
	    function() {
		    /* Custom scroll */
		    var accordionIdNested = $(this).parent().parent().attr('id');
		    accordionIdNested = ('#' + accordionIdNested);

		    if ($(this).is('.n-inactive')) {
			    $(accordionIdNested + ' .n-active').toggleClass('n-active')
			        .toggleClass('n-inactive').next().slideToggle();
			    $(this).toggleClass('n-active').toggleClass('n-inactive');
			    $(this).next().slideToggle();
			    /* Custom scroll */
			    $('.ui-jqgrid-bdiv').jScrollPane();
		    }

		    else {
			    $(this).toggleClass('n-active').toggleClass('n-inactive');
			    $(this).next().slideToggle();

		    }

	    });
	// onload keep open
        $('.accordionNested .n-heading').not('.n-active').addClass('n-inactive');
        $('.accordionNested .n-heading.n-active').next().show();
        /* Custom scroll */
        $('.ui-jqgrid-bdiv').jScrollPane();
}

/* show panel */
function tabGridNew(parentId) {
  $('#gridBlocks_Panel').hide();
  destroyScrollPane(parentId);
  $('#gridBlocks_Panel').show();
  $('#chartBlocks_Panel').hide();
  $('#graphBlocks_Panel').hide();
  /* Custom scroll */

  $('.ui-jqgrid-bdiv').each(function(index) {
    var tempName = $(this).parent().attr('id');
    if (tempName == parentId) {
      $(this).jScrollPane({maintainPosition : true, stickToRight : true});  
    }
  });

  $('#btngridIcon').addClass('gridIconActive');
  $('#btngridIcon').removeClass('gridIcon');

  $('#btnchartIcon').addClass('chartIcon');
  $('#btnchartIcon').removeClass('chartIconActive');

  $('#btngraphIcon').addClass('graphIcon');
  $('#btngraphIcon').removeClass('graphIconActive');
}

function tabChart() {

	$('#gridBlocks_Panel').hide();
	$('#chartBlocks_Panel').show();
	$('#graphBlocks_Panel').hide();

	$('#btngridIcon').addClass('gridIcon');
	$('#btngridIcon').removeClass('gridIconActive');

	$('#btnchartIcon').addClass('chartIconActive');
	$('#btnchartIcon').removeClass('chartIcon');

	$('#btngraphIcon').addClass('graphIcon');
	$('#btngraphIcon').removeClass('graphIconActive');
}

/* Top tab Panel */
function tabAll() {
  // update currentActiveNotificationTab value
  currentActiveNotificationTab = "ALL";
  if (isThisGoodResolution) {
    $('#TopTab_All').show();
  }
  $('#TopTab_Error').hide();
  $('#TopTab_Warning').hide();
  $('#TopTab_Severe').hide();

  $('#btnTopTab_All').addClass('TopTabLinkActive');
  $('#btnTopTab_Error').removeClass('TopTabLinkActive');
  $('#btnTopTab_Warning').removeClass('TopTabLinkActive');
  $('#btnTopTab_Severe').removeClass('TopTabLinkActive');
  $('.scroll-pane').jScrollPane();
}

function tabError() {
  // update currentActiveNotificationTab value
  currentActiveNotificationTab = "ERROR";
  $('#TopTab_All').hide();
  if (isThisGoodResolution) {
    $('#TopTab_Error').show();
  }
  $('#TopTab_Warning').hide();
  $('#TopTab_Severe').hide();

  $('#btnTopTab_All').removeClass('TopTabLinkActive');
  $('#btnTopTab_Error').addClass('TopTabLinkActive');
  $('#btnTopTab_Warning').removeClass('TopTabLinkActive');
  $('#btnTopTab_Severe').removeClass('TopTabLinkActive');
  $('.scroll-pane').jScrollPane();
}

function tabWarning() {
  // update currentActiveNotificationTab value
  currentActiveNotificationTab = "WARNING";
  $('#TopTab_All').hide();
  $('#TopTab_Error').hide();
  if (isThisGoodResolution) {
    $('#TopTab_Warning').show();
  }
  $('#TopTab_Severe').hide();

  $('#btnTopTab_All').removeClass('TopTabLinkActive');
  $('#btnTopTab_Error').removeClass('TopTabLinkActive');
  $('#btnTopTab_Warning').addClass('TopTabLinkActive');
  $('#btnTopTab_Severe').removeClass('TopTabLinkActive');
  $('.scroll-pane').jScrollPane();
}

function tabSevere() {
  // update currentActiveNotificationTab value
  currentActiveNotificationTab = "SEVERE";
  $('#TopTab_All').hide();
  $('#TopTab_Error').hide();
  $('#TopTab_Warning').hide();
  if (isThisGoodResolution) {
    $('#TopTab_Severe').show();
  }

  $('#btnTopTab_All').removeClass('TopTabLinkActive');
  $('#btnTopTab_Error').removeClass('TopTabLinkActive');
  $('#btnTopTab_Warning').removeClass('TopTabLinkActive');
  $('#btnTopTab_Severe').addClass('TopTabLinkActive');
  $('.scroll-pane').jScrollPane();
}
/* Auto Complete box */
/**
 * function used for opening Cluster View
 */
function openClusterDetail() {
	location.href = 'clusterDetail.html';
}
/**
 * function used for opening Data View
 */
function openDataView() {
	location.href = 'dataView.html';
}
/**
 * function used for opening Data Browser
 */
function openDataBrowser() {
	location.href = 'DataBrowser.html';
}

/**
 * function used for opening Query statistics
 */
function openQueryStatistics() {
  location.href = 'QueryStatistics.html';
}

function destroyScrollPane(scrollPaneParentId) {
    $('.ui-jqgrid-bdiv').each(function(index) {
      var tempName = $(this).parent().attr('id');
      if (tempName == scrollPaneParentId) {
        var api = $(this).data('jsp');
        api.destroy();
      }
    });
}

//Initial set up for hotspot drop down
function initHotspotDropDown() {

  var htmlHotSpotList = generateHotSpotListHTML(hotspotAttributes);

  // Set list height
  if(hotspotAttributes.length <= 5){
    $("#hotspotListContainer").height(hotspotAttributes.length * 26);
  }else{
    $("#hotspotListContainer").height(5 * 26);
  }

  $('#hotspotList').html(htmlHotSpotList);
  $('.jsonSuggestScrollFilter').jScrollPane();

  currentHotspotAttribiute = hotspotAttributes[0].id;
  $('#currentHotSpot').html(hotspotAttributes[0].name);

  // Hot Spot List event handlers
  $(".hotspotClicked-Off").click(function(e) {
    e.preventDefault();
    $("div#hotspotSetting").toggle();
    $(".hotspotClicked-Off").toggleClass("hotspotClicked-On");
    $('.jsonSuggestScrollFilter').jScrollPane();
  });

  $("div#hotspotSetting").mouseup(function() {
    return false;
  });
  $(document).mouseup(function(e) {
    if ($(e.target).parent("a.hotspotClicked-Off").length == 0) {
      $(".hotspotClicked-Off").removeClass("hotspotClicked-On");
      $("div#hotspotSetting").hide();
    }
  });
}

//Function to be called on when Hot Spot selection changes
function onHotspotChange(element) {
  if (element != currentHotspotAttribiute) {
    for ( var cnt = 0; cnt < hotspotAttributes.length; cnt++) {
      if (element == hotspotAttributes[cnt].id) {
        currentHotspotAttribiute = hotspotAttributes[cnt].id;
        $('#currentHotSpot').html(hotspotAttributes[cnt].name);
        applyHotspot();
      }
    }
  }
  // Hide drop down
  $(".hotspotClicked-Off").removeClass("hotspotClicked-On");
  $("div#hotspotSetting").hide();
}

//Function to generate HTML for Hot Spot list drop down
function generateHotSpotListHTML(hotspotAttributes) {
  var htmlHotSpotList = '';
  for ( var i = 0; i < hotspotAttributes.length; i++) {
    htmlHotSpotList += '<div class="resultItemFilter">'
        + '<a href="#"  onclick="onHotspotChange(\'' + hotspotAttributes[i].id
        + '\');" >' + hotspotAttributes[i].name + '</a></div>';
  }
  return htmlHotSpotList;
}

//Merged 3102 from 702X_maint
function destroyScrollPane(scrollPaneParentId) { 
   $('.ui-jqgrid-bdiv').each(function(index) { 
   var tempName = $(this).parent().attr('id'); 
   if (tempName == scrollPaneParentId) { 
     var api = $(this).data('jsp'); 
       api.destroy(); 
    } 
    }); 
}
