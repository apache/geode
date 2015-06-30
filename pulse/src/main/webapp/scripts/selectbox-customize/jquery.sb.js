(function( $, window, undefined ) {
    // utility functions
    $.fn.borderWidth = function() { return $(this).outerWidth() - $(this).innerWidth(); };
    $.fn.paddingWidth = function() { return $(this).innerWidth() - $(this).width(); };
    $.fn.extraWidth = function() { return $(this).outerWidth(true) - $(this).width(); };
    $.fn.offsetFrom = function( e ) {
        var $e = $(e);
        return {
            left: $(this).offset().left - $e.offset().left,
            top: $(this).offset().top - $e.offset().top
        };
    };
    $.fn.maxWidth = function() {
        var max = 0;
        $(this).each(function() {
            if($(this).width() > max) {
              max = $(this).width();
            }
        });
        return max;
    };
    $.fn.triggerAll = function(event, params) {
      return $(this).each(function() {
        $(this).triggerHandler(event, params);
      });
    };
    var aps = Array.prototype.slice,
        randInt = function() {
            return Math.floor(Math.random() * 999999999);
        };
    
    // jQuery-Proto
    $.proto = function() {
        var name = arguments[0],    // The name of the jQuery function that will be called
            clazz = arguments[1],   // A reference to the class that you are associating
            klazz = clazz,          // A version of clazz with a delayed constructor
            extOpt = {},            // used to extend clazz with a variable name for the init function
            undefined;              // safety net
        
        opts = $.extend({
            elem: "elem",           // the property name on the object that will be set to the current jQuery context
            access: "access",       // the name of the access function to be set on the object
            init: "init",           // the name of the init function to be set on the object
            instantAccess: false    // when true, treat all args as access args (ignore constructor args) and allow construct/function call at the same time
        }, arguments[2]);
        
        if(clazz._super) {
            extOpt[opts.init] = function(){};
            klazz = clazz.extend(extOpt);
        }
        
        $.fn[name] = function() {
            var result, args = arguments;
                
            $(this).each(function() {
                var $e = $(this),
                    obj = $e.data(name),
                    isNew = !obj;
                
                // if the object is not defined for this element, then construct
                if(isNew) {
                    
                    // create the new object and restore init if necessary
                    obj = new klazz();
                    if(clazz._super) {
                      obj[opts.init] = clazz.prototype.init;
                    }
                    
                    // set the elem property and initialize the object
                    obj[opts.elem] = $e[0];
                    if(obj[opts.init]) {
                        obj[opts.init].apply(obj, opts.instantAccess ? [] : aps.call(args, 0));
                    }
                    
                    // associate it with the element
                    $e.data(name, obj);
                    
                }
                
                // if it is defined or we allow instance access, then access
                if(!isNew || opts.instantAccess) {
                  
                    // call the access function if it exists (allows lazy loading)
                    if(obj[opts.access]) {
                        obj[opts.access].apply(obj, aps.call(args, 0));
                    }
                    
                    // do something with the object
                    if(args.length > 0) {
                    
                        if($.isFunction(obj[args[0]])) {
                        
                            // use the method access interface
                            result = obj[args[0]].apply(obj, aps.call(args, 1));
                            
                        } else if(args.length === 1) {
                          
                            // just retrieve the property (leverage deep access with getObject if we can)
                            if($.getObject) {
                              result = $.getObject(args[0], obj);
                            } else {
                              result = obj[args[0]];
                            }
                            
                        } else {
                          
                            // set the property (leverage deep access with setObject if we can)
                            if($.setObject) {
                              $.setObject(args[0], args[1], obj);
                            } else {
                              obj[args[0]] = args[1];
                            }
                            
                        }
                        
                    } else if(result === undefined) {
                    
                        // return the first object if there are no args
                        result = $e.data(name);
                        
                    }
                }
            });
            
            // chain if no results were returned from the clazz's method (it's a setter)
            if(result === undefined) {
              return $(this);
            }
            
            // return the first result if not chaining
            return result;
        };
    };
    
    var falseFunc = function() {
            return false;
        },
        SelectBox = function() {
        
        var self = this,
            o = {},
            $orig = null,
            $label = null,
            $sb = null,
            $display = null,
            $dd = null,
            $items = null,
            searchTerm = "",
            cstTimeout = null,
            delayReloadTimeout = null,
            resizeTimeout = null,
            
            // functions
            loadSB,
            createOption,
            focusOrig,
            blurOrig,
            destroySB,
            reloadSB,
            delayReloadSB,
            openSB,
            centerOnSelected,
            closeSB,
            positionSB,
            positionSBIfOpen,
            delayPositionSB,
            clickSB,
            clickSBItem,
            keyupSB,
            keydownSB,
            focusSB,
            blurSB,
            addHoverState,
            removeHoverState,
            addActiveState,
            removeActiveState,
            getDDCtx,
            getSelected,
            getEnabled,
            selectItem,
            clearSearchTerm,
            findMatchingItem,
            selectMatchingItem,
            selectNextItemStartsWith,
            closeAll,
            closeAllButMe,
            closeAndUnbind,
            blurAllButMe,
            stopPageHotkeys,
            flickerDisplay,
            unbind;
        
        loadSB = function() {
            
            // create the new sb
            $sb = $("<div class='sb " + o.selectboxClass + " " + $orig.attr("class") + "' id='sb" + randInt() + "'></div>")
                .attr("role", "listbox")
                .attr("aria-has-popup", "true")
                .attr("aria-labelledby", $label.attr("id") ? $label.attr("id") : "");
            $("body").append($sb);
            
            // generate the display markup
            var displayMarkup = $orig.children().size() > 0
                ? o.displayFormat.call($orig.find("option:selected")[0], 0, 0)
                : "&nbsp;";
            $display = $("<div class='display " + $orig.attr("class") + "' id='sbd" + randInt() + "'></div>")
                .append($("<div class='text'></div>").append(displayMarkup))
                .append(o.arrowMarkup);
            $sb.append($display);
            
            // generate the dropdown markup
            $dd = $("<ul class='" + o.selectboxClass + " items " + $orig.attr("class") + "' role='menu' id='sbdd" + randInt() + "'></ul>")
                .attr("aria-hidden", "true");
            $sb.append($dd)
                .attr("aria-owns", $dd.attr("id"));
            if($orig.children().size() === 0) {
                $dd.append(createOption().addClass("selected"));
            } else {
                $orig.children().each(function( i ) {
                    var $opt, $og, $ogItem, $ogList;
                    if($(this).is("optgroup")) {
                        $og = $(this);
                        $ogItem = $("<li class='optgroup'>" + o.optgroupFormat.call($og[0], i+1) + "</li>")
                            .addClass($og.is(":disabled") ? "disabled" : "")
                            .attr("aria-disabled", $og.is(":disabled") ? "true" : "");
                        $ogList = $("<ul class='items'></ul>");
                        $ogItem.append($ogList);
                        $dd.append($ogItem);
                        $og.children("option").each(function() {
                            $opt = createOption($(this), i)
                                .addClass($og.is(":disabled") ? "disabled" : "")
                                .attr("aria-disabled", $og.is(":disabled") ? "true" : "");
                            $ogList.append($opt);
                        });
                    } else {
                        $dd.append(createOption($(this), i));
                    }
                });
            }
            
            // cache all sb items
            $items = $dd.find("li").not(".optgroup");
            
            // for accessibility/styling
            $sb.attr("aria-active-descendant", $items.filter(".selected").attr("id"));
            $dd.children(":first").addClass("first");
            $dd.children(":last").addClass("last");
            
            // modify width based on fixedWidth/maxWidth options
            if(!o.fixedWidth) {
                var largestWidth = $dd.find(".text, .optgroup").maxWidth() + $display.extraWidth() + 1;
                $sb.width(o.maxWidth ? Math.min(o.maxWidth, largestWidth) : largestWidth);
            } else if(o.maxWidth && $sb.width() > o.maxWidth) {
                $sb.width(o.maxWidth);
            }
            
            // place the new markup in its semantic location (hide/show fixes positioning bugs)
            $orig.before($sb).addClass("has_sb").hide().show();
            
            // these two lines fix a div/span display bug on load in ie7
            positionSB();
            flickerDisplay();
            
            // hide the dropdown now that it's initialized
            $dd.hide();
            
            // bind events
            if(!$orig.is(":disabled")) {
                $orig
                    .bind("blur.sb", blurOrig)
                    .bind("focus.sb", focusOrig);
                $display
                    .mouseup(addActiveState)
                    .mouseup(clickSB)
                    .click(falseFunc)
                    .focus(focusSB)
                    .blur(blurSB)
                    .hover(addHoverState, removeHoverState);
                getEnabled()
                    .click(clickSBItem)
                    .hover(addHoverState, removeHoverState);
                $dd.find(".optgroup")
                    .hover(addHoverState, removeHoverState)
                    .click(falseFunc);
                $items.filter(".disabled")
                    .click(falseFunc);
                if(!$.browser.msie || $.browser.version >= 9) {
                    $(window).resize($.throttle ? $.throttle(100, positionSBIfOpen) : delayPositionSB);
                }
            } else {
                $sb.addClass("disabled").attr("aria-disabled");
                $display.click(function( e ) { e.preventDefault(); });
            }
            
            // bind custom events
            $sb.bind("close.sb", closeSB).bind("destroy.sb", destroySB);
            $orig.bind("reload.sb", reloadSB);
            if($.fn.tie && o.useTie) {
                $orig.bind("domupdate.sb", delayReloadSB);
            }
        };
        
        delayPositionSB = function() {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(positionSBIfOpen, 50);
        };
        
        positionSBIfOpen = function() {
            if($sb.is(".open")) {
                positionSB();
                openSB(true);
            }
        }
        
        // create new markup from an <option>
        createOption = function( $option, index ) {
            if(!$option) { 
                $option = $("<option value=''>&nbsp;</option>");
                index = 0;
            }
            var $li = $("<li id='sbo" + randInt() + "'></li>")
                    .attr("role", "option")
                    .data("orig", $option[0])
                    .data("value", $option ? $option.attr("value") : "")
                    .addClass($option.is(":selected") ? "selected" : "")
                    .addClass($option.is(":disabled") ? "disabled" : "")
                    .attr("aria-disabled", $option.is(":disabled") ? "true" : ""),
                $inner = $("<div class='item'></div>"),
                $text = $("<div class='text'></div>")
                    .html(o.optionFormat.call($option[0], 0, index + 1));
            return $li.append($inner.append($text));
        };
        
        // causes focus if original is focused
        focusOrig = function() {
            blurAllButMe();
            $display.triggerHandler("focus");
        };
        
        // loses focus if original is blurred
        blurOrig = function() {
            if(!$sb.is(".open")) {
                $display.triggerHandler("blur");
            }
        };
        
        // unbind and remove
        destroySB = function( internal ) {
            $sb.remove();
            $orig
                .unbind(".sb")
                .removeClass("has_sb");
            $(window).unbind("resize", delayPositionSB);
            if(!internal) {
                $orig.removeData("sb");
            }
        };
        
        // destroy then load, maintaining open/focused state if applicable
        reloadSB = function() {
            var isOpen = $sb.is(".open"),
                isFocused = $display.is(".focused");
            closeSB(true);
            destroySB(true);
            self.init(o);
            if(isOpen) {
                $orig.focus();
                openSB(true);
            } else if(isFocused) {
                $orig.focus();
            }
        };
        
        // debouncing when useTie === true
        delayReloadSB = function() {
            clearTimeout(delayReloadTimeout);
            delayReloadTimeout = setTimeout(reloadSB, 30);
        };
        
        // when the user clicks outside the sb
        closeAndUnbind = function() {
            $sb.removeClass("focused");
            closeSB();
            unbind();
        };
        
        unbind = function() {
          $(document)
              .unbind("click", closeAndUnbind)
              .unbind("keyup", keyupSB)
              .unbind("keypress", stopPageHotkeys)
              .unbind("keydown", stopPageHotkeys)
              .unbind("keydown", keydownSB);
        };
        
        // trigger all sbs to close
        closeAll = function() {
            $(".sb.open." + o.selectboxClass).triggerAll("close");
        };
        
        // trigger all sbs to blur
        blurAllButMe = function() {
            $(".sb.focused." + o.selectboxClass).not($sb[0]).find(".display").blur();
        };
        
        // to prevent multiple selects open at once
        closeAllButMe = function() {
            $(".sb.open." + o.selectboxClass).not($sb[0]).triggerAll("close");
        };
        
        // hide and reset dropdown markup
        closeSB = function( instantClose ) {
            if($sb.is(".open")) {
                $display.blur();
                $items.removeClass("hover");
                unbind();
                $dd.attr("aria-hidden", "true");
                if(instantClose === true) {
                  $dd.hide();
                  $sb.removeClass("open");
                  $sb.append($dd);
                } else {
                    $dd.fadeOut(o.animDuration, function() {
                        $sb.removeClass("open");
                        $sb.append($dd);
                    });
                }
            }
        };
        
        // since the context can change, we should get it dynamically
        getDDCtx = function() {
            var $ddCtx = null;
            if(o.ddCtx === "self") {
                $ddCtx = $sb;
            } else if($.isFunction(o.ddCtx)) {
                $ddCtx = $(o.ddCtx.call($orig[0]));
            } else {
                $ddCtx = $(o.ddCtx);
            }
            return $ddCtx;
        };
        
        // DRY
        getSelected = function() {
          return $items.filter(".selected");
        };
        
        // DRY
        getEnabled = function() {
          return $items.not(".disabled");
        };
        
        // reposition the scroll of the dropdown so the selected option is centered (or appropriately onscreen)
        centerOnSelected = function() {
            $dd.scrollTop($dd.scrollTop() + getSelected().offsetFrom($dd).top - $dd.height() / 2 + getSelected().outerHeight(true) / 2);
        };
        
        flickerDisplay = function() {
            if($.browser.msie && $.browser.version < 8) {
                $("." + o.selectboxClass + " .display").hide().show(); // fix ie7 display bug
            }
        };
        
        // show, reposition, and reset dropdown markup
        openSB = function( instantOpen ) {
            var dir,
                $ddCtx = getDDCtx();
            blurAllButMe();
            $sb.addClass("open");
            $ddCtx.append($dd);
            dir = positionSB();
            $dd.attr("aria-hidden", "false");
            if(instantOpen === true) {
                $dd.show();
                centerOnSelected();
            } else if(dir === "down") {
                $dd.slideDown(o.animDuration, centerOnSelected);
            } else {
                $dd.fadeIn(o.animDuration, centerOnSelected);
            }
            $orig.focus();
        };
        
        // position dropdown based on collision detection
        positionSB = function() {
            var $ddCtx = getDDCtx(),
                ddMaxHeight = 0,
                ddX = $display.offsetFrom($ddCtx).left,
                ddY = 0,
                dir = "",
                ml, mt,
                bottomSpace, topSpace,
                bottomOffset, spaceDiff,
                bodyX, bodyY;
            
            // modify dropdown css for getting values
            $dd.removeClass("above");
            $dd.show().css({
                maxHeight: "none",
                position: "relative",
                visibility: "hidden"
            });
            if(!o.fixedWidth) {
              $dd.width($display.outerWidth() - $dd.extraWidth() + 1);
            }
            
            // figure out if we should show above/below the display box
            bottomSpace = $(window).scrollTop() + $(window).height() - $display.offset().top - $display.outerHeight();
            topSpace = $display.offset().top - $(window).scrollTop();
            bottomOffset = $display.offsetFrom($ddCtx).top + $display.outerHeight();
            spaceDiff = bottomSpace - topSpace + o.dropupThreshold;
            if($dd.outerHeight() < bottomSpace) {
                ddMaxHeight = o.maxHeight ? o.maxHeight : bottomSpace;
                ddY = bottomOffset;
                dir = "down";
            } else if($dd.outerHeight() < topSpace) {
                ddMaxHeight = o.maxHeight ? o.maxHeight : topSpace;
                ddY = $display.offsetFrom($ddCtx).top - Math.min(ddMaxHeight, $dd.outerHeight());
                dir = "up";
            } else if(spaceDiff >= 0) {
                ddMaxHeight = o.maxHeight ? o.maxHeight : bottomSpace;
                ddY = bottomOffset;
                dir = "down";
            } else if(spaceDiff < 0) {
                ddMaxHeight = o.maxHeight ? o.maxHeight : topSpace;
                ddY = $display.offsetFrom($ddCtx).top - Math.min(ddMaxHeight, $dd.outerHeight());
                dir = "up";
            } else {
                ddMaxHeight = o.maxHeight ? o.maxHeight : "none";
                ddY = bottomOffset;
                dir = "down";
            }
            
            ml = ("" + $("body").css("margin-left")).match(/^\d+/) ? $("body").css("margin-left") : 0;
            mt = ("" + $("body").css("margin-top")).match(/^\d+/) ? $("body").css("margin-top") : 0;
            bodyX = $().jquery >= "1.4.2"
                ? parseInt(ml)
                : $("body").offset().left;
            bodyY = $().jquery >= "1.4.2"
                ? parseInt(mt)
                : $("body").offset().top;
            
            
            // modify dropdown css for display
            $dd.hide().css({
                left: ddX + ($ddCtx.is("body") ? bodyX : 0),
                maxHeight: ddMaxHeight,
                position: "absolute",
                top: ddY + ($ddCtx.is("body") ? bodyY : 0),
                visibility: "visible"
            });
            if(dir === "up") {
              $dd.addClass("above");
            }
            return dir;
        };
        
        // when the user explicitly clicks the display
        clickSB = function( e ) {
            if($sb.is(".open")) {
                closeSB();
            } else {
                openSB();
            }
            return false;
        };
        
        // when the user selects an item in any manner
        selectItem = function() {
            var $item = $(this),
                oldVal = $orig.val(),
                newVal = $item.data("value");
            
            // update the original <select>
            $orig.find("option").each(function() { this.selected = false; });
            $($item.data("orig")).each(function() { this.selected = true; });
            
            // change the selection to this item
            $items.removeClass("selected");
            $item.addClass("selected");
            $sb.attr("aria-active-descendant", $item.attr("id"));
            
            // update the title attr and the display markup
            $display.find(".text").attr("title", $item.find(".text").html());
            $display.find(".text").html(o.displayFormat.call($item.data("orig")));
            
            // trigger change on the old <select> if necessary
            if(oldVal !== newVal) {
                $orig.change();
            }
        };
        
        // when the user explicitly clicks an item
        clickSBItem = function( e ) {
            closeAndUnbind();
            $orig.focus();
            selectItem.call(this);
            return false;
        };
        
        // start over for generating the search term
        clearSearchTerm = function() {
            searchTerm = "";
        };
        
        // iterate over all the options to see if any match the search term
        findMatchingItem = function( term ) {
            var i, t, $tNode,
                $available = getEnabled();
            for(i=0; i < $available.size(); i++) {
                $tNode = $available.eq(i).find(".text");
                t = $tNode.children().size() == 0 ? $tNode.text() : $tNode.find("*").text();
                if(term.length > 0 && t.toLowerCase().match("^" + term.toLowerCase())) {
                    return $available.eq(i);
                }
            }
            return null;
        };
        
        // if we get a match for any options, select it
        selectMatchingItem = function( text ) {
            var $matchingItem = findMatchingItem(text);
            if($matchingItem !== null) {
                selectItem.call($matchingItem[0]);
                return true;
            }
            return false;
        };
        
        // stop up/down/backspace/space from moving the page
        stopPageHotkeys = function( e ) {
            if(e.ctrlKey || e.altKey) {
                return;
            }
            if(e.which === 38 || e.which === 40 || e.which === 8 || e.which === 32) {
                e.preventDefault();
            }
        };
        
        // if a normal match fails, try matching the next element that starts with the pressed letter
        selectNextItemStartsWith = function( c ) {
            var i, t,
                $selected = getSelected(),
                $available = getEnabled();
            for(i = $available.index($selected) + 1; i < $available.size(); i++) {
                t = $available.eq(i).find(".text").text();
                if(t !== "" && t.substring(0,1).toLowerCase() === c.toLowerCase()) {
                    selectItem.call($available.eq(i)[0]);
                    return true;
                }
            }
            return false;
        };
        
        // go up/down using arrows or attempt to autocomplete based on string
        keydownSB = function( e ) {
            if(e.altKey || e.ctrlKey) {
                return false;
            }
            var $selected = getSelected(),
                $enabled = getEnabled();
            switch(e.which) {
            case 9: // tab
                closeSB();
                blurSB();
                break;
            case 35: // end
                if($selected.size() > 0) {
                    e.preventDefault();
                    selectItem.call($enabled.filter(":last")[0]);
                    centerOnSelected();
                }
                break;
            case 36: // home
                if($selected.size() > 0) {
                    e.preventDefault();
                    selectItem.call($enabled.filter(":first")[0]);
                    centerOnSelected();
                }
                break;
            case 38: // up
                if($selected.size() > 0) {
                    if($enabled.filter(":first")[0] !== $selected[0]) {
                        e.preventDefault();
                        selectItem.call($enabled.eq($enabled.index($selected)-1)[0]);
                    }
                    centerOnSelected();
                }
                break;
            case 40: // down
                if($selected.size() > 0) {
                    if($enabled.filter(":last")[0] !== $selected[0]) {
                        e.preventDefault();
                        selectItem.call($enabled.eq($enabled.index($selected)+1)[0]);
                        centerOnSelected();
                    }
                } else if($items.size() > 1) {
                    e.preventDefault();
                    selectItem.call($items.eq(0)[0]);
                }
                break;
            default:
                break;
            }
        };
        
        // the user is typing -- try to select an item based on what they press
        keyupSB = function( e ) {
            if(e.altKey || e.ctrlKey) {
              return false;
            }
            if(e.which !== 38 && e.which !== 40) {
                
                // add to the search term
                searchTerm += String.fromCharCode(e.keyCode);
                
                if(selectMatchingItem(searchTerm)) {
                
                    // we found a match, continue with the current search term
                    clearTimeout(cstTimeout);
                    cstTimeout = setTimeout(clearSearchTerm, o.acTimeout);
                    
                } else if(selectNextItemStartsWith(String.fromCharCode(e.keyCode))) {
                    
                    // we selected the next item that starts with what you just pressed
                    centerOnSelected();
                    clearTimeout(cstTimeout);
                    cstTimeout = setTimeout(clearSearchTerm, o.acTimeout);
                    
                } else {
                    
                    // no matches were found, clear everything
                    clearSearchTerm();
                    clearTimeout(cstTimeout);
                    
                }
            }
        };
        
        // when the sb is focused (by tab or click), allow hotkey selection and kill all other selectboxes
        focusSB = function() {
            closeAllButMe();
            $sb.addClass("focused");
            $(document)
                .click(closeAndUnbind)
                .keyup(keyupSB)
                .keypress(stopPageHotkeys)
                .keydown(stopPageHotkeys)
                .keydown(keydownSB);
        };
        
        // when the sb is blurred (by tab or click), disable hotkey selection
        blurSB = function() {
            $sb.removeClass("focused");
            $display.removeClass("active");
            $(document)
                .unbind("keyup", keyupSB)
                .unbind("keydown", stopPageHotkeys)
                .unbind("keypress", stopPageHotkeys)
                .unbind("keydown", keydownSB);
        };
        
        // add hover class to an element
        addHoverState = function() {
          $(this).addClass("hover");
        };
        
        // remove hover class from an element
        removeHoverState = function() {
          $(this).removeClass("hover");
        };
        
        // add active class to the display
        addActiveState = function() {
          $display.addClass("active");
          $(document).bind("mouseup", removeActiveState);
        };
        
        // remove active class from an element
        removeActiveState = function() {
          $display.removeClass("active");
          $(document).unbind("mouseup", removeActiveState);
        };
        
        // constructor
        this.init = function( opts ) {
            
            // this plugin is not compatible with IE6 and below;
            // a normal <select> will be displayed for old browsers
            if($.browser.msie && $.browser.version < 7) {
              return;
            }
        
            // get the original <select> and <label>
            $orig = $(this.elem);
            if($orig.attr("id")) {
                $label = $("label[for='" + $orig.attr("id") + "']:first");
            }
            if(!$label || $label.size() === 0) {
                $label = $orig.closest("label");
            }
            
            // don't create duplicate SBs
            if($orig.hasClass("has_sb")) {
                return;
            }
            
            // set the various options
            o = $.extend({
                acTimeout: 800,               // time between each keyup for the user to create a search string
                animDuration: 200,            // time to open/close dropdown in ms
                ddCtx: 'body',                // body | self | any selector | a function that returns a selector (the original select is the context)
                dropupThreshold: 150,         // the minimum amount of extra space required above the selectbox for it to display a dropup
                fixedWidth: false,            // if false, dropdown expands to widest and display conforms to whatever is selected
                maxHeight: false,             // if an integer, show scrollbars if the dropdown is too tall
                maxWidth: false,              // if an integer, prevent the display/dropdown from growing past this width; longer items will be clipped
                selectboxClass: 'selectbox',  // class to apply our markup
                useTie: false,                // if jquery.tie is included and this is true, the selectbox will update dynamically
                
                // markup appended to the display, typically for styling an arrow
                arrowMarkup: "<div class='arrow_btn'><span class='arrow'></span></div>",
                
                // use optionFormat by default
                displayFormat: undefined,
                
                // formatting for the display; note that it will be wrapped with <a href='#'><span class='text'></span></a>
                optionFormat: function( ogIndex, optIndex ) {
                    if($(this).size() > 0) {
                        var label = $(this).attr("label");
                        if(label && label.length > 0) {
                          return label;
                        }
                        return $(this).text();
                    } else {
                        return "";
                    }
                },
                
                // the function to produce optgroup markup
                optgroupFormat: function( ogIndex ) {
                    return "<span class='label'>" + $(this).attr("label") + "</span>";
                }
            }, opts);
            o.displayFormat = o.displayFormat || o.optionFormat;
            
            // generate the new sb
            loadSB();
        };
        
        // public method interface
        this.open = openSB;
        this.close = closeSB;
        this.refresh = reloadSB;
        this.destroy = destroySB;
        this.options = function( opts ) {
            o = $.extend(o, opts);
            reloadSB();
        };
    };

    $.proto("sb", SelectBox);

}(jQuery, window));