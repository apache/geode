(function() {
  var MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

  function toggleClass(el, className) {
    var check = new RegExp("\\b" + className + "\\b");
    if (check.test(el.className)) {
      el.className = el.className.replace(check, '');
    } else {
      el.className += ' ' + className;
    }
  }

  function openSubmenu(e) {
    if (e.target.tagName !== 'A') {
      var el = e.currentTarget;
      toggleClass(el, 'expanded');
      e.stopPropagation();
    }
  }

  function registerOnClick(el, handler) {
    if (el.addEventListener) {
      el.addEventListener('click', handler);
    } else {
      el.onclick = handler;
    }
  }

  function toggleMainMenu(e) {
    var el = e.currentTarget;
    toggleClass(el.parentNode, 'menu-active');
  }

  function toggleSubMenu(e) {
    var el = e.currentTarget;
    toggleClass(el.parentNode, 'active');
  }

  function displayDate(millis) {
    millis = parseInt(millis, 10);
    var date = new Date(millis);

    return [MONTHS[date.getMonth()], ' ', date.getDate(), ', ', date.getFullYear()].join('');
  }

  function maybeOpenNewWindow(e) {
    var el = e.currentTarget;
    var href = el.href;

    if (Bookbinder.needsNewWindow(e, href, window.location.host)) {
      e.preventDefault();
      e.stopPropagation();
      window.open(href);
    }
  }

  window.Bookbinder = {
    startSidenav: function(rootEl, currentPath) {
      if (!rootEl) { return; }

      var submenus = rootEl.querySelectorAll('.has_submenu');

      for (var i = 0; i < submenus.length; i++) {
        registerOnClick(submenus[i], openSubmenu);
      }

      if (currentPath) {
        var currentLink = rootEl.querySelector('a[href="' + currentPath + '"]');
        if (currentLink) {
          currentLink.className += ' active';

          var hasSubmenu = /\bhas_submenu\b/;
          var subnavLocation = currentLink.parentNode;

          while(subnavLocation.parentNode !== rootEl) {
            subnavLocation = subnavLocation.parentNode;
            if (hasSubmenu.test(subnavLocation.className)) {
              subnavLocation.className += ' expanded';
            }
          }

          rootEl.scrollTop = currentLink.offsetTop - rootEl.offsetTop;
        }
      }
    },
    mobileMainMenu: function(root) {
      var mainMenus = root.querySelectorAll('[data-behavior=MenuMobile]');

      for (var i = 0; i < mainMenus.length; i++) {
        registerOnClick(mainMenus[i], toggleMainMenu);
      }
    },
    mobileSubMenu: function(root) {
      var subMenus = root.querySelectorAll('[data-behavior=SubMenuMobile]');

      for (var i = 0; i < subMenus.length; i++) {
        registerOnClick(subMenus[i], toggleSubMenu);
      }
    },
    modifiedDates: function(root) {
      var datesElements = root.querySelectorAll('[data-behavior=DisplayModifiedDate]');

      for (var i = 0; i < datesElements.length; i++) {
        datesElements[i].innerText = displayDate(datesElements[i].getAttribute('data-modified-date'));
      }
    },
    externalLinks: function(root) {
      var links = root.querySelectorAll('a[href]');

      for (var i = 0; i < links.length; i++) {
        registerOnClick(links[i], maybeOpenNewWindow);
      }
    },
    needsNewWindow: function(e, destinationUrl, currentDomain) {
      if (e.button !== 0 || e.ctrlKey || e.altKey || e.shiftKey || e.metaKey) {
        return false;
      }

      var destinationDomain = destinationUrl.replace(/^https?:\/\//, '').split('/')[0];
      return destinationDomain !== currentDomain;
    },
    boot: function() {
      Bookbinder.startSidenav(document.querySelector('#sub-nav'), document.location.pathname);
      Bookbinder.mobileMainMenu(document);
      Bookbinder.mobileSubMenu(document);
      Bookbinder.modifiedDates(document);
      Bookbinder.externalLinks(document);
    }
  };
})();
