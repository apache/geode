function loadFeedControl() {
  var feed = "https://www.google.com/calendar/feeds/gejamo1ppmtohgneonsen1a9vc%40group.calendar.google.com/public/basic?"+new Date().getTime();
  var fg = new GFdynamicFeedControl(feed, "calendar");
}
google.load("feeds", "1");
google.setOnLoadCallback(loadFeedControl);
