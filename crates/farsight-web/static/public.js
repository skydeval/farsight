// Farsight public UI: the theme toggle and a relative reading next to
// absolute times. Nothing here is needed to read a page.
(function () {
  "use strict";
  var root = document.documentElement;
  var KEY = "farsight-theme";

  function stored() {
    try {
      return window.localStorage.getItem(KEY);
    } catch (e) {
      return null;
    }
  }

  // Runs before the page paints: a visitor's own choice overrides the
  // default the server wrote into <html data-theme>.
  var chosen = stored();
  if (chosen === "light" || chosen === "dark") {
    root.setAttribute("data-theme", chosen);
  }

  function effective() {
    var t = root.getAttribute("data-theme");
    if (t === "light" || t === "dark") {
      return t;
    }
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  function label(button) {
    button.textContent = effective() === "dark" ? "Light mode" : "Dark mode";
  }

  function unit(n, word) {
    return n + " " + word + (n === 1 ? "" : "s");
  }

  // The server renders absolute UTC times only; a cached page would keep
  // saying "3 seconds ago". The browser can say it truthfully.
  function relative(then, now) {
    var s = Math.round((now - then) / 1000);
    if (s < 0) {
      return null;
    }
    if (s < 90) {
      return unit(s, "second") + " ago";
    }
    if (s < 5400) {
      return unit(Math.round(s / 60), "minute") + " ago";
    }
    if (s < 129600) {
      return unit(Math.round(s / 3600), "hour") + " ago";
    }
    return unit(Math.round(s / 86400), "day") + " ago";
  }

  function annotate(scope) {
    var now = Date.now();
    var times = scope.querySelectorAll("time[datetime]");
    for (var i = 0; i < times.length; i++) {
      var el = times[i];
      if (el.getAttribute("data-rel")) {
        continue;
      }
      var then = Date.parse(el.getAttribute("datetime"));
      if (isNaN(then)) {
        continue;
      }
      var words = relative(then, now);
      if (!words) {
        continue;
      }
      el.setAttribute("data-rel", "1");
      var span = document.createElement("span");
      span.className = "rel";
      span.textContent = " (" + words + ")";
      el.parentNode.insertBefore(span, el.nextSibling);
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    var button = document.getElementById("theme-toggle");
    if (button) {
      button.hidden = false;
      label(button);
      button.addEventListener("click", function () {
        var next = effective() === "dark" ? "light" : "dark";
        root.setAttribute("data-theme", next);
        try {
          window.localStorage.setItem(KEY, next);
        } catch (e) {
          // Private mode: the choice lasts for this page only.
        }
        label(button);
      });
    }
    annotate(document);
  });

  // A section swapped in by htmx carries new times.
  document.addEventListener("htmx:afterSwap", function (event) {
    annotate(event.target && event.target.parentNode ? event.target.parentNode : document);
  });
})();
