// Farsight UI: the theme toggle, times in the visitor's timezone, and
// profile cards. Nothing here is needed to read a page: without it the
// times stay in UTC, the links work and there are no cards. The admin
// pages load the same file; it does nothing where its elements are absent.
(function () {
  "use strict";
  var root = document.documentElement;
  var KEY = "farsight-theme";

  // ---- Theme: light, dark or system ------------------------------------

  function stored() {
    try {
      var v = window.localStorage.getItem(KEY);
      return v === "light" || v === "dark" || v === "system" ? v : null;
    } catch (e) {
      return null;
    }
  }

  // What <html data-theme> must say for a choice: an explicit theme, or
  // nothing, which follows prefers-color-scheme.
  function apply(choice) {
    if (choice === "light" || choice === "dark") {
      root.setAttribute("data-theme", choice);
    } else {
      root.removeAttribute("data-theme");
    }
  }

  // The visitor's choice, or the operator's default the server wrote.
  function current() {
    var s = stored();
    if (s) {
      return s;
    }
    var d = root.getAttribute("data-theme-default");
    return d === "light" || d === "dark" ? d : "system";
  }

  // Runs before the page paints: a stored choice overrides the default
  // in <html data-theme>, and a stored "system" removes it.
  var chosen = stored();
  if (chosen) {
    apply(chosen);
  }

  function mark(buttons, choice) {
    for (var i = 0; i < buttons.length; i++) {
      var on = buttons[i].getAttribute("data-theme-choice") === choice;
      buttons[i].setAttribute("aria-pressed", on ? "true" : "false");
    }
  }

  function themeToggle() {
    var box = document.querySelector(".theme-toggle");
    if (!box) {
      return;
    }
    var buttons = box.querySelectorAll("button[data-theme-choice]");
    var shown = current();
    mark(buttons, shown);
    box.hidden = false;
    box.addEventListener("click", function (event) {
      var b = event.target.closest ? event.target.closest("button[data-theme-choice]") : null;
      if (!b) {
        return;
      }
      var choice = b.getAttribute("data-theme-choice");
      apply(choice);
      try {
        window.localStorage.setItem(KEY, choice);
      } catch (e) {
        // No storage: the choice lasts for this page only.
      }
      mark(buttons, choice);
    });
  }

  // ---- Times -----------------------------------------------------------

  function unit(n, word) {
    return n + " " + word + (n === 1 ? "" : "s");
  }

  // Seconds, minutes, hours or days; null for an instant in the future
  // (author-stated times can be).
  function span(then, now) {
    var s = Math.round((now - then) / 1000);
    if (s < 0) {
      return null;
    }
    if (s < 90) {
      return unit(s, "second");
    }
    if (s < 5400) {
      return unit(Math.round(s / 60), "minute");
    }
    if (s < 129600) {
      return unit(Math.round(s / 3600), "hour");
    }
    return unit(Math.round(s / 86400), "day");
  }

  function two(n) {
    return (n < 10 ? "0" : "") + n;
  }

  // The short zone name the browser gives for that instant: "EDT" in some
  // locales, "GMT-4" in others. Whatever it returns is used.
  function zone(date) {
    var parts = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" }).formatToParts(date);
    for (var i = 0; i < parts.length; i++) {
      if (parts[i].type === "timeZoneName") {
        return parts[i].value;
      }
    }
    return "";
  }

  // YYYY-MM-DD HH:MM:SS TZ, in the visitor's timezone, 24-hour.
  function local(date) {
    var text =
      date.getFullYear() + "-" + two(date.getMonth() + 1) + "-" + two(date.getDate()) + " " +
      two(date.getHours()) + ":" + two(date.getMinutes()) + ":" + two(date.getSeconds());
    var tz = zone(date);
    return tz ? text + " " + tz : text;
  }

  // What a processed element reads at `now`. `abs` is the local instant,
  // written once; only the relative part changes between readings.
  function reading(t, now) {
    var ago = span(t.then, now);
    if (t.updated) {
      // "Last updated": the relative reading alone.
      return ago ? ago + " ago" : t.abs;
    }
    return t.abs + (ago ? " (" + ago + " ago)" : "");
  }

  // The processed elements whose text carries a relative reading.
  var shown = [];

  // Rewrites every <time datetime> not yet processed. The server's text
  // is absolute UTC and stays in `title`. Each element changes in one
  // assignment or not at all: a failure leaves the server's text.
  function times(scope) {
    var now = Date.now();
    var list = scope.querySelectorAll("time[datetime]");
    for (var i = 0; i < list.length; i++) {
      var el = list[i];
      if (el.getAttribute("data-local")) {
        continue;
      }
      try {
        var then = Date.parse(el.getAttribute("datetime"));
        if (isNaN(then)) {
          continue;
        }
        var t = { el: el, then: then, abs: local(new Date(then)), updated: !!el.closest(".updated") };
        // A card states the age on its own line.
        var inCard = !!el.closest(".pc");
        var text = inCard ? t.abs : reading(t, now);
        var utc = el.textContent;
        el.setAttribute("title", utc);
        el.textContent = text;
        el.setAttribute("data-local", "1");
        if (!inCard) {
          shown.push(t);
        }
      } catch (e) {
        // Left as the server wrote it.
      }
    }
  }

  // Once a minute the relative readings are brought up to date; elements
  // that left the page (an htmx swap) are dropped.
  var REFRESH_EVERY = 60000;
  var ticking = null;

  function refresh() {
    var now = Date.now();
    var kept = [];
    for (var i = 0; i < shown.length; i++) {
      var t = shown[i];
      if (!document.contains(t.el)) {
        continue;
      }
      kept.push(t);
      var text = reading(t, now);
      if (t.el.textContent !== text) {
        t.el.textContent = text;
      }
    }
    shown = kept;
  }

  // Runs while the page is visible; a page shown again is refreshed at
  // once.
  function keepTimes() {
    if (document.hidden) {
      clearInterval(ticking);
      ticking = null;
    } else if (!ticking) {
      refresh();
      ticking = setInterval(refresh, REFRESH_EVERY);
    }
  }

  // ---- Profile cards ---------------------------------------------------

  var OPEN_AFTER = 300;
  var canHover = !!(window.matchMedia && window.matchMedia("(hover: hover)").matches);
  var serial = 0;
  var timer = null;

  function linkOf(node) {
    return node && node.closest ? node.closest("a.who[data-card]") : null;
  }

  function wrapOf(node) {
    return node && node.closest ? node.closest(".who-wrap") : null;
  }

  function line(text, className) {
    var p = document.createElement("p");
    if (className) {
      p.className = className;
    }
    p.textContent = text;
    return p;
  }

  // The DID and one line: while loading, and when no card can be shown.
  function placeholder(card, did, words) {
    while (card.firstChild) {
      card.removeChild(card.firstChild);
    }
    var code = document.createElement("code");
    code.className = "pc-did";
    code.textContent = did;
    var p = document.createElement("p");
    p.appendChild(code);
    card.appendChild(p);
    card.appendChild(line(words, "muted"));
  }

  function age(card) {
    var t = card.querySelector(".pc time[datetime]");
    var slots = card.querySelectorAll(".pc-age");
    if (!t || slots.length !== 2) {
      return;
    }
    var then = Date.parse(t.getAttribute("datetime"));
    var words = isNaN(then) ? null : span(then, Date.now());
    if (!words) {
      return;
    }
    slots[1].textContent = words;
    slots[0].hidden = false;
    slots[1].hidden = false;
  }

  // Requests the fragment once per link per page load.
  function load(link, card) {
    var did = link.getAttribute("title") || "";
    placeholder(card, did, "Loading…");
    var failed = function () {
      placeholder(card, did, "Profile not available.");
    };
    // A public card is the same for every caller and is requested without
    // cookies. A link that says so asks with the session cookie: the page
    // that carries it was rendered for a signed-in admin.
    var session = link.hasAttribute("data-card-session");
    fetch(link.getAttribute("data-card"), { credentials: session ? "same-origin" : "omit" })
      .then(function (r) {
        // Only the card itself is shown. Anything else — a refusal, or an
        // answer reached through a redirect, which would be some other
        // page — leaves the placeholder.
        if (r.status !== 200 || r.redirected) {
          throw new Error("card " + r.status);
        }
        return r.text();
      })
      .then(function (html) {
        // The fragment is this origin's own markup, escaped by the server.
        card.innerHTML = html;
        times(card);
        age(card);
        link.setAttribute("aria-describedby", card.id);
      })
      .catch(failed);
  }

  function open(link) {
    var wrap = wrapOf(link);
    if (!wrap) {
      return;
    }
    var card = wrap.querySelector(".profile-card");
    if (!card) {
      card = document.createElement("div");
      card.className = "profile-card";
      card.id = "profile-card-" + ++serial;
      card.setAttribute("role", "tooltip");
      wrap.appendChild(card);
      load(link, card);
    }
    wrap.classList.add("open");
  }

  function close(wrap) {
    if (wrap) {
      wrap.classList.remove("open");
    }
  }

  function closeAll() {
    var shown = document.querySelectorAll(".who-wrap.open");
    for (var i = 0; i < shown.length; i++) {
      close(shown[i]);
    }
  }

  // Delegated, so rows swapped in by htmx need no binding of their own.
  function cards() {
    if (!canHover || !window.fetch) {
      return;
    }
    document.addEventListener("mouseover", function (event) {
      var link = linkOf(event.target);
      if (!link || (event.relatedTarget && link.contains(event.relatedTarget))) {
        return;
      }
      clearTimeout(timer);
      timer = setTimeout(function () {
        open(link);
      }, OPEN_AFTER);
    });
    document.addEventListener("mouseout", function (event) {
      var wrap = wrapOf(event.target);
      if (!wrap) {
        return;
      }
      // Still on the link or on its card: stays open.
      if (event.relatedTarget && wrap.contains(event.relatedTarget)) {
        return;
      }
      clearTimeout(timer);
      var link = wrap.querySelector("a.who");
      if (document.activeElement !== link) {
        close(wrap);
      }
    });
    document.addEventListener("focusin", function (event) {
      var link = linkOf(event.target);
      if (link) {
        open(link);
      }
    });
    document.addEventListener("focusout", function (event) {
      if (linkOf(event.target)) {
        close(wrapOf(event.target));
      }
    });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" || event.key === "Esc") {
        clearTimeout(timer);
        closeAll();
      }
    });
  }

  // ----------------------------------------------------------------------

  document.addEventListener("DOMContentLoaded", function () {
    themeToggle();
    times(document);
    keepTimes();
    document.addEventListener("visibilitychange", keepTimes);
    cards();
  });

  // A section swapped in by htmx carries new times.
  document.addEventListener("htmx:afterSwap", function (event) {
    times(event.target && event.target.parentNode ? event.target.parentNode : document);
  });
})();
