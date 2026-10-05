// Farsight UI: the theme toggle, times in the visitor's timezone, profile
// cards and the "/" search shortcut. Nothing here is needed
// to read a page: without it the times stay in UTC, the links work and
// there are no cards. The admin pages load the same file; it does nothing
// where its elements are absent.
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

  // The public pages carry one toggle; an admin page carries one in its
  // header. Every toggle on the page shows the same choice.
  function themeToggle() {
    var boxes = document.querySelectorAll(".theme-toggle");
    if (!boxes.length) {
      return;
    }
    var all = document.querySelectorAll(".theme-toggle button[data-theme-choice]");
    mark(all, current());
    for (var j = 0; j < boxes.length; j++) {
      boxes[j].hidden = false;
      boxes[j].addEventListener("click", function (event) {
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
        mark(all, choice);
      });
    }
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

  // YYYY-MM-DD HH:MM:SS TZ, in the visitor's timezone, 24-hour. `bare`:
  // without the zone, for a table whose page names the zone once.
  function local(date, bare) {
    var text =
      date.getFullYear() + "-" + two(date.getMonth() + 1) + "-" + two(date.getDate()) + " " +
      two(date.getHours()) + ":" + two(date.getMinutes()) + ":" + two(date.getSeconds());
    var tz = bare ? "" : zone(date);
    return tz ? text + " " + tz : text;
  }

  // "All times are in …": the server says UTC; once the row times read in
  // the visitor's timezone the line names that timezone.
  function zoneNote() {
    var list = document.querySelectorAll("[data-zone]");
    if (!list.length) {
      return;
    }
    // The zone's short name today ("EDT"; in locales without one,
    // "GMT-4"); the region's name if the browser gives no short one.
    var name = zone(new Date());
    if (!name) {
      try {
        name = (new Intl.DateTimeFormat().resolvedOptions().timeZone || "").replace(/_/g, " ");
      } catch (e) {
        name = "";
      }
    }
    for (var i = 0; i < list.length; i++) {
      if (name) {
        list[i].textContent = name;
      }
    }
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
        // A time marked data-abs is a table row's: the instant alone,
        // without the zone the page states once.
        var bare = el.hasAttribute("data-abs") && !el.closest(".pc");
        var t = { el: el, then: then, abs: local(new Date(then), bare), updated: !!el.closest(".updated") };
        // A card states the age on its own line.
        var fixed = !!el.closest(".pc") || el.hasAttribute("data-abs");
        var text = fixed ? t.abs : reading(t, now);
        var utc = bare ? el.textContent + " UTC" : el.textContent;
        el.setAttribute("title", utc);
        el.textContent = text;
        el.setAttribute("data-local", "1");
        if (!fixed) {
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
    var words = isNaN(then) ? null : since(new Date(then), new Date());
    if (!words) {
      return;
    }
    slots[1].textContent = words;
    slots[0].hidden = false;
    slots[1].hidden = false;
  }

  // Requests the fragment once per link per page load.
  function load(link, card) {
    var did = link.getAttribute("title") || link.getAttribute("data-did") || "";
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
    // The card states the DID: while it is open the link's own tooltip
    // (its title, the DID again) is put aside, or the browser draws it
    // over the card.
    if (link.hasAttribute("title")) {
      link.setAttribute("data-did", link.getAttribute("title"));
      link.removeAttribute("title");
    }
    wrap.classList.add("open");
  }

  function close(wrap) {
    if (wrap) {
      wrap.classList.remove("open");
      var link = wrap.querySelector("a.who");
      if (link && !link.hasAttribute("title") && link.hasAttribute("data-did")) {
        link.setAttribute("title", link.getAttribute("data-did"));
      }
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

  // ---- Micro-interactions: '/' key to focus search --------------------

  function shortcuts() {
    document.addEventListener("keydown", function (event) {
      if (event.key === "/" && !event.ctrlKey && !event.metaKey && !event.altKey) {
        var active = document.activeElement;
        var isInput = active && (active.tagName === "INPUT" || active.tagName === "TEXTAREA" || active.isContentEditable);
        if (!isInput) {
          // The page's own search box where it has one, else the bar's.
          var searchInput =
            document.querySelector("form.home-search input") ||
            document.querySelector("nav.public-nav form.search input");
          if (searchInput) {
            event.preventDefault();
            searchInput.focus();
            if (searchInput.select) {
              searchInput.select();
            }
          }
        }
      } else if (event.key === "Escape" || event.key === "Esc") {
        var activeInput = document.activeElement;
        if (activeInput && (activeInput.tagName === "INPUT" || activeInput.tagName === "TEXTAREA")) {
          activeInput.blur();
        }
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
    shortcuts();
    zoneNote();
    tabs();
    pending();
    swaps();
    pagers();
    finds();
    heroAvatar();
    listImage();
    guide();
    fitPagers();
    var refit = null;
    window.addEventListener("resize", function () {
      clearTimeout(refit);
      refit = setTimeout(fitPagers, 120);
    });
  });

  // Tabs of a data page: each brings one table into view. The link is a
  // real address (the server marks the table for a browser without this
  // script); here it switches in place and the address follows.
  function tabs() {
    var box = document.querySelector(".tabbed");
    var nav = document.querySelector("nav.tabs");
    if (!box || !nav) {
      return;
    }
    function show(id) {
      box.setAttribute("data-active", id);
      fitPagers();
      var all = nav.querySelectorAll("a.tab");
      for (var i = 0; i < all.length; i++) {
        var on = all[i].getAttribute("data-tab") === id;
        all[i].classList.toggle("active", on);
        all[i].setAttribute("aria-selected", on ? "true" : "false");
      }
    }
    nav.addEventListener("click", function (event) {
      var a = event.target.closest ? event.target.closest("a.tab") : null;
      if (!a || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button) {
        return;
      }
      // A tab whose table is not in the page (History, which the server
      // reads only when asked) is followed as the link it is.
      if (!document.getElementById(a.getAttribute("data-tab"))) {
        return;
      }
      event.preventDefault();
      show(a.getAttribute("data-tab"));
      try {
        history.replaceState(null, "", a.getAttribute("href"));
      } catch (e) {
        // The table is in view either way.
      }
    });
    // An address that points into a table (#lists) shows that table.
    var hash = location.hash.slice(1);
    if (/^[a-z]+$/.test(hash) && nav.querySelector('a.tab[data-tab="' + hash + '"]')) {
      show(hash);
    }
  }

  // Page controls list a long run of pages around the current one. Each
  // set is cut to what its row holds: the numbers farthest from the
  // current page go first, the first and last page stay, and a gap is
  // written wherever numbers are missing. A table behind a tab has no
  // width yet; it is fitted when its tab is shown.
  function fitPager(nav) {
    if (!nav.clientWidth) {
      return;
    }
    var current = nav.querySelector("[aria-current]");
    var here = current ? Number(current.getAttribute("data-p")) : 1;
    var pages = [].slice.call(nav.querySelectorAll("[data-p]"));
    var last = nav.lastElementChild;
    var far = 0;
    for (var i = 0; i < pages.length; i++) {
      if (!pages[i].hasAttribute("data-keep")) {
        far = Math.max(far, Math.abs(Number(pages[i].getAttribute("data-p")) - here));
      }
    }
    function gap() {
      var s = document.createElement("span");
      s.className = "pager-gap";
      s.setAttribute("aria-hidden", "true");
      s.textContent = "\u2026";
      return s;
    }
    function show(reach) {
      var gaps = nav.querySelectorAll(".pager-gap");
      for (var g = 0; g < gaps.length; g++) {
        nav.removeChild(gaps[g]);
      }
      var before = 0;
      for (var j = 0; j < pages.length; j++) {
        var n = Number(pages[j].getAttribute("data-p"));
        var keep = pages[j].hasAttribute("data-keep") || Math.abs(n - here) <= reach;
        pages[j].hidden = !keep;
        if (!keep) {
          continue;
        }
        if (n > before + 1) {
          nav.insertBefore(gap(), pages[j]);
        }
        before = n;
      }
      if (nav.hasAttribute("data-open")) {
        nav.insertBefore(gap(), last);
      }
    }
    nav.classList.add("fit");
    for (var reach = far; reach >= 0; reach--) {
      show(reach);
      if (nav.scrollWidth <= nav.clientWidth + 1) {
        break;
      }
    }
  }

  function fitPagers() {
    var list = document.querySelectorAll("nav.pager");
    for (var i = 0; i < list.length; i++) {
      fitPager(list[i]);
    }
  }

  // How long ago `then` was, in calendar terms, as its two largest
  // units: "1 year, 11 months", "5 months, 12 days", "9 days". Null for
  // a date that is not in the past.
  function since(then, now) {
    if (isNaN(then.getTime()) || then > now) {
      return null;
    }
    var years = now.getFullYear() - then.getFullYear();
    var months = now.getMonth() - then.getMonth();
    var days = now.getDate() - then.getDate();
    if (days < 0) {
      months -= 1;
      // The days of the month before this one.
      days += new Date(now.getFullYear(), now.getMonth(), 0).getDate();
    }
    if (months < 0) {
      years -= 1;
      months += 12;
    }
    function unit(n, word) {
      return n + " " + word + (n === 1 ? "" : "s");
    }
    var parts = [];
    if (years > 0) {
      parts.push(unit(years, "year"));
      if (months > 0) {
        parts.push(unit(months, "month"));
      }
    } else if (months > 0) {
      parts.push(unit(months, "month"));
      if (days > 0) {
        parts.push(unit(days, "day"));
      }
    } else {
      parts.push(days > 0 ? unit(days, "day") : "less than a day");
    }
    return parts.join(", ");
  }

  // Under the DID in the account page's header: when the DID was created
  // and which host holds the account, as the profile card knows them.
  function heroFacts(card) {
    var line = document.querySelector("[data-hero-facts]");
    if (!line) {
      return;
    }
    var made = card.querySelector(".pc-facts time[datetime]");
    var pc = card.querySelector(".pc");
    var host = pc ? pc.getAttribute("data-pds") : null;
    if (made) {
      line.appendChild(document.createTextNode("Created "));
      var t = document.createElement("time");
      t.setAttribute("datetime", made.getAttribute("datetime"));
      t.setAttribute("data-abs", "");
      t.textContent = made.textContent.replace(/ UTC$/, "");
      line.appendChild(t);
      var old = since(new Date(Date.parse(made.getAttribute("datetime"))), new Date());
      if (old) {
        line.appendChild(document.createTextNode(" (" + old + " ago)"));
      }
    }
    if (host) {
      line.appendChild(document.createTextNode(made ? " \u00b7 Hosted on " : "Hosted on "));
      var h = document.createElement("span");
      h.className = "host";
      h.textContent = host;
      line.appendChild(h);
    }
    if (made || host) {
      line.hidden = false;
      times(line);
    }
  }

  // The account page's header shows the account's avatar. The server
  // does not know it; the profile card does (and says nothing where
  // avatars are switched off), so the image is taken from the card.
  function heroAvatar() {
    var row = document.querySelector("[data-hero-card]");
    if (!row || !window.fetch) {
      return;
    }
    fetch(row.getAttribute("data-hero-card"), { credentials: "same-origin" })
      .then(function (r) {
        if (r.status !== 200 || r.redirected) {
          throw new Error("no card");
        }
        return r.text();
      })
      .then(function (html) {
        var card = new DOMParser().parseFromString(html, "text/html");
        heroFacts(card);
        var found = card.querySelector("img.pc-avatar");
        if (!found) {
          return;
        }
        var img = document.createElement("img");
        img.className = "hero-avatar";
        img.alt = "";
        img.width = 88;
        img.height = 88;
        img.referrerPolicy = "no-referrer";
        img.onerror = function () {
          if (img.parentNode) {
            img.parentNode.removeChild(img);
          }
        };
        img.src = found.getAttribute("src");
        row.insertBefore(img, row.firstChild);
      })
      .catch(function () {
        // No avatar: the header is complete without one.
      });
  }

  // The guide in the bar closes when something outside it is clicked
  // or Escape is pressed. Its heading opens and closes it without this.
  function guide() {
    var box = document.querySelector("details.nav-guide");
    if (!box) {
      return;
    }
    document.addEventListener("click", function (event) {
      if (box.open && !box.contains(event.target)) {
        box.open = false;
      }
    });
    document.addEventListener("keydown", function (event) {
      if (box.open && (event.key === "Escape" || event.key === "Esc")) {
        box.open = false;
        box.querySelector("summary").focus();
      }
    });
  }

  // The list page's header shows the list's image. The server stores
  // which image (its CID) and never fetches it; the owner's profile card
  // says which host holds the owner's data, and the image is named there.
  function listImage() {
    var row = document.querySelector("[data-list-image]");
    if (!row || !window.fetch) {
      return;
    }
    var cid = row.getAttribute("data-list-image");
    var owner = row.getAttribute("data-owner");
    if (!/^b[a-z2-7]{7,127}$/.test(cid) || !owner) {
      return;
    }
    var place = function (src) {
      var img = document.createElement("img");
      img.className = "hero-avatar";
      img.alt = "";
      img.width = 88;
      img.height = 88;
      img.referrerPolicy = "no-referrer";
      img.onerror = function () {
        if (img.parentNode) {
          img.parentNode.removeChild(img);
        }
      };
      img.src = src;
      row.insertBefore(img, row.firstChild);
    };
    // The server names the image itself where it is a thumbnail on the
    // image service; otherwise the owner's card says which host has it.
    var named = row.getAttribute("data-list-image-src");
    if (named) {
      place(named);
      return;
    }
    fetch(row.getAttribute("data-owner-card"), { credentials: "same-origin" })
      .then(function (r) {
        if (r.status !== 200 || r.redirected) {
          throw new Error("no card");
        }
        return r.text();
      })
      .then(function (html) {
        var card = new DOMParser().parseFromString(html, "text/html");
        var pc = card.querySelector(".pc[data-pds]");
        var host = pc && pc.getAttribute("data-pds");
        if (!host || !/^[a-z0-9.-]+(:[0-9]+)?$/i.test(host)) {
          return;
        }
        place(
          "https://" +
            host +
            "/xrpc/com.atproto.sync.getBlob?did=" +
            encodeURIComponent(owner) +
            "&cid=" +
            encodeURIComponent(cid)
        );
      })
      .catch(function () {
        // No image: the header is complete without one.
      });
  }

  // Puts the tabs and tables of the page at `href` in place of the ones
  // shown, without a page load, and shows that address. `done` runs once
  // they are in place; `fallback` if the page could not be read.
  var swapRun = 0;
  function swapTo(href, done, fallback, push) {
    var box = document.querySelector(".tabbed");
    if (!box || !window.fetch) {
      fallback();
      return;
    }
    var run = ++swapRun;
    fetch(href, { credentials: "same-origin", cache: "no-store" })
      .then(function (r) {
        if (r.status !== 200) {
          throw new Error("not the page");
        }
        // A table that got shorter answers with its last page: the
        // address shown is the one the server settled on.
        if (r.redirected) {
          var to = new URL(r.url);
          if (to.origin !== location.origin) {
            throw new Error("not the page");
          }
          var hash = href.indexOf("#");
          href = to.pathname + to.search + (hash < 0 ? "" : href.slice(hash));
        }
        return r.text();
      })
      .then(function (html) {
        // An answer overtaken by a later request is dropped.
        if (run !== swapRun) {
          return;
        }
        var doc = new DOMParser().parseFromString(html, "text/html");
        var fresh = doc.querySelector(".tabbed");
        if (!fresh) {
          throw new Error("not the page");
        }
        box.innerHTML = fresh.innerHTML;
        box.setAttribute("data-active", fresh.getAttribute("data-active"));
        var nav = document.querySelector("nav.tabs");
        var freshNav = doc.querySelector("nav.tabs");
        if (nav && freshNav) {
          nav.innerHTML = freshNav.innerHTML;
        }
        times(box);
        zoneNote();
        fitPagers();
        try {
          var shown = href.replace(/([?&])go=1(&|$)/, "$1").replace(/[?&]$/, "");
          // A turned page is a step Back returns from; a filter is not.
          if (push) {
            history.pushState({ swapped: true }, "", shown);
          } else {
            history.replaceState(history.state, "", shown);
          }
        } catch (e) {
          // The tables are in place either way.
        }
        pending();
        if (done) {
          done();
        }
        release(box);
      })
      .catch(function () {
        if (run === swapRun) {
          fallback();
        }
      });
  }

  // A turned page can come back shorter than the one before it: rows
  // whose accounts are still being checked are left out and arrive a
  // moment later. A shorter page would pull the window up, and each
  // arrival would push it down again. The tables' box keeps the height
  // it had until its rows are all there and letting go moves nothing.
  function hold(box) {
    if (box) {
      // As a flow root the box contains its last table's bottom margin,
      // which otherwise reaches outside it and is lost when the box is
      // taller than its content.
      box.style.display = "flow-root";
      box.style.minHeight = box.offsetHeight + "px";
    }
  }
  function release(box) {
    if (!box || !box.style.minHeight || box.querySelector("[data-pending]")) {
      return;
    }
    var kept = box.style.minHeight;
    box.style.minHeight = "";
    var page = document.documentElement;
    if (page.scrollHeight < window.scrollY + window.innerHeight - 1) {
      // The window is below where the page would now end: keep the room.
      box.style.minHeight = kept;
    } else {
      box.style.display = "";
    }
  }

  // Page controls turn the table where it stands. Followed as links they
  // load the whole page and the browser then scrolls to the table, which
  // shows as a jump; here the tables are replaced in place and the window
  // stays where it is. Without this script the links are followed.
  function pagers() {
    document.addEventListener("click", function (event) {
      var a = event.target.closest ? event.target.closest("nav.pager a[href]") : null;
      if (!a || !window.fetch || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button) {
        return;
      }
      var section = a.closest(".tabbed > section");
      if (!section) {
        return;
      }
      event.preventDefault();
      var href = a.getAttribute("href");
      // Which of the table's two sets of controls was used, to put the
      // keyboard's place back in it afterwards.
      var navs = section.querySelectorAll("nav.pager");
      var which = navs.length > 1 && navs[navs.length - 1].contains(a) ? navs.length - 1 : 0;
      var id = section.id;
      var x = window.scrollX;
      var y = window.scrollY;
      section.setAttribute("aria-busy", "true");
      hold(document.querySelector(".tabbed"));
      swapTo(
        href,
        function () {
          // If the page is still too short for where the window was (a
          // margin that no longer collapses is enough), give the tables'
          // box the difference.
          var box = document.querySelector(".tabbed");
          var short = y + window.innerHeight - document.documentElement.scrollHeight;
          if (short > 0 && box && box.style.minHeight) {
            box.style.minHeight = parseFloat(box.style.minHeight) + short + "px";
          }
          window.scrollTo(x, y);
          var again = document.getElementById(id);
          var controls = again ? again.querySelectorAll("nav.pager") : [];
          var here = controls[Math.min(which, controls.length - 1)];
          var current = here ? here.querySelector(".current") : null;
          if (current) {
            current.setAttribute("tabindex", "-1");
            try {
              current.focus({ preventScroll: true });
            } catch (e) {
              // Focus is a convenience; the page is in place.
            }
          }
        },
        function () {
          location.href = href;
        },
        true
      );
    });
    // Back and Forward over pages turned this way.
    window.addEventListener("popstate", function () {
      if (!document.querySelector(".tabbed")) {
        return;
      }
      var href = location.pathname + location.search + location.hash;
      var y = window.scrollY;
      swapTo(
        href,
        function () {
          window.scrollTo(window.scrollX, y);
        },
        function () {
          location.reload();
        }
      );
    });
  }

  // A link marked data-swap (the "Show taken down accounts" switch) changes
  // what the tables hold. Without this script the link is followed.
  function swaps() {
    document.addEventListener("click", function (event) {
      var a = event.target.closest ? event.target.closest("a[data-swap]") : null;
      if (!a || !window.fetch || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button) {
        return;
      }
      if (!document.querySelector(".tabbed")) {
        return;
      }
      event.preventDefault();
      var href = a.getAttribute("href");
      swapTo(href, null, function () {
        location.href = href;
      });
    });
  }

  // The filter box of a table: what is typed filters the page's tables
  // in place, a moment after the typing stops; Enter also looks up a
  // whole handle. Without this script the form is sent as it is.
  function finds() {
    var timer = null;
    function send(form, go) {
      var input = form.querySelector('input[name="find"]');
      var typed = input.value;
      var pairs = [];
      var fields = form.querySelectorAll("input[name]");
      for (var i = 0; i < fields.length; i++) {
        var v = fields[i].name === "find" ? typed.trim() : fields[i].value;
        if (v) {
          pairs.push(encodeURIComponent(fields[i].name) + "=" + encodeURIComponent(v));
        }
      }
      if (go && typed.trim()) {
        pairs.push("go=1");
      }
      var action = form.getAttribute("action");
      var hash = action.indexOf("#");
      var path = hash < 0 ? action : action.slice(0, hash);
      var section = hash < 0 ? "" : action.slice(hash + 1);
      var href = path + (pairs.length ? "?" + pairs.join("&") : "") + (hash < 0 ? "" : action.slice(hash));
      swapTo(
        href,
        function () {
          // The box was replaced with the page's: keep what the visitor
          // has typed since, and the caret.
          var again = document.querySelector("#" + section + ' form.find input[name="find"]');
          if (again) {
            var now = lastTyped === null ? typed : lastTyped;
            again.value = now;
            again.focus();
            try {
              again.setSelectionRange(now.length, now.length);
            } catch (e) {
              // Not every input type has a caret to set.
            }
          }
        },
        function () {
          location.href = href;
        }
      );
    }
    var lastTyped = null;
    document.addEventListener("input", function (event) {
      var input = event.target;
      if (!input || !input.matches || !input.matches('form.find input[name="find"]')) {
        return;
      }
      lastTyped = input.value;
      clearTimeout(timer);
      var form = input.form;
      timer = setTimeout(function () {
        send(form, false);
      }, 350);
    });
    document.addEventListener("submit", function (event) {
      var form = event.target;
      if (!form || !form.matches || !form.matches("form.find") || !window.fetch) {
        return;
      }
      event.preventDefault();
      clearTimeout(timer);
      lastTyped = form.querySelector('input[name="find"]').value;
      send(form, true);
    });
  }

  // Rows held back until their account's handle is checked (a table says
  // how many in [data-pending]): read the page again until they are
  // there, and put each table that was waiting in place. Quick at first,
  // then every three seconds, for about a minute; after that the line
  // stays and a reload shows the rest.
  function pending() {
    // One reader at a time: a call made after the tables changed (the
    // taken-down switch) retires the reader before it.
    var run = ++pendingRun;
    var tries = 0;
    function again() {
      if (run !== pendingRun || tries >= 20 || !document.querySelector("section[id] [data-pending]")) {
        return;
      }
      tries++;
      setTimeout(function () {
        if (run !== pendingRun) {
          return;
        }
        var asked = location.href;
        fetch(asked, { credentials: "same-origin", cache: "no-store" })
          .then(function (r) {
            if (r.status !== 200 || r.redirected) {
              throw new Error("not the page");
            }
            return r.text();
          })
          .then(function (html) {
            // An answer for an address the page has since left (the
            // switch was used meanwhile) is not put in place.
            if (run !== pendingRun || location.href !== asked) {
              again();
              return;
            }
            var doc = new DOMParser().parseFromString(html, "text/html");
            var sections = document.querySelectorAll("section[id]");
            for (var i = 0; i < sections.length; i++) {
              var here = sections[i];
              var fresh = doc.getElementById(here.id);
              if (here.querySelector("[data-pending]") && fresh && fresh.tagName === "SECTION") {
                here.innerHTML = fresh.innerHTML;
                times(here);
                zoneNote();
                fitPagers();
              }
            }
            again();
          })
          .catch(again);
      }, tries <= 4 ? 1200 : 3000);
    }
    again();
  }
  var pendingRun = 0;

  // A section swapped in by htmx carries new times.
  document.addEventListener("htmx:afterSwap", function (event) {
    times(event.target && event.target.parentNode ? event.target.parentNode : document);
  });
})();
