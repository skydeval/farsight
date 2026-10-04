// Browser probes of the stage-6 harness (Mode A): the parts of the public
// UI that only a browser can show — the theme toggle and its persistence,
// times rewritten in the visitor's timezone, the sticky bar, profile cards
// on hover and focus, and their absence on a touch device.
//
// Run by `farsight-stage6-harness --browser` inside the Playwright image;
// prints one JSON object per line: {"what", "ok", "detail"}.
//
//   node stage6-browser-probes.mjs <base> <account path> <operator default>
//        [<path of a page with a live row> <live DID> <live handle>]
import { chromium } from "playwright";

const [base, accountPath, operatorDefault, livePath, liveDid, liveHandle] = process.argv.slice(2);
const DARK_BG = "rgb(9, 12, 21)";
const LIGHT_BG = "rgb(248, 250, 252)";
const ZONE = "America/New_York";

function out(what, ok, detail = "") {
  console.log(JSON.stringify({ what, ok: !!ok, detail: String(detail).slice(0, 500) }));
}

async function probe(what, f) {
  try {
    const r = await f();
    out(what, r.ok, r.detail);
  } catch (e) {
    out(what, false, `threw: ${e.message}`);
  }
}

const bg = (page) => page.evaluate(() => getComputedStyle(document.body).backgroundColor);
const theme = (page) =>
  page.evaluate(() => ({
    attr: document.documentElement.getAttribute("data-theme"),
    stored: localStorage.getItem("farsight-theme"),
    pressed: [...document.querySelectorAll(".theme-toggle button[aria-pressed='true']")].map((b) =>
      b.getAttribute("data-theme-choice"),
    ),
  }));

// "YYYY-MM-DD HH:MM:SS" of an instant in ZONE, computed outside the page.
function wallClock(iso) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date(iso));
  const p = Object.fromEntries(parts.map((x) => [x.type, x.value]));
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

const browser = await chromium.launch();
const problems = [];

function watch(page) {
  page.on("console", (m) => {
    if (m.type() === "error") {
      problems.push(m.text());
    }
  });
  page.on("pageerror", (e) => problems.push(String(e)));
}

// ---- A visitor with a pointer, in New York, light system theme -----------
const ctx = await browser.newContext({
  colorScheme: "light",
  timezoneId: ZONE,
  locale: "en-US",
  viewport: { width: 1200, height: 700 },
});
const page = await ctx.newPage();
watch(page);
await page.goto(base + accountPath);

await probe(
  `a first visit gets the operator's default theme (${operatorDefault}), with nothing stored`,
  async () => {
    const t = await theme(page);
    const color = await bg(page);
    const want = operatorDefault === "dark" ? DARK_BG : LIGHT_BG;
    return {
      ok:
        t.stored === null &&
        t.attr === (operatorDefault === "system" ? null : operatorDefault) &&
        color === want &&
        t.pressed.join() === operatorDefault,
      detail: `${JSON.stringify(t)} background ${color}`,
    };
  },
);

await probe("the bar is sticky, holds the search form and the three-state toggle, and no link leaves the public UI", async () => {
  const d = await page.evaluate(() => {
    const nav = document.querySelector("nav.public-nav");
    const form = nav.querySelector("form");
    const input = form.querySelector("input[name=q]");
    window.scrollTo(0, document.body.scrollHeight);
    const hrefs = [...document.querySelectorAll("a[href]")].map((a) => a.getAttribute("href"));
    return {
      position: getComputedStyle(nav).position,
      top: nav.getBoundingClientRect().top,
      scrolled: window.scrollY,
      action: form.getAttribute("action"),
      method: (form.getAttribute("method") || "").toLowerCase(),
      value: input.value,
      buttons: [...nav.querySelectorAll(".theme-toggle button")].map((b) => b.getAttribute("data-theme-choice")),
      toggleShown: !nav.querySelector(".theme-toggle").hidden,
      // Links that leave the public UI: anything but its pages at the root and anchors.
      out: hrefs.filter((h) => !(h === "/" || ["/search", "/did/", "/list/", "#"].some((p) => h.startsWith(p)))),
      text: nav.textContent,
    };
  });
  return {
    ok:
      d.position === "sticky" &&
      d.top === 0 &&
      d.scrolled > 0 &&
      d.action === "/search" &&
      d.method === "get" &&
      d.value === "" &&
      d.buttons.join() === "light,dark,system" &&
      d.toggleShown &&
      d.out.length === 0 &&
      !/log ?in|dashboard|settings/i.test(d.text),
    detail: JSON.stringify({ ...d, text: undefined }),
  };
});

await probe("the toggle switches light / dark / system, stores the choice under farsight-theme, and a reload keeps it", async () => {
  const steps = [];
  await page.click(".theme-toggle button[data-theme-choice=light]");
  steps.push([await theme(page), await bg(page)]);
  await page.reload();
  steps.push([await theme(page), await bg(page)]);
  await page.click(".theme-toggle button[data-theme-choice=dark]");
  steps.push([await theme(page), await bg(page)]);
  await page.reload();
  steps.push([await theme(page), await bg(page)]);
  const ok =
    steps[0][0].attr === "light" && steps[0][0].stored === "light" && steps[0][1] === LIGHT_BG &&
    steps[1][0].attr === "light" && steps[1][0].stored === "light" && steps[1][1] === LIGHT_BG &&
    steps[1][0].pressed.join() === "light" &&
    steps[2][0].attr === "dark" && steps[2][0].stored === "dark" && steps[2][1] === DARK_BG &&
    steps[3][0].attr === "dark" && steps[3][1] === DARK_BG && steps[3][0].pressed.join() === "dark";
  return { ok, detail: JSON.stringify(steps) };
});

await probe("a stored \"system\" overrides the operator's default and follows prefers-color-scheme", async () => {
  await page.click(".theme-toggle button[data-theme-choice=system]");
  const a = [await theme(page), await bg(page)];
  await page.reload();
  const b = [await theme(page), await bg(page)];
  await page.emulateMedia({ colorScheme: "dark" });
  const c = await bg(page);
  await page.emulateMedia({ colorScheme: "light" });
  return {
    ok:
      a[0].attr === null && a[0].stored === "system" && a[1] === LIGHT_BG &&
      b[0].attr === null && b[0].stored === "system" && b[1] === LIGHT_BG &&
      b[0].pressed.join() === "system" && c === DARK_BG,
    detail: JSON.stringify([a, b, c]),
  };
});

await probe("every <time> is rewritten once, in the visitor's timezone, with the UTC text kept in title", async () => {
  const times = await page.evaluate(() =>
    [...document.querySelectorAll("time[datetime]")].map((t) => ({
      iso: t.getAttribute("datetime"),
      text: t.textContent,
      title: t.getAttribute("title"),
      done: t.getAttribute("data-local"),
      updated: !!t.closest(".updated"),
    })),
  );
  const bad = [];
  let rows = 0;
  let footer = 0;
  for (const t of times) {
    const utc = /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC$/.test(t.title || "");
    if (t.updated) {
      footer += 1;
      if (!(utc && t.done === "1" && /^\d+ (second|minute|hour|day)s? ago$/.test(t.text))) {
        bad.push(t);
      }
      continue;
    }
    rows += 1;
    const m = /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) (\S+)( \(\d+ (second|minute|hour|day)s? ago\))?$/.exec(t.text);
    if (!(utc && t.done === "1" && m && m[1] === wallClock(t.iso) && /^(EDT|EST|GMT-[45])$/.test(m[2]))) {
      bad.push(t);
    }
  }
  return {
    ok: bad.length === 0 && rows >= 10 && footer === 1,
    detail: `${rows} row times, ${footer} footer; first: ${JSON.stringify(times[0])}; bad: ${JSON.stringify(bad.slice(0, 2))}`,
  };
});

// Slow (over a minute), so it waits on a page of its own while the probes
// below run. A <time> of ten seconds ago is added and announced the way an
// htmx swap is, so the outcome does not depend on the age of the seeded rows.
const refreshed = (async () => {
  const p = await ctx.newPage();
  watch(p);
  await p.goto(base + accountPath);
  const read = () =>
    p.evaluate(() => {
      const t = document.querySelector("#refresh-probe time");
      const f = document.querySelector(".updated time");
      return { text: t.textContent, title: t.getAttribute("title"), footer: f.textContent, footerIso: f.getAttribute("datetime") };
    });
  await p.evaluate(() => {
    const box = document.createElement("p");
    box.id = "refresh-probe";
    const t = document.createElement("time");
    t.setAttribute("datetime", new Date(Date.now() - 10000).toISOString());
    t.textContent = "probe UTC";
    box.appendChild(t);
    document.body.appendChild(box);
    t.dispatchEvent(new CustomEvent("htmx:afterSwap", { bubbles: true }));
  });
  const before = await read();
  await p.waitForTimeout(65000);
  const after = await read();
  await p.close();
  return { before, after };
})();

// ---- Profile cards -------------------------------------------------------
await probe("a card opens after the pointer rests on a row's link, is requested once, and closes on leave and on Escape", async () => {
  await page.goto(base + accountPath);
  const requests = [];
  page.on("request", (r) => {
    if (r.url().includes("/card/")) {
      requests.push(r.url());
    }
  });
  const link = page.locator("#blockers a.who").first();
  const did = await link.getAttribute("title");
  await link.hover();
  await page.waitForTimeout(120);
  const early = requests.length;
  const card = page.locator("#blockers .who-wrap").first().locator(".profile-card");
  await card.waitFor({ state: "visible", timeout: 5000 });
  await page.waitForResponse((r) => r.url().includes("/card/"), { timeout: 8000 }).catch(() => null);
  await page.waitForTimeout(300);
  const text = await card.innerText();
  const role = await card.getAttribute("role");
  await page.mouse.move(600, 650);
  await page.waitForTimeout(150);
  const hiddenAfterLeave = !(await card.isVisible());
  await link.hover();
  await card.waitFor({ state: "visible", timeout: 5000 });
  const afterSecondHover = requests.length;
  await page.mouse.move(600, 650);
  // Keyboard: focus opens at once, Escape closes, the link still navigates.
  await link.focus();
  const onFocus = await card.isVisible();
  await page.keyboard.press("Escape");
  const afterEscape = !(await card.isVisible());
  const href = await link.getAttribute("href");
  return {
    ok:
      early === 0 &&
      requests.length === 1 &&
      afterSecondHover === 1 &&
      requests[0].endsWith("/card/" + did) &&
      text.includes(did) &&
      role === "tooltip" &&
      hiddenAfterLeave &&
      onFocus &&
      afterEscape &&
      href === "/did/" + did,
    detail: JSON.stringify({ early, requests: requests.length, afterSecondHover, hiddenAfterLeave, onFocus, afterEscape, text: text.slice(0, 120) }),
  };
});

if (livePath) {
  await probe("a real account's card shows its avatar (fetched by the browser from the account's PDS), handle, DID, creation date and age", async () => {
    await page.goto(base + livePath);
    const link = page.locator(`a.who[title="${liveDid}"]`).first();
    await link.hover();
    const card = page.locator(".who-wrap", { has: link }).first().locator(".profile-card");
    await card.waitFor({ state: "visible", timeout: 5000 });
    await card.locator(".pc").waitFor({ state: "visible", timeout: 10000 });
    const img = card.locator("img.pc-avatar");
    const hasImg = (await img.count()) === 1;
    let image = {};
    if (hasImg) {
      await img.evaluate((i) => (i.complete ? null : new Promise((r) => { i.onload = r; i.onerror = r; })));
      image = await img.evaluate((i) => ({ src: i.src, w: i.naturalWidth, ref: i.referrerPolicy }));
    }
    const d = await card.evaluate((c) => ({
      handle: c.querySelector(".handle")?.textContent,
      did: c.querySelector(".pc-did")?.textContent,
      time: c.querySelector("time")?.textContent,
      title: c.querySelector("time")?.getAttribute("title"),
      age: [...c.querySelectorAll(".pc-age")].map((e) => [e.hidden, e.textContent]),
      focusable: c.querySelectorAll("a, button, input, [tabindex]").length,
    }));
    const described = await link.getAttribute("aria-describedby");
    const id = await card.getAttribute("id");
    return {
      ok:
        hasImg &&
        image.w > 0 &&
        new URL(image.src).origin !== new URL(base).origin &&
        image.src.includes("/xrpc/com.atproto.sync.getBlob?did=") &&
        image.ref === "no-referrer" &&
        d.handle === "@" + liveHandle &&
        d.did === liveDid &&
        /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d \S+$/.test(d.time || "") &&
        / UTC$/.test(d.title || "") &&
        d.age.length === 2 &&
        d.age.every((a) => a[0] === false) &&
        /^\d+ days$/.test(d.age[1][1]) &&
        d.focusable === 0 &&
        described === id,
      detail: JSON.stringify({ image, ...d, described, id }),
    };
  });
}
await probe("after 65 seconds on an open page the relative part of a <time> has moved on; the absolute part and title have not", async () => {
  const { before, after } = await refreshed;
  const row = /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d \S+) \((\d+) seconds ago\)$/;
  const a = row.exec(before.text);
  const b = row.exec(after.text);
  const moved = a && b ? Number(b[2]) - Number(a[2]) : null;
  // The footer reads in minutes up to 90 minutes, and must have moved too.
  const footerAge = Date.now() - Date.parse(after.footerIso);
  const footerOk =
    /^\d+ (second|minute|hour|day)s? ago$/.test(after.footer) &&
    (footerAge > 5400000 || after.footer !== before.footer);
  return {
    ok: !!a && !!b && a[1] === b[1] && moved >= 60 && moved <= 70 && before.title === "probe UTC" && after.title === "probe UTC" && footerOk,
    detail: JSON.stringify({ before, after }),
  };
});
await ctx.close();

// ---- Without script ------------------------------------------------------
await probe("without JavaScript the page reads the same: UTC times, working links and search, no toggle, no card", async () => {
  const c = await browser.newContext({ javaScriptEnabled: false, timezoneId: ZONE });
  const p = await c.newPage();
  await p.goto(base + accountPath);
  const texts = await p.locator("time[datetime]").allInnerTexts();
  const toggle = await p.locator(".theme-toggle").isVisible();
  const links = await p.locator("#blockers a.who").count();
  const action = await p.locator("nav.public-nav form").getAttribute("action");
  await p.locator("#blockers a.who").first().hover();
  await p.waitForTimeout(600);
  const cards = await p.locator(".profile-card").count();
  await c.close();
  return {
    ok:
      texts.length >= 10 &&
      texts.every((t) => /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC$/.test(t)) &&
      !toggle &&
      links >= 10 &&
      action === "/search" &&
      cards === 0,
    detail: `${texts.length} times, first ${texts[0]}; toggle visible ${toggle}; ${links} row links; ${cards} cards`,
  };
});

// ---- A touch device ------------------------------------------------------
await probe("on a touch device (hover: none) there are no cards: no request, no card element, and the rule is under @media (hover: hover)", async () => {
  const c = await browser.newContext({
    hasTouch: true,
    isMobile: true,
    viewport: { width: 390, height: 800 },
    timezoneId: ZONE,
  });
  const p = await c.newPage();
  watch(p);
  let requests = 0;
  p.on("request", (r) => {
    if (r.url().includes("/card/")) {
      requests += 1;
    }
  });
  await p.goto(base + accountPath);
  const hover = await p.evaluate(() => matchMedia("(hover: hover)").matches);
  const rule = await p.evaluate(() => {
    for (const sheet of document.styleSheets) {
      for (const r of sheet.cssRules) {
        if (r.media && r.media.mediaText.replace(/\s/g, "") === "(hover:hover)") {
          return [...r.cssRules].map((x) => x.selectorText).join(" | ");
        }
      }
    }
    return null;
  });
  const link = p.locator("#blockers a.who").first();
  await link.dispatchEvent("mouseover");
  await link.focus();
  await p.waitForTimeout(700);
  const cards = await p.locator(".profile-card").count();
  const href = await link.getAttribute("href");
  await c.close();
  return {
    ok: hover === false && requests === 0 && cards === 0 && !!rule && rule.includes(".profile-card") && href.startsWith("/did/"),
    detail: `matchMedia(hover: hover) = ${hover}; ${requests} card requests; ${cards} cards; rule: ${rule}`,
  };
});

out("no console error and no CSP violation on any page", problems.length === 0, problems.slice(0, 3).join(" | "));
await browser.close();
