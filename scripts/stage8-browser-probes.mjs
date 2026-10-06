// Browser probes of the stage-8 harness (Mode A): what only a browser can
// show about the admin pages after UI v2.4.3 — the header stays at the top
// while the page scrolls, account links open profile cards for a signed-in
// admin, a card request that is refused never puts another page into the
// card, times are rewritten to the viewer's timezone by the script the
// admin pages load, and without a session the lookup page is not served
// at all (the browser is sent to /enter).
//
// Run by `farsight-stage8-harness --browser` inside the Playwright image;
// prints one JSON object per line: {"what", "ok", "detail"}.
//
//   node stage8-browser-probes.mjs <farsight base> <session cookie value> <subject DID> <second DID>
import { chromium, firefox, webkit } from "playwright";

const [base, session, subject, second] = process.argv.slice(2);
const host = new URL(base).hostname;

function out(what, ok, detail = "") {
  console.log(JSON.stringify({ what, ok: !!ok, detail: String(detail).slice(0, 500) }));
}

const lookup = (did) => `${base}/admin/lookup/did?q=${encodeURIComponent(did)}`;

async function signedIn(browser) {
  const context = await browser.newContext({ viewport: { width: 1200, height: 700 } });
  await context.addCookies([
    { name: "farsight_admin", value: session, domain: host, path: "/", httpOnly: true, sameSite: "Strict" },
  ]);
  return context;
}

// The header's box after scrolling to the bottom of a long page.
async function header(page) {
  return page.evaluate(async () => {
    const h = document.querySelector("header.top");
    const style = getComputedStyle(h);
    // The page may still be growing, and some engines move the window
    // over several frames: scroll until the position stops changing.
    let last = -1;
    for (let i = 0; i < 100 && window.scrollY !== last; i++) {
      last = window.scrollY;
      window.scrollTo(0, document.documentElement.scrollHeight);
      await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
    }
    const box = h.getBoundingClientRect();
    return {
      position: style.position,
      top: Math.round(box.top),
      bottom: Math.round(box.bottom),
      scrolled: Math.round(window.scrollY),
      // What is drawn at the header's place: the header, not page content.
      onTop: h.contains(document.elementFromPoint(box.left + 5, box.top + 5)),
    };
  });
}

for (const [name, engine] of [
  ["Chromium", chromium],
  ["Firefox", firefox],
  ["WebKit", webkit],
]) {
  let browser;
  try {
    browser = await engine.launch();

    // ---- A signed-in admin on the DID lookup.
    const context = await signedIn(browser);
    const page = await context.newPage();
    await page.goto(lookup(subject));
    await page.waitForLoadState("load");
    const h = await header(page);
    out(
      `${name}: the admin header is position: sticky and stays at the top of the window, above the page, after scrolling to the bottom of a long lookup page`,
      h.position === "sticky" && h.top === 0 && h.bottom > 0 && h.scrolled > 300 && h.onTop,
      JSON.stringify(h),
    );
    await page.evaluate(() => window.scrollTo(0, 0));

    const times = await page.evaluate(() => {
      const cells = [...document.querySelectorAll("td time[datetime]")];
      const done = cells.filter((t) => t.getAttribute("data-local") && /UTC$/.test(t.getAttribute("title") || ""));
      return { cells: cells.length, rewritten: done.length, sample: done[0] ? done[0].textContent : "" };
    });
    out(
      `${name}: the admin pages run the UI script — every time cell of the lookup tables is rewritten to the viewer's timezone, with the server's UTC text kept in its title`,
      times.cells > 0 && times.rewritten === times.cells,
      JSON.stringify(times),
    );

    // Cards are for devices that can hover (the rule is under
    // @media (hover: hover), as on the public pages). A headless engine
    // that reports (hover: none) gets none, by design.
    const canHover = await page.evaluate(() => matchMedia("(hover: hover)").matches);
    const marked = await page.locator("a.who[data-card-session]").count();
    const first = page.locator("a.who[data-card-session]").first();
    const requests = [];
    page.on("request", (r) => {
      if (r.url().includes("/card/")) requests.push(new URL(r.url()).pathname);
    });
    await first.hover();
    if (!canHover) {
      await page.waitForTimeout(900);
      const none = await page.locator(".profile-card").count();
      out(
        `${name}: this engine reports (hover: none), where the UI shows no cards on any page, admin or public: hovering an account makes no card and no request`,
        marked >= 50 && none === 0 && requests.length === 0,
        `${marked} links, ${none} cards, ${requests.length} requests`,
      );
      await context.close();
      const plain = await browser.newContext({ viewport: { width: 1200, height: 700 } });
      const ppage = await plain.newPage();
      await ppage.goto(`${base}/did/${subject}`);
      await ppage.waitForLoadState("load");
      const bar = await ppage.evaluate(() => {
        const n = document.querySelector("nav.public-nav");
        return { position: getComputedStyle(n).position, toggle: !document.querySelector(".theme-toggle").hidden, times: document.querySelectorAll("td time[data-local]").length };
      });
      out(
        `${name}: the public page is as before — sticky bar, theme toggle shown, times in the viewer's timezone`,
        bar.position === "sticky" && bar.toggle && bar.times > 0,
        JSON.stringify(bar),
      );
      await plain.close();
      continue;
    }
    const card = page.locator(".who-wrap.open .profile-card");
    await card.locator(".pc-did").waitFor({ state: "visible", timeout: 8000 }).catch(() => {});
    const text = (await card.count()) ? await card.innerText() : "";
    // While the card is open the link's title is put aside as data-did.
    const did = (await first.getAttribute("title")) || (await first.getAttribute("data-did"));
    out(
      `${name}: resting the pointer on an account in an admin table opens its profile card, fetched from /admin/card/{did} with the session cookie`,
      marked >= 50 && text.includes(did) && text.includes("DID created") && requests.length === 1 && requests[0] === `/admin/card/${did}`,
      `${marked} links; request ${requests.join(",")}; card: ${text.replace(/\s+/g, " ").slice(0, 120)}`,
    );
    const below = await page.evaluate(() => {
      const c = document.querySelector(".who-wrap.open .profile-card");
      const h = document.querySelector("header.top");
      return c && Number(getComputedStyle(c).zIndex) < Number(getComputedStyle(h).zIndex);
    });
    out(`${name}: the card sits below the sticky header (z-index)`, below, String(below));
    await page.keyboard.press("Escape");

    // ---- The session ends while the page is open: the next card request is
    // refused. Whatever the refusal is, it must not end up in the card.
    await context.clearCookies();
    const other = page.locator("a.who[data-card-session]").nth(3);
    let status = 0;
    page.on("response", (r) => {
      if (r.url().includes("/admin/card/")) status = r.status();
    });
    await other.hover();
    const gone = page.locator(".who-wrap.open .profile-card");
    // The page's policy allows no evaluated script, so this waits on a
    // locator, not on a function run in the page.
    await gone
      .filter({ hasNotText: /Loading/ })
      .first()
      .waitFor({ state: "visible", timeout: 8000 })
      .catch(() => {});
    const goneText = (await gone.count()) ? await gone.innerText() : "";
    const leaked = await gone.locator("form, header, nav, button").count();
    out(
      `${name}: without a session the card request is answered 404 — not a redirect to the sign-in page — and the card shows the DID and "Profile not available.", with nothing of another page in it`,
      status === 404 && goneText.includes("Profile not available.") && !/Sign in/i.test(goneText) && leaked === 0,
      `status ${status}; card: ${goneText.replace(/\s+/g, " ").slice(0, 120)}`,
    );
    await context.close();

    // ---- No session: the lookup page is not served; the browser follows
    // the redirect to the sign-in page.
    const anon = await browser.newContext({ viewport: { width: 1200, height: 700 } });
    const apage = await anon.newPage();
    const asked = [];
    apage.on("request", (r) => {
      if (r.url().includes("/card/")) asked.push(r.url());
    });
    await apage.goto(lookup(second));
    await apage.waitForLoadState("load");
    const landed = new URL(apage.url()).pathname;
    const links = await apage.locator("a.who").count();
    const cards = await apage.locator(".profile-card").count();
    const navs = await apage.locator("header.top nav").count();
    const brand = await apage.locator("header.top .brand").count();
    const ah = await header(apage);
    out(
      `${name}: without a session the lookup page is not served — the browser lands on /enter, with no account link, no card and no card request; the header there is sticky and has the brand and no nav`,
      landed === "/enter" && links === 0 && cards === 0 && asked.length === 0 && navs === 0 && brand === 1 && ah.position === "sticky" && ah.top === 0,
      `landed on ${landed}; ${links} links, ${cards} cards, ${asked.length} requests, ${navs} navs`,
    );

    // ---- The public pages still do what they did.
    await apage.goto(`${base}/did/${subject}`);
    await apage.waitForLoadState("load");
    const nav = await apage.evaluate(async () => {
      const n = document.querySelector("nav.public-nav");
      window.scrollTo(0, document.body.scrollHeight);
      await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
      const top = Math.round(n.getBoundingClientRect().top);
      window.scrollTo(0, 0);
      return { position: getComputedStyle(n).position, top, toggle: !document.querySelector(".theme-toggle").hidden };
    });
    const plink = apage.locator("#blockers a.who").first();
    await plink.hover();
    const pcard = apage.locator("#blockers .who-wrap.open .profile-card");
    await pcard.locator(".pc-did").waitFor({ state: "visible", timeout: 8000 }).catch(() => {});
    const ptext = (await pcard.count()) ? await pcard.innerText() : "";
    const rel = await apage.evaluate(() => {
      const t = document.querySelector("#blockers td time[data-local]");
      return t ? t.textContent : "";
    });
    out(
      `${name}: the public page is as before — sticky bar, theme toggle shown, a card on hover from /card/{did}, times in the viewer's timezone`,
      nav.position === "sticky" && nav.top === 0 && nav.toggle && ptext.includes("did:plc:") && asked.some((u) => new URL(u).pathname.startsWith("/card/")) && rel.length > 0,
      `${JSON.stringify(nav)}; card: ${ptext.replace(/\s+/g, " ").slice(0, 80)}; time: ${rel}`,
    );
    await anon.close();
  } catch (e) {
    out(`${name}: the browser probes ran`, false, `threw: ${e.message}`);
  } finally {
    if (browser) await browser.close();
  }
}
