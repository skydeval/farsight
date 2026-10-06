// Browser probes of the stage-9 harness (Mode A): what only a browser can
// show about UI v2.5.2 — the wizard's admin DID field appears with its
// checkbox and without any script, a dashboard whose poll is refused keeps
// its content instead of swapping in another page, the script still
// rewrites times on the pages' new addresses, and an account on an admin
// page under /admin opens its card.
//
// Run by `farsight-stage9-harness --browser` inside the Playwright image;
// prints one JSON object per line: {"what", "ok", "detail"}.
//
//   node stage9-browser-probes.mjs <farsight base> <session cookie value> <subject DID> <setup base> <setup cookie value>
import { chromium, firefox, webkit } from "playwright";

const [base, session, subject, setupBase, setupCookie] = process.argv.slice(2);
const host = new URL(base).hostname;

function out(what, ok, detail = "") {
  console.log(JSON.stringify({ what, ok: !!ok, detail: String(detail).slice(0, 500) }));
}

async function probe(what, fn) {
  try {
    const [ok, detail] = await fn();
    out(what, ok, detail);
  } catch (e) {
    out(what, false, e && e.message ? e.message : e);
  }
}

// ---- The wizard's access step, with JavaScript switched off, in the three
// engines.
for (const [name, engine] of [
  ["Chromium", chromium],
  ["Firefox", firefox],
  ["WebKit", webkit],
]) {
  let browser;
  try {
    browser = await engine.launch();
    const context = await browser.newContext({ javaScriptEnabled: false });
    await context.addCookies([
      {
        name: "farsight_setup",
        value: setupCookie,
        domain: new URL(setupBase).hostname,
        path: "/setup",
        httpOnly: true,
        sameSite: "Strict",
      },
    ]);
    const page = await context.newPage();
    await page.goto(`${setupBase}/setup/access`);
    await probe(
      `${name}, JavaScript off: both boxes start unticked and the admin DID field is not shown; ticking "Enable admin UI" shows it, unticking hides it again`,
      async () => {
        const field = page.locator("#admin_did");
        const before = {
          pub: await page.locator("#public_ui").isChecked(),
          adm: await page.locator("#admin_ui").isChecked(),
          shown: await field.isVisible(),
          scripts: await page.locator("script").count(),
        };
        await page.locator("#admin_ui").check();
        const ticked = await field.isVisible();
        // The public box does not reveal it.
        await page.locator("#admin_ui").uncheck();
        await page.locator("#public_ui").check();
        const other = await field.isVisible();
        await page.locator("#public_ui").uncheck();
        return [
          !before.pub && !before.adm && !before.shown && before.scripts === 0 && ticked && !other,
          JSON.stringify({ ...before, ticked, other }),
        ];
      },
    );
    await context.close();
  } catch (e) {
    out(`${name}: the wizard probe ran`, false, e && e.message ? e.message : e);
  } finally {
    if (browser) await browser.close();
  }
}

// ---- The admin and public pages, in Chromium.
const browser = await chromium.launch();
try {
  const context = await browser.newContext({ viewport: { width: 1200, height: 700 } });
  await context.addCookies([
    { name: "farsight_admin", value: session, domain: host, path: "/", httpOnly: true, sameSite: "Strict" },
  ]);

  // An account on the lookup page opens its card from /admin/card/….
  const lookup = await context.newPage();
  const asked = [];
  lookup.on("response", (r) => {
    const u = new URL(r.url());
    if (u.pathname.startsWith("/admin/card/") || u.pathname.startsWith("/static/")) {
      asked.push(`${r.status()} ${u.pathname}`);
    }
  });
  await lookup.goto(`${base}/admin/lookup/did?q=${encodeURIComponent(subject)}`);
  await probe(
    "the lookup page under /admin loads the admin script, /static/admin.js (not /static/public.js or /static/farsight.js); hovering an account asks /admin/card/{did} with the session and shows the card",
    async () => {
      const link = lookup.locator("a.who[data-card-session]").first();
      const n = await lookup.locator("a.who[data-card-session]").count();
      const got = lookup.waitForResponse((r) => new URL(r.url()).pathname.startsWith("/admin/card/"), { timeout: 15000 });
      await link.hover();
      const r = await got;
      await lookup.waitForTimeout(500);
      const cards = await lookup.locator(".profile-card, [role=tooltip], .card-pop").count();
      const described = await link.getAttribute("aria-describedby");
      return [
        n > 0 &&
          r.status() === 200 &&
          asked.some((a) => a === "200 /static/admin.js") &&
          !asked.some((a) => a.includes("/static/public.js")) &&
          !asked.some((a) => a.includes("farsight.js")) &&
          (cards > 0 || !!described),
        `${n} links; ${asked.join(", ")}; card elements ${cards}; aria-describedby ${described}`,
      ];
    },
  );
  await probe("times on the admin page under /admin are rewritten by the script (title keeps the UTC text)", async () => {
    const t = await lookup.evaluate(() =>
      [...document.querySelectorAll("time[datetime]")].map((e) => ({ text: e.textContent, title: e.getAttribute("title") })),
    );
    return [t.length > 0 && t.every((x) => x.title && x.title.includes("UTC")), JSON.stringify(t.slice(0, 2))];
  });

  await probe("the bar's Alerts opens under the bar with the coverage sentence, its time in the viewer's timezone; Escape closes it", async () => {
    const box = lookup.locator("details.nav-alerts");
    await lookup.locator(".alerts-panel .alerts-coverage").waitFor({ state: "attached", timeout: 15000 });
    await box.locator("summary").click();
    await lookup.locator(".alerts-panel .alerts-coverage").waitFor({ state: "visible", timeout: 5000 });
    const seen = await lookup.evaluate(() => {
      const panel = document.querySelector(".alerts-panel");
      const r = panel.getBoundingClientRect();
      return {
        text: panel.querySelector(".alerts-coverage").textContent.slice(0, 80),
        times: [...panel.querySelectorAll("time[data-plain]")].map((t) => [t.textContent, t.getAttribute("title"), t.getAttribute("data-local")]),
        inside: r.left >= 0 && r.right <= window.innerWidth + 1,
        count: document.getElementById("alerts-count").textContent,
        banners: panel.querySelectorAll(".banner").length,
      };
    });
    await lookup.keyboard.press("Escape");
    const open = await box.evaluate((d) => d.open);
    return [
      seen.text.startsWith("Coverage:") &&
        seen.inside &&
        String(seen.banners) === seen.count &&
        seen.times.every((t) => t[2] === "1" && / UTC$/.test(t[1] || "")) &&
        !open,
      JSON.stringify(seen),
    ];
  });

  // The public account page at its new address.
  const anon = await browser.newContext();
  const pub = await anon.newPage();
  const failed = [];
  pub.on("response", (r) => {
    if (r.status() >= 400) failed.push(`${r.status()} ${new URL(r.url()).pathname}`);
  });
  await pub.goto(`${base}/did/${subject}`);
  await probe(
    "the public account page at /did/{did}: stylesheet and scripts load from /static, every <time> is rewritten with the UTC text kept in title, the theme toggle is shown",
    async () => {
      const t = await pub.evaluate(() => ({
        times: [...document.querySelectorAll("time[datetime]")].map((e) => ({ text: e.textContent, title: e.getAttribute("title") })),
        toggle: !document.querySelector(".theme-toggle").hidden,
        styled: getComputedStyle(document.querySelector("nav.public-nav")).position,
      }));
      return [
        failed.length === 0 && t.times.length > 0 && t.times.every((x) => x.title && x.title.includes("UTC")) && t.toggle && t.styled === "sticky",
        `${JSON.stringify(t.times.slice(0, 2))} toggle ${t.toggle} nav ${t.styled} failed [${failed.join(", ")}]`,
      ];
    },
  );
  await probe("following the old address /public/did/{did} in a browser lands on /did/{did}", async () => {
    const p = await anon.newPage();
    await p.goto(`${base}/public/did/${subject}?x=1#blocked-by`);
    const u = new URL(p.url());
    await p.close();
    return [u.pathname === `/did/${subject}` && u.search === "?x=1" && u.hash === "#blocked-by", p.url()];
  });
  await anon.close();

  // The dashboard when its poll is refused: htmx leaves the page as it is.
  const dash = await context.newPage();
  await dash.goto(`${base}/admin`);
  await probe(
    "a dashboard whose session ends keeps its content: the next poll gets 404 and htmx swaps nothing in — no sign-in page inside the dashboard, same URL",
    async () => {
      const before = await dash.locator("#dash h2").allInnerTexts();
      await context.clearCookies();
      const r = await dash.waitForResponse((x) => new URL(x.url()).pathname === "/admin/dashboard/fragment", { timeout: 25000 });
      await dash.waitForTimeout(500);
      const after = await dash.locator("#dash h2").allInnerTexts();
      const form = await dash.locator('form[action="/enter"]').count();
      return [
        r.status() === 404 && before.length > 0 && JSON.stringify(after) === JSON.stringify(before) && form === 0 && new URL(dash.url()).pathname === "/admin",
        `poll ${r.status()}; headings ${before.length} → ${after.length}; sign-in forms ${form}; ${dash.url()}`,
      ];
    },
  );
  await probe("the next navigation from that tab goes to the sign-in page", async () => {
    await dash.goto(`${base}/admin/settings`);
    return [new URL(dash.url()).pathname === "/enter", dash.url()];
  });
  await context.close();
} catch (e) {
  out("the admin and public browser probes ran", false, e && e.message ? e.message : e);
} finally {
  await browser.close();
}
