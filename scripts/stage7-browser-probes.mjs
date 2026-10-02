// Browser probes of the stage-7 harness (Mode A): what only a browser can
// show about the admin sign-in. The OAuth callback is the end of a
// cross-site redirect chain; it answers 200 with a page that sets the
// SameSite=Strict session cookie and continues to / by meta refresh. The
// question is whether each engine sends that cookie on the refresh
// navigation — if not, the page's "Continue" link is the fallback.
//
// Run by `farsight-stage7-harness --browser` inside the Playwright image;
// prints one JSON object per line: {"what", "ok", "detail"}.
//
//   node stage7-browser-probes.mjs <farsight base> <stand-in base> <ui mode>
import { chromium, firefox, webkit } from "playwright";

const [base, standin, mode] = process.argv.slice(2);

function out(what, ok, detail = "") {
  console.log(JSON.stringify({ what, ok: !!ok, detail: String(detail).slice(0, 500) }));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let first = true;
for (const [name, engine] of [
  ["Chromium", chromium],
  ["Firefox", firefox],
  ["WebKit", webkit],
]) {
  // Each sign-in costs this address two of its five per minute.
  if (!first) await sleep(26000);
  first = false;
  const what = `${name}, ui = ${mode}: after signing in, the callback page's meta refresh lands on / with the session cookie (admin page, not the anonymous one)`;
  let browser;
  try {
    browser = await engine.launch();
    const context = await browser.newContext();
    const page = await context.newPage();
    // Every top-level navigation request, redirect hops included (the
    // stand-in answers with a redirect, which never commits a page).
    const seen = [];
    page.on("request", (r) => {
      if (r.isNavigationRequest() && r.frame() === page.mainFrame()) seen.push(r.url().split("?")[0]);
    });
    await page.goto(`${base}/enter`);
    await page.click("text=Sign in with ATProto");
    // POST /enter → the stand-in → /enter/callback → (meta refresh) → /
    await page.waitForURL(`${base}/`, { timeout: 15000 }).catch(() => {});
    await page.waitForLoadState("load").catch(() => {});
    const url = page.url();
    const admin = (await page.locator('a[href="/settings"]').count()) > 0;
    const viaStandin = seen.some((u) => u.startsWith(standin));
    const viaCallback = seen.some((u) => u.startsWith(`${base}/enter/callback`));
    const cookies = await context.cookies(base);
    const session = cookies.find((c) => c.name === "farsight_admin");
    const flow = cookies.find((c) => c.name === "farsight_flow");
    const auto = url === `${base}/` && admin;
    out(
      what,
      auto && viaStandin && viaCallback && session && session.sameSite === "Strict" && session.httpOnly && !flow,
      `ended at ${url.replace(base, "")}, admin nav: ${admin}, via stand-in: ${viaStandin}, via callback: ${viaCallback}, session cookie SameSite=${session && session.sameSite}, flow cookie cleared: ${!flow}`,
    );
    if (!auto) {
      // The fallback: the visible link starts a same-site navigation.
      if (page.url().startsWith(`${base}/enter/callback`)) {
        await page.click("text=Continue");
        await page.waitForLoadState("load").catch(() => {});
      } else {
        await page.goto(`${base}/settings`);
      }
      const after = (await page.locator('a[href="/settings"]').count()) > 0;
      out(`${name}, ui = ${mode}: the "Continue" link (a same-site navigation) reaches the admin pages`, after, page.url());
    }
    await context.close();
  } catch (e) {
    out(what, false, `threw: ${e.message}`);
  } finally {
    if (browser) await browser.close();
  }
}
