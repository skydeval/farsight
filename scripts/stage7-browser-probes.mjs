// Browser probes of the stage-7 harness: what only a browser can
// show about the admin sign-in. The OAuth callback is the end of a
// cross-site redirect chain; it answers 200 with a page that sets the
// SameSite=Strict session cookie and continues to /admin by meta refresh. The
// question is whether each engine sends that cookie on the refresh
// navigation — if not, the page's "Continue" link is the fallback.
//
// Then the same browser is asked to sign in again: the harness starts
// this server with a sign-in that stays fresh for 12 seconds
// (`STEP_UP_SECS`; ten minutes in a release build). Creating an API key
// after that is refused, the sign-in page says why, and after signing in
// again the browser is back on the page it came from, where the key is
// created.
//
// Run by `farsight-stage7-harness --browser` inside the Playwright image;
// prints one JSON object per line: {"what", "ok", "detail"}.
//
//   node stage7-browser-probes.mjs <farsight base> <stand-in base>
import { chromium, firefox, webkit } from "playwright";

const [base, standin] = process.argv.slice(2);

function out(what, ok, detail = "") {
  console.log(JSON.stringify({ what, ok: !!ok, detail: String(detail).slice(0, 500) }));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// How long a sign-in stays fresh on the server the harness started.
const STEP_UP_SECS = 12;

let first = true;
for (const [name, engine] of [
  ["Chromium", chromium],
  ["Firefox", firefox],
  ["WebKit", webkit],
]) {
  // Each engine signs in twice, and a sign-in costs this address two of
  // its five per minute.
  if (!first) await sleep(34000);
  first = false;
  const what = `${name}: after signing in, the callback page's meta refresh lands on /admin with the session cookie (the dashboard, not the redirect to /enter)`;
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
    // POST /enter → the stand-in → /enter/callback → (meta refresh) → /admin
    await page.waitForURL(`${base}/admin`, { timeout: 15000 }).catch(() => {});
    await page.waitForLoadState("load").catch(() => {});
    const url = page.url();
    const admin = (await page.locator('a[href="/admin/settings"]').count()) > 0;
    const viaStandin = seen.some((u) => u.startsWith(standin));
    const viaCallback = seen.some((u) => u.startsWith(`${base}/enter/callback`));
    const cookies = await context.cookies(base);
    const session = cookies.find((c) => c.name === "farsight_admin");
    const flow = cookies.find((c) => c.name === "farsight_flow");
    const auto = url === `${base}/admin` && admin;
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
        await page.goto(`${base}/admin/settings`);
      }
      const after = (await page.locator('a[href="/admin/settings"]').count()) > 0;
      out(`${name}: the "Continue" link (a same-site navigation) reaches the admin pages`, after, page.url());
    }
    // --- signing in again before a sensitive action -----------------
    const again = `${name}: once the sign-in is no longer fresh, creating an API key is refused and the browser is on the sign-in page, which says "Sign in again to change this setting"; no key was made`;
    const back = `${name}: "Sign in again with ATProto" runs the sign-in and lands back on Operations, and the key is created when the form is sent again`;
    try {
      const keyName = `probe-${name.toLowerCase()}`;
      await sleep((STEP_UP_SECS + 2) * 1000);
      await page.goto(`${base}/admin/ops`);
      await page.fill("#name", keyName);
      seen.length = 0;
      await page.click("text=Create key");
      await page.waitForURL(`${base}/enter?again=ops`, { timeout: 15000 }).catch(() => {});
      await page.waitForLoadState("load").catch(() => {});
      const asked = page.url();
      const banner = ((await page.locator("#again").count()) > 0 ? await page.locator("#again").innerText() : "").trim();
      const button = (await page.getByRole("button", { name: "Sign in again with ATProto" }).count()) === 1;
      // The operations page in another tab still shows no such key.
      const other = await context.newPage();
      await other.goto(`${base}/admin/ops`);
      const madeEarly = (await other.locator(`strong:text-is("${keyName}")`).count()) > 0;
      await other.close();
      out(
        again,
        asked === `${base}/enter?again=ops` &&
          banner.startsWith("Sign in again to change this setting.") &&
          banner.includes("Nothing was changed.") &&
          button &&
          !madeEarly,
        `ended at ${asked.replace(base, "")}; banner: ${banner.slice(0, 90)}; button: ${button}; key made: ${madeEarly}`,
      );
      seen.length = 0;
      await page.getByRole("button", { name: "Sign in again with ATProto" }).click();
      // POST /enter → the stand-in → /enter/callback → (meta refresh) → /admin/ops
      await page.waitForURL(`${base}/admin/ops`, { timeout: 15000 }).catch(() => {});
      await page.waitForLoadState("load").catch(() => {});
      let returned = page.url();
      if (returned.startsWith(`${base}/enter/callback`)) {
        // The fallback of an engine that does not follow the refresh.
        await page.click("text=Continue");
        await page.waitForLoadState("load").catch(() => {});
        returned = page.url();
      }
      const viaStandinAgain = seen.some((u) => u.startsWith(standin));
      const viaCallbackAgain = seen.some((u) => u.startsWith(`${base}/enter/callback`));
      // The form is empty again: nothing was carried through the sign-in.
      const emptied = (await page.inputValue("#name").catch(() => "?")) === "";
      await page.fill("#name", keyName);
      await page.click("text=Create key");
      await page.waitForLoadState("load").catch(() => {});
      const notice = ((await page.locator(".banner.ok").count()) > 0 ? await page.locator(".banner.ok").first().innerText() : "").trim();
      const listed = (await page.locator(`strong:text-is("${keyName}")`).count()) === 1;
      const shown = (await page.locator(".secret").count()) > 0 ? (await page.locator(".secret").first().innerText()).trim() : "";
      out(
        back,
        returned === `${base}/admin/ops` &&
          viaStandinAgain &&
          viaCallbackAgain &&
          emptied &&
          /^API key \d+ created\.$/.test(notice) &&
          listed &&
          shown.startsWith("fsk_"),
        `returned to ${returned.replace(base, "")}, via stand-in: ${viaStandinAgain}, via callback: ${viaCallbackAgain}, form empty: ${emptied}, notice: ${notice}, listed once: ${listed}, token shown: ${shown.startsWith("fsk_")}`,
      );
    } catch (e) {
      out(again, false, `threw: ${e.message}`);
    }
    await context.close();
  } catch (e) {
    out(what, false, `threw: ${e.message}`);
  } finally {
    if (browser) await browser.close();
  }
}
