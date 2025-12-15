import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";
import { parseStringPromise } from "xml2js";
import * as dotenv from "dotenv";

dotenv.config();

/* ============================================================
   ENV
============================================================ */
const APPS_SCRIPT_URL = process.env.APPS_SCRIPT_URL;

const CLOUDFLARE_ZONE_ID = process.env.CLOUDFLARE_ZONE_ID;
const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;

/* ============================================================
   SINGLE CONFIG (ID ONLY)
============================================================ */
const DOMAIN = "https://seoboost.id";
const PROXY = process.env.BRD_PROXY_ID;
const USER_AGENT = "Seoboost-CacheWarmer-ID/1.0";

/* ============================================================
   UTIL
============================================================ */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const runId = Math.random().toString(36).slice(2) + Date.now().toString(36);

function makeSheetName() {
  const d = new Date(Date.now() + 8 * 3600 * 1000); // WITA
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(
    d.getUTCDate()
  )}_${p(d.getUTCHours())}-${p(d.getUTCMinutes())}-${p(
    d.getUTCSeconds()
  )}_WITA`;
}

function extractCfEdge(cfRay) {
  if (!cfRay || typeof cfRay !== "string") return "UNKNOWN";
  const parts = cfRay.split("-");
  return parts[parts.length - 1] || "UNKNOWN";
}

function extractVercelEdge(xvid) {
  if (!xvid || typeof xvid !== "string") return "N/A";
  return xvid.split("::")[0] || "N/A";
}

/* ============================================================
   LOGGER → GOOGLE SHEETS
============================================================ */
class AppsScriptLogger {
  constructor() {
    this.rows = [];
    this.sheetName = makeSheetName();
    this.startedAt = new Date().toISOString();
    this.finishedAt = null;
  }

  log(row) {
    this.rows.push([
      runId,
      this.startedAt,
      this.finishedAt,
      row.edge,
      row.url,
      row.status || "",
      row.cfCache || "",
      row.vercelCache || "",
      row.cfRay || "",
      row.vercelEdge || "",
      row.responseMs || "",
      row.error ? 1 : 0,
      row.message || "",
    ]);
  }

  finalize() {
    this.finishedAt = new Date().toISOString();
    this.rows = this.rows.map((r) => ((r[2] = this.finishedAt), r));
  }

  async flush() {
    if (!APPS_SCRIPT_URL || this.rows.length === 0) return;

    await axios.post(
      APPS_SCRIPT_URL,
      { sheetName: this.sheetName, rows: this.rows },
      { timeout: 20000, headers: { "Content-Type": "application/json" } }
    );

    console.log(`📝 Logged ${this.rows.length} rows → ${this.sheetName}`);
    this.rows = [];
  }
}

/* ============================================================
   AXIOS CONFIG
============================================================ */
function axiosCfg() {
  const agent = PROXY ? new HttpsProxyAgent(PROXY) : undefined;
  return {
    httpsAgent: agent,
    headers: { "User-Agent": USER_AGENT },
    timeout: 30000,
  };
}

/* ============================================================
   SITEMAP
============================================================ */
async function fetchUrls() {
  try {
    const xml = await axios
      .get(`${DOMAIN}/sitemap.xml`, axiosCfg())
      .then((r) => r.data);

    const parsed = await parseStringPromise(xml, {
      explicitArray: false,
      ignoreAttrs: true,
    });

    const urls = parsed?.urlset?.url;
    if (!urls) return [];

    return (Array.isArray(urls) ? urls : [urls])
      .map((u) => u.loc)
      .filter(Boolean);
  } catch (e) {
    console.warn("❌ Failed to fetch sitemap:", e?.message || e);
    return [];
  }
}

/* ============================================================
   CLOUDFLARE PURGE
============================================================ */
async function purgeCloudflare(url) {
  if (!CLOUDFLARE_ZONE_ID || !CLOUDFLARE_API_TOKEN) return;

  try {
    await axios.post(
      `https://api.cloudflare.com/client/v4/zones/${CLOUDFLARE_ZONE_ID}/purge_cache`,
      { files: [url] },
      {
        headers: {
          Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );
  } catch {}
}

/* ============================================================
   WARMER (EDGE = CF EDGE REAL)
============================================================ */
async function warm(urls, logger) {
  for (const url of urls) {
    const t0 = Date.now();
    try {
      const res = await axios.get(url, axiosCfg());
      const dt = Date.now() - t0;

      const cfRay = res.headers["cf-ray"] || "";
      const edge = extractCfEdge(cfRay);

      const cfCache = res.headers["cf-cache-status"] || "N/A";
      const vercelCache = res.headers["x-vercel-cache"] || "N/A";

      console.log(
        `[${edge}] ${res.status} cf=${cfCache} vercel=${vercelCache} - ${url}`
      );

      logger.log({
        edge,
        url,
        status: res.status,
        cfCache,
        vercelCache,
        cfRay,
        vercelEdge: extractVercelEdge(res.headers["x-vercel-id"]),
        responseMs: dt,
      });

      if (vercelCache !== "HIT") {
        await purgeCloudflare(url);
      }
    } catch (e) {
      logger.log({
        edge: "ERROR",
        url,
        error: 1,
        message: e?.message || "request failed",
      });
    }

    await sleep(1500);
  }
}

/* ============================================================
   MAIN
============================================================ */
(async () => {
  console.log(`[CacheWarmer] Started ${new Date().toISOString()}`);
  const logger = new AppsScriptLogger();

  try {
    const urls = await fetchUrls();
    console.log(`[ID] Found ${urls.length} URLs`);
    await warm(urls, logger);
  } finally {
    logger.finalize();
    await logger.flush();
  }

  console.log(`[CacheWarmer] Finished ${new Date().toISOString()}`);
})();
