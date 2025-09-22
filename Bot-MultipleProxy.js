// install: npm install puppeteer
import fs from "fs/promises";
import puppeteer, { TimeoutError } from "puppeteer";

function randomDelay(min = 3000, max = 5000) {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return delay(ms);
}

const PRODUCTS_CSV_PATH = new URL("./Products.csv", import.meta.url);
const PROXY_LIST_PATH = new URL("./Proxy.txt", import.meta.url);
const DEFAULT_CONCURRENT_CHECKS = 3;
const PER_PRODUCT_DELAY = { min: 1000, max: 2500 };
const PRODUCT_PAGE_TIMEOUT = 12000;
const PROXY_LAUNCH_TIMEOUT_MS = 12000;
const PROXY_LAUNCH_MAX_ATTEMPTS = 3;
const ORDER_CONFIRMATION_URL = "https://www.popmart.com/vn/order-confirmation";
const POPMART_BLOCK_PATTERNS = [
  "ban dang truy cap qua thuong xuyen",
  "mot so tinh nang da bi han che",
];
const DEFAULT_SINGLE_BUY_COUNT = 12;
const DEFAULT_SET_BUY_COUNT = 2;
const GMT7_OFFSET_MINUTES = 7 * 60;
const ACTIVE_WINDOW = { startHour: 8, endHour: 19 };
const MS_PER_SECOND = 1000;
const MS_PER_MINUTE = 60 * MS_PER_SECOND;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;
const DAY_IN_MS = 24 * MS_PER_HOUR;

function getNowInGmt7() {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * MS_PER_MINUTE;
  return new Date(utc + GMT7_OFFSET_MINUTES * MS_PER_MINUTE);
}

function getMsSinceStartOfDay(date) {
  return (
    date.getHours() * MS_PER_HOUR +
    date.getMinutes() * MS_PER_MINUTE +
    date.getSeconds() * MS_PER_SECOND +
    date.getMilliseconds()
  );
}

function isWithinActiveWindow(date = getNowInGmt7()) {
  const msSinceStart = getMsSinceStartOfDay(date);
  const startMs = ACTIVE_WINDOW.startHour * MS_PER_HOUR;
  const endMs = ACTIVE_WINDOW.endHour * MS_PER_HOUR;
  return msSinceStart >= startMs && msSinceStart < endMs;
}

function msUntilNextActiveWindow(date = getNowInGmt7()) {
  const msSinceStart = getMsSinceStartOfDay(date);
  const startMs = ACTIVE_WINDOW.startHour * MS_PER_HOUR;
  const endMs = ACTIVE_WINDOW.endHour * MS_PER_HOUR;

  if (msSinceStart < startMs) {
    return startMs - msSinceStart;
  }

  if (msSinceStart >= startMs && msSinceStart < endMs) {
    return 0;
  }

  return DAY_IN_MS - msSinceStart + startMs;
}

function formatDuration(ms) {
  const totalSeconds = Math.ceil(ms / MS_PER_SECOND);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0) {
    parts.push(`${minutes}m`);
  }
  if (hours === 0 && seconds > 0) {
    parts.push(`${seconds}s`);
  }
  return parts.join(" ") || "0s";
}
function normalizeForMatch(value) {
  if (!value) {
    return "";
  }

  return value
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function isPopmartBlockPage(html) {
  if (!html) {
    return false;
  }

  const normalized = normalizeForMatch(html);
  if (!normalized) {
    return false;
  }

  return POPMART_BLOCK_PATTERNS.some((pattern) => normalized.includes(pattern));
}


async function delay(ms) {
  if (ms <= 0) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const entry = {
      timer: null,
      completed: false,
      finish() {
        if (entry.completed) {
          return;
        }
        entry.completed = true;
        if (entry.timer !== null) {
          clearTimeout(entry.timer);
          entry.timer = null;
        }
        activeDelays.delete(entry);
        resolve();
      },
    };

    entry.timer = setTimeout(() => {
      entry.finish();
    }, ms);

    activeDelays.add(entry);
  });
}

function cancelAllDelays() {
  for (const entry of Array.from(activeDelays)) {
    entry.finish();
  }
}

async function waitUntilActiveWindow() {
  const waitMs = msUntilNextActiveWindow();
  if (waitMs <= 0) {
    return;
  }

  console.log(
    `Outside monitoring window (08:00-19:00 GMT+7). Waiting ${formatDuration(waitMs)} before resuming.`
  );
  await delay(waitMs);
}

async function gracefulShutdown() {
  if (shuttingDown) {
    await shutdownComplete;
    return;
  }

  shuttingDown = true;
  console.log("Shutdown requested. Cleaning up...");

  cancelAllDelays();
  await closeAllPages();
  await safeCloseAllBrowsers();

  await shutdownComplete;
}

["SIGINT", "SIGTERM"].forEach((signal) => {
  process.once(signal, () => {
    gracefulShutdown()
      .then(() => process.exit(0))
      .catch((error) => {
        console.error("Error during shutdown:", error);
        process.exit(1);
      });
  });
});
function parseProxyLine(line, lineNumber) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) {
    return null;
  }

  const segments = trimmed.split(":");
  if (segments.length < 2) {
    console.warn(`Skipping invalid proxy entry on line ${lineNumber}: ${line}`);
    return null;
  }

  const host = segments[0].trim();
  const portValue = Number.parseInt(segments[1], 10);

  if (!host || Number.isNaN(portValue) || portValue <= 0 || portValue > 65535) {
    console.warn(`Skipping invalid proxy entry on line ${lineNumber}: ${line}`);
    return null;
  }

  let username = null;
  let password = null;

  if (segments.length >= 3) {
    username = segments[2].trim() || null;
  }

  if (segments.length >= 4) {
    password = segments.slice(3).join(":").trim() || null;
  }

  return {
    host,
    port: portValue,
    username,
    password,
    protocol: "http",
    label: `${host}:${portValue}`,
  };
}

async function readProxyList() {
  let raw;
  try {
    raw = await fs.readFile(PROXY_LIST_PATH, "utf8");
  } catch (error) {
    throw new Error(`Unable to read Proxy.txt: ${error.message}`);
  }

  const lines = raw.split(/\r?\n/);
  const proxies = [];

  lines.forEach((line, index) => {
    const parsed = parseProxyLine(line, index + 1);
    if (parsed) {
      proxies.push(parsed);
    }
  });

  if (proxies.length === 0) {
    throw new Error("Proxy.txt must contain at least one valid proxy entry.");
  }

  return proxies;
}

class ProxySession {
  constructor(config, index) {
    this.config = config;
    this.index = index;
    this.browser = null;
    this.launching = null;
    this.busy = false;
    this.failed = false;
    this.lastError = null;
  }

  get label() {
    return this.config.label ?? `${this.config.host}:${this.config.port}`;
  }

  async ensureBrowser() {
    if (this.failed) {
      throw this.lastError || new Error(`Proxy ${this.label} is unavailable due to repeated failures.`);
    }

    if (this.browser) {
      return this.browser;
    }

    if (!this.launching) {
      this.launching = this.launchBrowser();
    }

    try {
      return await this.launching;
    } finally {
      this.launching = null;
    }
  }

  async launchBrowser() {
    const { host, port, protocol } = this.config;
    const scheme = protocol || "http";
    const proxyTarget = `${scheme}://${host}:${port}`;
    let lastError = null;

    for (let attempt = 1; attempt <= PROXY_LAUNCH_MAX_ATTEMPTS; attempt += 1) {
      if (shuttingDown) {
        const error = new Error("Shutdown in progress");
        error.code = "SHUTTING_DOWN";
        this.failed = true;
        this.lastError = error;
        throw error;
      }

      try {
        const browser = await puppeteer.launch({
          headless: true,
          timeout: PROXY_LAUNCH_TIMEOUT_MS,
          args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            `--proxy-server=${proxyTarget}`,
          ],
        });

        browser.once("disconnected", () => {
          this.browser = null;
        });

        this.failed = false;
        this.lastError = null;
        this.browser = browser;
        return browser;
      } catch (error) {
        lastError = error;
        const message = typeof error?.message === "string" ? error.message.toLowerCase() : "";
        const isTimeout =
          error instanceof TimeoutError || message.includes("timed out") || message.includes("timeout");

        if (isTimeout && attempt < PROXY_LAUNCH_MAX_ATTEMPTS && !shuttingDown) {
          console.warn(
            `Proxy ${this.label} timed out after ${PROXY_LAUNCH_TIMEOUT_MS}ms (attempt ${attempt}/${PROXY_LAUNCH_MAX_ATTEMPTS}). Retrying...`
          );
          continue;
        }

        this.failed = true;
        this.lastError = error;
        throw error;
      }
    }

    this.failed = true;
    this.lastError = lastError;
    throw lastError;
  }

  async close() {
    if (!this.browser) {
      return;
    }

    try {
      await this.browser.close();
    } catch (error) {
      if (!shuttingDown) {
        console.warn(`Error closing browser for proxy ${this.label}:`, error);
      }
    } finally {
      this.browser = null;
    }
  }
}

class ProxyPool {
  constructor(proxyConfigs) {
    this.sessions = proxyConfigs.map((config, index) => new ProxySession(config, index));
    this.available = [];
    this.waitingResolvers = [];
    this.closed = false;
  }

  async init() {
    const usableSessions = [];

    for (const session of this.sessions) {
      try {
        await session.ensureBrowser();
        usableSessions.push(session);
      } catch (error) {
        console.error(`Failed to initialize proxy ${session.label}:`, error);
      }
    }

    this.sessions = usableSessions;
    this.available = usableSessions.slice();

    if (this.sessions.length === 0) {
      throw new Error("Unable to initialize any proxy browsers.");
    }
  }

  get size() {
    return this.sessions.length;
  }

  acquire() {
    if (this.closed) {
      return Promise.reject(new Error("Proxy pool has been shut down."));
    }

    const session = this._takeAvailable();
    if (session) {
      session.busy = true;
      return Promise.resolve(session);
    }

    return new Promise((resolve, reject) => {
      this.waitingResolvers.push({ resolve, reject });
    });
  }

  release(session) {
    if (!session) {
      return;
    }

    session.busy = false;

    if (session.failed) {
      this._evictSession(session);
      this._dispatchWaiting();
      return;
    }

    if (this.closed) {
      session.close().catch(() => {});
      return;
    }

    if (this.waitingResolvers.length > 0) {
      const index = Math.floor(Math.random() * this.waitingResolvers.length);
      const { resolve } = this.waitingResolvers.splice(index, 1)[0];
      session.busy = true;
      resolve(session);
      return;
    }

    if (!this.available.includes(session)) {
      this.available.push(session);
    }
  }

  _evictSession(session) {
    if (!session) {
      return;
    }

    this.available = this.available.filter((entry) => entry !== session);
    this.sessions = this.sessions.filter((entry) => entry !== session);

    if (!shuttingDown) {
      console.warn(`Removing proxy ${session.label} from pool after repeated failures.`);
      if (session.lastError) {
        console.warn(`Last error for proxy ${session.label}:`, session.lastError);
      }
    }

    session.close().catch((error) => {
      if (!shuttingDown) {
        console.warn(`Error closing proxy session ${session.label}:`, error);
      }
    });
  }

  _dispatchWaiting() {
    while (this.waitingResolvers.length > 0) {
      const session = this._takeAvailable();
      if (!session) {
        break;
      }

      if (session.failed) {
        this._evictSession(session);
        continue;
      }

      session.busy = true;
      const { resolve } = this.waitingResolvers.shift();
      resolve(session);
    }
  }

  _takeAvailable() {
    while (this.available.length > 0) {
      const index = Math.floor(Math.random() * this.available.length);
      const [session] = this.available.splice(index, 1);

      if (!session) {
        continue;
      }

      if (session.failed) {
        this._evictSession(session);
        continue;
      }

      return session;
    }

    return null;
  }

  async shutdown() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    while (this.waitingResolvers.length > 0) {
      const { reject } = this.waitingResolvers.shift();
      reject(new Error("Proxy pool shutting down."));
    }

    await Promise.all(
      this.sessions.map((session) =>
        session.close().catch((error) => {
          if (!shuttingDown) {
            console.warn(`Error closing proxy session ${session.label}:`, error);
          }
        })
      )
    );

    this.sessions = [];
    this.available = [];
  }
}



const telegramConfig = {
  token: process.env.TELEGRAM_BOT_TOKEN
    ? process.env.TELEGRAM_BOT_TOKEN.trim()
    : "",
  chatId: process.env.TELEGRAM_CHAT_ID ? process.env.TELEGRAM_CHAT_ID.trim() : "",
};

let telegramConfigWarningShown = false;
let telegramConfigLoadAttempted = false;
const buyNowWarningKeys = new Set();
const activeDelays = new Set();

let popmartBlockHandled = false;
let shuttingDown = false;
let proxyPoolRef = null;
const activePages = new Set();
let resolveShutdownPromise;
const shutdownComplete = new Promise((resolve) => {
  resolveShutdownPromise = resolve;
});

function parseDotEnv(content) {
  const values = {};
  content.split(/\r?\n/).forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      return;
    }

    const equalsIndex = trimmed.indexOf("=");
    if (equalsIndex === -1) {
      return;
    }

    const key = trimmed.slice(0, equalsIndex).trim();
    let value = trimmed.slice(equalsIndex + 1).trim();

    if (
      (value.startsWith("\"") && value.endsWith("\"")) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    values[key] = value;
  });
  return values;
}


let envFileLoaded = false;

async function loadEnvFromFile() {
  if (envFileLoaded) {
    return;
  }

  envFileLoaded = true;

  try {
    const envPath = new URL("./.env", import.meta.url);
    const content = await fs.readFile(envPath, "utf8");
    const values = parseDotEnv(content);

    Object.entries(values).forEach(([key, value]) => {
      if (typeof process.env[key] === "undefined" || process.env[key] === "") {
        process.env[key] = value;
      }
    });
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.warn("Unable to load .env file:", error);
    }
  }
}



async function ensureTelegramConfig() {
  if (telegramConfig.token && telegramConfig.chatId) {
    return telegramConfig;
  }

  if (!telegramConfigLoadAttempted) {
    telegramConfigLoadAttempted = true;
    await loadEnvFromFile();

    if (!telegramConfig.token && process.env.TELEGRAM_BOT_TOKEN) {
      telegramConfig.token = process.env.TELEGRAM_BOT_TOKEN.trim();
    }

    if (!telegramConfig.chatId && process.env.TELEGRAM_CHAT_ID) {
      telegramConfig.chatId = process.env.TELEGRAM_CHAT_ID.trim();
    }
  }

  return telegramConfig;
}

async function sendTelegramMessage(text) {
  await ensureTelegramConfig();

  if (!telegramConfig.token || !telegramConfig.chatId) {
    if (!telegramConfigWarningShown) {
      console.warn(
        "Telegram notifications are disabled. Provide TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID (environment or .env)."
      );
      telegramConfigWarningShown = true;
    }
    return;
  }

  try {
    const response = await fetch(
      `https://api.telegram.org/bot${telegramConfig.token}/sendMessage`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          chat_id: telegramConfig.chatId,
          text,
        }),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      if (response.status === 404) {
        console.error(
          "Telegram API returned 404. Double-check TELEGRAM_BOT_TOKEN (likely invalid)."
        );
      } else if (response.status === 401) {
        console.error(
          "Telegram API returned 401. Verify TELEGRAM_CHAT_ID or bot permissions."
        );
      }
      console.error("Failed to send Telegram message:", errorText);
    }
  } catch (error) {
    console.error("Failed to send Telegram message:", error);
  }
}

function slugify(value) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .trim();
}

function deriveBuyNowTitle(productUrl, productName) {
  try {
    const url = new URL(productUrl);
    const segments = url.pathname.split("/").filter(Boolean);
    if (segments.length > 0) {
      const lastSegment = segments[segments.length - 1];
      if (lastSegment) {
        return slugify(lastSegment);
      }
    }
  } catch (error) {
    // ignore malformed URLs
  }
  return slugify(productName);
}

function normalizeHeaderName(value) {
  return value.replace(/[\s_-]/g, "").toLowerCase();
}

function findHeaderIndex(headers, ...candidates) {
  const normalizedCandidates = candidates.map(normalizeHeaderName);
  return headers.findIndex((header) =>
    normalizedCandidates.includes(normalizeHeaderName(header))
  );
}

function extractSkuIdFromData(skuData) {
  if (!skuData || typeof skuData !== "object") {
    return "";
  }

  const raw =
    skuData.skuId ??
    skuData.id ??
    skuData.sku_id ??
    skuData.skuid ??
    skuData.skuID ??
    skuData.sku;

  if (typeof raw === "number") {
    return String(raw);
  }

  if (typeof raw === "string") {
    return raw.trim();
  }

  return "";
}

function resolveSkuId(product, skuIndex, skuData) {
  const variantKind = resolveVariantKind(product, skuIndex, skuData);
  const derivedSkuId = extractSkuIdFromData(skuData);

  if (variantKind === "single" && product.skuSingleId) {
    return product.skuSingleId;
  }

  if (variantKind === "set" && product.skuSetId) {
    return product.skuSetId;
  }

  return derivedSkuId;
}

function resolveVariantKind(product, skuIndex, skuData) {
  const skuId = extractSkuIdFromData(skuData);

  if (skuId) {
    if (product.skuSingleId && skuId === product.skuSingleId) {
      return "single";
    }

    if (product.skuSetId && skuId === product.skuSetId) {
      return "set";
    }
  }

  if (!product.skuSingleId && product.skuSetId) {
    return "set";
  }

  if (skuIndex === 0) {
    if (!product.skuSingleId) {
      return "set";
    }
    return "single";
  }

  if (skuIndex === 1) {
    return "set";
  }

  return "other";
}

function resolveBuyCount(product, skuIndex, skuData) {
  const variantKind = resolveVariantKind(product, skuIndex, skuData);
  if (variantKind === "single") {
    return product.limitSingle ?? DEFAULT_SINGLE_BUY_COUNT;
  }
  if (variantKind === "set") {
    return product.limitSet ?? DEFAULT_SET_BUY_COUNT;
  }
  return 1;
}

function createBuyNowLink(product, skuIndex, skuData) {
  const skuId = resolveSkuId(product, skuIndex, skuData);
  const warningKey = `${product.url}#${skuIndex}`;

  if (!product.spuId) {
    if (!buyNowWarningKeys.has(product.url)) {
      console.warn(
        `Unable to build buy-now link for ${product.name} - missing spuId.`
      );
      buyNowWarningKeys.add(product.url);
    }
    return null;
  }

  if (!skuId) {
    if (!buyNowWarningKeys.has(warningKey)) {
      console.warn(
        `Unable to build buy-now link for ${product.name} SKU${skuIndex + 1} - missing skuId.`
      );
      buyNowWarningKeys.add(warningKey);
    }
    return null;
  }

  const count = resolveBuyCount(product, skuIndex, skuData);

  const params = new URLSearchParams({
    spuId: String(product.spuId),
    skuId: String(skuId),
    count: String(count),
    spuTitle: product.buyNowTitle,
  });

  return `${ORDER_CONFIRMATION_URL}?${params.toString()}`;
}

function splitCsvRow(row) {
  const values = [];
  let current = "";
  let insideQuotes = false;

  for (let i = 0; i < row.length; i += 1) {
    const char = row[i];

    if (char === "\"") {
      if (insideQuotes && row[i + 1] === "\"") {
        current += "\"";
        i += 1;
      } else {
        insideQuotes = !insideQuotes;
      }
      continue;
    }

    if (char === "," && !insideQuotes) {
      values.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current);
  return values;
}

function normalizeCsvValue(value) {
  const trimmed = value.replace(/\r/g, "").trim();
  if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
    return trimmed.slice(1, -1).replace(/""/g, "\"").trim();
  }
  return trimmed;
}

function parseLimitValue(value) {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}



function extractSpuId(url) {
  const match = url.match(/products\/(\d+)/);
  return match ? match[1] : null;
}

async function readProducts() {
  let rawCsv;
  try {
    rawCsv = await fs.readFile(PRODUCTS_CSV_PATH, "utf8");
  } catch (error) {
    throw new Error(`Unable to read Products.csv: ${error.message}`);
  }

  const rows = rawCsv
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0);

  if (rows.length < 2) {
    throw new Error("Products.csv must contain at least one product row.");
  }

  const headers = splitCsvRow(rows[0]).map(normalizeCsvValue);
  const spIndex = findHeaderIndex(headers, "sp");
  const urlIndex = findHeaderIndex(headers, "url");
  const spuIndex = findHeaderIndex(headers, "spuid", "spu_id");
  const skuSingleIndex = findHeaderIndex(
    headers,
    "sku_single",
    "skuid_single",
    "sku_single_id"
  );
  const skuSetIndex = findHeaderIndex(
    headers,
    "skuid_set",
    "sku_set",
    "sku_set_id"
  );
  const limitSingleIndex = findHeaderIndex(headers, "limit_single");
  const limitSetIndex = findHeaderIndex(headers, "limit_set");

  if (spIndex === -1 || urlIndex === -1) {
    throw new Error('Products.csv header must contain "sp" and "url" columns.');
  }

  if (skuSingleIndex === -1 || skuSetIndex === -1) {
    throw new Error(
      'Products.csv header must contain "sku_single" and "skuid_set" columns.'
    );
  }

  if (limitSingleIndex === -1 || limitSetIndex === -1) {
    throw new Error(
      'Products.csv header must contain "limit_single" and "limit_set" columns.'
    );
  }

  return rows.slice(1).map((line, index) => {
    const cells = splitCsvRow(line).map(normalizeCsvValue);
    const name = cells[spIndex];
    const url = cells[urlIndex];
    const csvSpuId = spuIndex === -1 ? "" : cells[spuIndex];
    const skuSingleId = skuSingleIndex === -1 ? "" : cells[skuSingleIndex];
    const skuSetId = skuSetIndex === -1 ? "" : cells[skuSetIndex];
    const limitSingleRaw =
      limitSingleIndex === -1 ? "" : cells[limitSingleIndex];
    const limitSetRaw = limitSetIndex === -1 ? "" : cells[limitSetIndex];

    if (!name || !url) {
      throw new Error(
        `Invalid row ${index + 2} in Products.csv. Expected values for "sp" and "url".`
      );
    }

    const inferredSpuId = extractSpuId(url);
    const spuId = (csvSpuId || inferredSpuId || "").trim();

    if (!spuId) {
      throw new Error(
        `Invalid row ${index + 2} in Products.csv. Provide "spuid" or ensure the URL contains the numeric product ID.`
      );
    }

    return {
      name,
      url,
      spuId,
      skuSingleId: skuSingleId || null,
      skuSetId: skuSetId || null,
      limitSingle: parseLimitValue(limitSingleRaw),
      limitSet: parseLimitValue(limitSetRaw),
      buyNowTitle: deriveBuyNowTitle(url, name),
    };
  });
}


function resolveDesiredConcurrency(defaultValue) {
  const envKeys = [
    "PRODUCT_CHECK_CONCURRENCY",
    "CONCURRENT_PRODUCT_CHECKS",
    "PRODUCT_CHECK_BATCH_SIZE",
  ];

  for (const key of envKeys) {
    const rawValue = process.env[key];
    if (!rawValue) {
      continue;
    }

    const parsedValue = Number.parseInt(rawValue, 10);
    if (Number.isNaN(parsedValue) || parsedValue <= 0) {
      console.warn(
        `Ignoring invalid concurrency value for ${key}: ${rawValue}`
      );
      continue;
    }

    return parsedValue;
  }

  return defaultValue;
}

function createResponseHandler(product) {
  return async (response) => {
    if (shuttingDown) {
      return;
    }

    try {
      const responseUrl = response.url();
      if (!responseUrl.includes("productDetails?spuId=")) {
        return;
      }

      if (product.spuId && !responseUrl.includes(`spuId=${product.spuId}`)) {
        return;
      }

      const json = await response.json();
      const skus = json?.data?.skus;

      if (!Array.isArray(skus)) {
        return;
      }

      for (let index = 0; index < skus.length; index += 1) {
        const sku = skus[index];
        const stock = sku?.stock?.onlineStock;

        if (typeof stock === "undefined") {
          continue;
        }

        const key = `${product.url}#${index}`;
        const previousStock = lastKnownStocks.get(key);
        lastKnownStocks.set(key, stock);

        const variantKind = resolveVariantKind(product, index, sku);

        if (
          stock > 0 &&
          stock !== previousStock &&
          index < 2 &&
          variantKind !== "other"
        ) {
          await notifyStock(product, index, stock, sku);
        }
      }
    } catch (error) {
      // Ignore non-JSON responses or pages that close while downloading.
    }
  };
}

async function safeClosePage(page) {
  if (!page) {
    return;
  }

  activePages.delete(page);

  try {
    if (typeof page.isClosed === "function" && page.isClosed()) {
      return;
    }
    await page.close({ runBeforeUnload: false });
  } catch (error) {
    if (!shuttingDown) {
      console.warn("Error closing page:", error);
    }
  }
}

async function closeAllPages() {
  const pages = Array.from(activePages);
  await Promise.all(pages.map((page) => safeClosePage(page)));
}

async function safeCloseAllBrowsers() {
  if (!proxyPoolRef) {
    return;
  }

  try {
    await proxyPoolRef.shutdown();
  } catch (error) {
    if (!shuttingDown) {
      console.warn("Error closing proxy browsers:", error);
    }
  } finally {
    proxyPoolRef = null;
  }
}

async function handlePopmartBlock(product) {
  if (popmartBlockHandled) {
    return;
  }

  popmartBlockHandled = true;

  console.error(
    "Detected Pop Mart block page while loading " + product.name + ". Initiating shutdown."
  );

  const messageLines = [
    "[ALERT] Pop Mart da gioi han truy cap bot.",
    "San pham: " + product.name,
    "URL: " + product.url,
    "Thoi gian: " + new Date().toISOString(),
  ];

  await sendTelegramMessage(messageLines.join("\n"));

  gracefulShutdown().catch((error) => {
    console.error("Error during shutdown after Pop Mart block:", error);
  });
}

async function checkProduct(product, proxySession) {
  if (shuttingDown) {
    return { success: false, failureReason: "Shutting down" };
  }

  let page = null;
  let success = false;
  let failureReason = null;
  const responseHandler = createResponseHandler(product);
  const proxyLabel = proxySession?.label ?? "proxy";

  try {
    const browser = await proxySession.ensureBrowser();
    page = await browser.newPage();
    activePages.add(page);

    const { username, password } = proxySession.config;
    if (username && password) {
      await page.authenticate({ username, password });
    }

    await page.setExtraHTTPHeaders({
      "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    });

    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    );

    page.on("response", responseHandler);

    console.log(`Loading ${product.name} via proxy ${proxyLabel}`);

    await page.goto(product.url, {
      waitUntil: "networkidle2",
      timeout: PRODUCT_PAGE_TIMEOUT,
    });

    if (!shuttingDown) {
      const pageHtml = await page.content();
      if (isPopmartBlockPage(pageHtml)) {
        failureReason = "Pop Mart block detected";
        await handlePopmartBlock(product);
        return { success: false, failureReason };
      }

      await randomDelay(PER_PRODUCT_DELAY.min, PER_PRODUCT_DELAY.max);
      success = true;
    } else {
      failureReason = "Shutting down";
    }
  } catch (error) {
    if (shuttingDown) {
      failureReason = "Shutting down";
      return { success: false, failureReason };
    }

    if (error instanceof TimeoutError) {
      failureReason = `Timeout after ${PRODUCT_PAGE_TIMEOUT}ms`;
      console.warn(
        `Skipping ${product.name} via proxy ${proxyLabel} after ${PRODUCT_PAGE_TIMEOUT}ms without response.`
      );
    } else {
      failureReason = error?.message || "Unknown navigation error";
      console.error(
        `Failed to load ${product.url} via proxy ${proxyLabel}:`,
        error
      );
    }
  } finally {
    if (page) {
      if (typeof page.off === "function") {
        page.off("response", responseHandler);
      } else if (typeof page.removeListener === "function") {
        page.removeListener("response", responseHandler);
      }
    }

    await safeClosePage(page);
  }

  return { success, failureReason };
}

async function processProductWithProxyRetries(product) {
  if (!proxyPoolRef) {
    throw new Error("Proxy pool is not initialized.");
  }

  const attemptedSessions = new Set();
  let attempts = 0;
  let lastFailureReason = null;

  while (!shuttingDown) {
    const poolSize = proxyPoolRef.size;

    if (poolSize <= 0) {
      lastFailureReason = lastFailureReason || "No proxies available in the pool";
      break;
    }

    const currentPoolSize = poolSize;

    for (const session of Array.from(attemptedSessions)) {
      if (!proxyPoolRef.sessions.includes(session)) {
        attemptedSessions.delete(session);
      }
    }

    if (attemptedSessions.size >= currentPoolSize) {
      break;
    }

    let session = null;
    try {
      session = await proxyPoolRef.acquire();
    } catch (error) {
      if (!shuttingDown) {
        console.error("Failed to acquire proxy session:", error);
      }
      lastFailureReason = error?.message || "Unable to acquire proxy session";
      break;
    }

    if (currentPoolSize > 1 && attemptedSessions.has(session)) {
      proxyPoolRef.release(session);
      continue;
    }

    attemptedSessions.add(session);
    attempts += 1;

    let result;
    try {
      result = await checkProduct(product, session);
    } catch (error) {
      result = { success: false, failureReason: error?.message || String(error) };
      if (!shuttingDown) {
        console.error(
          `Unexpected error while loading ${product.url} via proxy ${session?.label ?? "proxy"}:`,
          error
        );
      }
    } finally {
      if (session) {
        if (proxyPoolRef) {
          proxyPoolRef.release(session);
        } else {
          try {
            await session.close();
          } catch (closeError) {
            if (!shuttingDown) {
              console.warn(
                `Error closing proxy session ${session?.label ?? "proxy"}:`,
                closeError
              );
            }
          }
        }
      }
    }

    if (result?.success) {
      if (!shuttingDown && attempts > 1) {
        console.log(
          `Loaded ${product.name} successfully after ${attempts} proxy attempts.`
        );
      }
      return true;
    }

    lastFailureReason = result?.failureReason || lastFailureReason;

    const remainingCapacity = proxyPoolRef.size;
    if (!shuttingDown && remainingCapacity > attemptedSessions.size) {
      console.log(
        `Retrying ${product.name} with a different proxy (attempt ${attempts + 1}).`
      );
    }
  }

  if (!shuttingDown) {
    const attemptsText =
      attempts === 0
        ? `No proxy attempts could be made for ${product.name}.`
        : `Exhausted ${attempts} proxy attempt${attempts === 1 ? "" : "s"} for ${product.name}.`;
    const reasonText = lastFailureReason ? ` Last error: ${lastFailureReason}.` : "";
    console.warn(`${attemptsText}${reasonText}`);
  }

  return false;
}

const lastKnownStocks = new Map();

async function notifyStock(product, skuIndex, stock, skuData) {
  const lines = [];
  const variantKind = resolveVariantKind(product, skuIndex, skuData);

  if (variantKind === "single") {
    lines.push(`Restock box le: ${product.name}`);
  } else if (variantKind === "set") {
    lines.push(`Restock full set: ${product.name}`);
  } else {
    lines.push(`Restock ship: ${product.name}`);
  }

  lines.push(`So luong online: ${stock}`);

  const buyNowLink = createBuyNowLink(product, skuIndex, skuData);
  if (buyNowLink) {
    lines.push(`Mua ngay: ${buyNowLink}`);
  } else {
    lines.push(product.url);
  }

  await sendTelegramMessage(lines.join("\n"));
}



async function run() {
  await loadEnvFromFile();

  const products = await readProducts();
  console.log(`Loaded ${products.length} products from Products.csv.`);

  const proxies = await readProxyList();
  console.log(`Loaded ${proxies.length} proxies from Proxy.txt.`);

  proxyPoolRef = new ProxyPool(proxies);
  await proxyPoolRef.init();

  const availableProxyCount = proxyPoolRef.size;
  if (availableProxyCount !== proxies.length) {
    console.warn(
      `Initialized ${availableProxyCount} of ${proxies.length} proxies after filtering failures.`
    );
  }
  console.log(`Initialized ${availableProxyCount} proxy browsers.`);

  const desiredConcurrency = resolveDesiredConcurrency(DEFAULT_CONCURRENT_CHECKS);
  const constraints = [];

  if (products.length < desiredConcurrency) {
    constraints.push(`products (${products.length})`);
  }

  if (availableProxyCount < desiredConcurrency) {
    constraints.push(`proxies (${availableProxyCount})`);
  }

  const targetConcurrency = Math.max(
    1,
    Math.min(desiredConcurrency, products.length, availableProxyCount)
  );
  let currentConcurrency = Math.min(3, targetConcurrency);

  if (targetConcurrency < desiredConcurrency) {
    if (constraints.length > 0) {
      console.warn(
        `Reducing concurrency from ${desiredConcurrency} to ${targetConcurrency} to match available ${constraints.join(" and ")}.`
      );
    } else {
      console.warn(
        `Reducing concurrency from ${desiredConcurrency} to ${targetConcurrency}.`
      );
    }
  }

  if (currentConcurrency >= targetConcurrency) {
    console.log(`Using up to ${targetConcurrency} concurrent checks per pass.`);
  } else {
    console.log(
      `Target concurrency ${targetConcurrency}. Warmup starting with ${currentConcurrency} concurrent check and increasing by 1 after each full pass.`
    );
  }

  try {
    while (!shuttingDown) {
      await waitUntilActiveWindow();

      if (shuttingDown) {
        break;
      }

      const passStartMs = Date.now();
      const passConcurrencyLimit = Math.min(currentConcurrency, targetConcurrency);
      let endedDueToWindow = false;
      const runningTasks = new Set();
      const taskPromises = [];

      for (const product of products) {
        if (shuttingDown) {
          endedDueToWindow = true;
          break;
        }

        if (!isWithinActiveWindow()) {
          console.log(
            "Monitoring window closed (outside 08:00-19:00 GMT+7). Pausing until it reopens."
          );
          endedDueToWindow = true;
          break;
        }

        while (runningTasks.size >= passConcurrencyLimit && !shuttingDown) {
          await Promise.race(runningTasks);
        }

        if (shuttingDown) {
          endedDueToWindow = true;
          break;
        }

        const execution = (async () => {
          if (shuttingDown) {
            return;
          }

          await processProductWithProxyRetries(product);
        })().catch((error) => {
          if (!shuttingDown) {
            console.error(
              `Unexpected error while processing ${product.url}:`,
              error
            );
          }
        });

        let taskPromise;
        taskPromise = execution.finally(() => {
          runningTasks.delete(taskPromise);
        });

        runningTasks.add(taskPromise);
        taskPromises.push(taskPromise);
      }

      await Promise.all(taskPromises);

      if (shuttingDown) {
        break;
      }

      if (endedDueToWindow) {
        continue;
      }

      if (!isWithinActiveWindow()) {
        console.log(
          "Monitoring window closed after completing the product list. Waiting for the next window."
        );
        continue;
      }

      const passDurationMs = Date.now() - passStartMs;
      console.log(
        `Completed one pass through the product list in ${passDurationMs}ms (limit ${passConcurrencyLimit}).`
      );

      if (currentConcurrency < targetConcurrency) {
        currentConcurrency += 1;
        const nextLimit = Math.min(currentConcurrency, targetConcurrency);
        console.log(`Increasing allowed concurrency to ${nextLimit}.`);
      }

      await randomDelay();
    }
  } finally {
    await closeAllPages();
    await safeCloseAllBrowsers();

    cancelAllDelays();

    if (resolveShutdownPromise) {
      resolveShutdownPromise();
      resolveShutdownPromise = null;
    }
  }
}

run()
  .then(() => {
    if (!shuttingDown) {
      process.exit(0);
    }
  })
  .catch((error) => {
    if (shuttingDown) {
      console.error("Error during shutdown:", error);
      process.exit(1);
      return;
    }

    console.error("Fatal error:", error);
    process.exit(1);
  });









