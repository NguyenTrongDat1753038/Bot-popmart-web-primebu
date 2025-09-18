// install: npm install puppeteer
import fs from "fs/promises";
import puppeteer from "puppeteer";

function randomDelay(min = 3000, max = 5000) {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return delay(ms);
}

const PRODUCTS_CSV_PATH = new URL("./Products.csv", import.meta.url);
const PER_PRODUCT_DELAY = { min: 2000, max: 3000 };
const ORDER_CONFIRMATION_URL = "https://www.popmart.com/vn/order-confirmation";
const GMT7_OFFSET_MINUTES = 7 * 60;
const ACTIVE_WINDOW = { startHour: 8, endHour: 21 };
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
    `Outside monitoring window (08:00-21:00 GMT+7). Waiting ${formatDuration(waitMs)} before resuming.`
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

let shuttingDown = false;
let browserRef = null;
let pageRef = null;
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

async function ensureTelegramConfig() {
  if (telegramConfig.token && telegramConfig.chatId) {
    return telegramConfig;
  }

  if (telegramConfigLoadAttempted) {
    return telegramConfig;
  }

  telegramConfigLoadAttempted = true;

  try {
    const envPath = new URL("./.env", import.meta.url);
    const content = await fs.readFile(envPath, "utf8");
    const values = parseDotEnv(content);

    if (!telegramConfig.token && values.TELEGRAM_BOT_TOKEN) {
      telegramConfig.token = values.TELEGRAM_BOT_TOKEN.trim();
    }

    if (!telegramConfig.chatId && values.TELEGRAM_CHAT_ID) {
      telegramConfig.chatId = values.TELEGRAM_CHAT_ID.trim();
    }
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.warn("Unable to load .env file for Telegram config:", error);
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
  if (skuIndex === 0) {
    return product.skuSingleId || extractSkuIdFromData(skuData);
  }

  if (skuIndex === 1) {
    return product.skuSetId || extractSkuIdFromData(skuData);
  }

  return extractSkuIdFromData(skuData);
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

  let count = 1;
  if (skuIndex === 0) {
    count = 12;
  } else if (skuIndex === 1) {
    count = 2;
  }

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

  if (spIndex === -1 || urlIndex === -1) {
    throw new Error('Products.csv header must contain "sp" and "url" columns.');
  }

  if (skuSingleIndex === -1 || skuSetIndex === -1) {
    throw new Error(
      'Products.csv header must contain "sku_single" and "skuid_set" columns.'
    );
  }

  return rows.slice(1).map((line, index) => {
    const cells = splitCsvRow(line).map(normalizeCsvValue);
    const name = cells[spIndex];
    const url = cells[urlIndex];
    const csvSpuId = spuIndex === -1 ? "" : cells[spuIndex];
    const skuSingleId = skuSingleIndex === -1 ? "" : cells[skuSingleIndex];
    const skuSetId = skuSetIndex === -1 ? "" : cells[skuSetIndex];

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
      buyNowTitle: deriveBuyNowTitle(url, name),
    };
  });
}

const lastKnownStocks = new Map();

async function notifyStock(product, skuIndex, stock, skuData) {
  const lines = [];

  if (skuIndex === 0) {
    lines.push(`Restock box le: ${product.name}`);
  } else if (skuIndex === 1) {
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
  const products = await readProducts();
  console.log(`Loaded ${products.length} products from Products.csv.`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-blink-features=AutomationControlled",
    ],
  });

  browserRef = browser;

  const page = await browser.newPage();

  pageRef = page;

  await page.setExtraHTTPHeaders({
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
  });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  );

  let currentProduct = null;

  page.on("response", async (response) => {
    if (!currentProduct || shuttingDown) {
      return;
    }

    try {
      const responseUrl = response.url();
      if (!responseUrl.includes("productDetails?spuId=")) {
        return;
      }

      if (
        currentProduct.spuId &&
        !responseUrl.includes(`spuId=${currentProduct.spuId}`)
      ) {
        return;
      }

      const json = await response.json();
      const skus = json?.data?.skus;

      if (Array.isArray(skus)) {
        for (let index = 0; index < skus.length; index += 1) {
          const sku = skus[index];
          const stock = sku?.stock?.onlineStock;

          if (typeof stock !== "undefined") {
            const key = `${currentProduct.url}#${index}`;
            const previousStock = lastKnownStocks.get(key);
            lastKnownStocks.set(key, stock);

            /*console.log(
              `[${currentProduct.name}] SKU${index + 1} onlineStock: ${stock}`
            );*/

            if (stock > 0 && stock !== previousStock) {
              await notifyStock(currentProduct, index, stock, sku);
            }
          }
        }
      }
    } catch (error) {
      // Ignore non-JSON responses.
    }
  });

  try {
    while (!shuttingDown) {
      await waitUntilActiveWindow();

      if (shuttingDown) {
        break;
      }

      let endedDueToWindow = false;

      for (const product of products) {
        if (shuttingDown) {
          endedDueToWindow = true;
          break;
        }

        if (!isWithinActiveWindow()) {
          console.log(
            "Monitoring window closed (outside 08:00-21:00 GMT+7). Pausing until it reopens."
          );
          endedDueToWindow = true;
          break;
        }

        currentProduct = product;
        console.log(`Loading ${product.name}`);

        try {
          await page.goto(product.url, { waitUntil: "networkidle2" });
        } catch (error) {
          if (shuttingDown) {
            endedDueToWindow = true;
            break;
          }

          console.error(`Failed to load ${product.url}:`, error);
          continue;
        }

        if (shuttingDown) {
          endedDueToWindow = true;
          break;
        }

        await randomDelay(PER_PRODUCT_DELAY.min, PER_PRODUCT_DELAY.max);
      }

      currentProduct = null;

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

      console.log("Completed one pass through the product list.");

      if (shuttingDown) {
        break;
      }

      await randomDelay();
    }
  } finally {
    currentProduct = null;

    try {
      if (pageRef) {
        await pageRef.close();
      }
    } catch (error) {
      if (!shuttingDown) {
        console.warn("Error closing page:", error);
      }
    } finally {
      pageRef = null;
    }

    try {
      if (browserRef) {
        await browserRef.close();
      }
    } catch (error) {
      if (!shuttingDown) {
        console.warn("Error closing browser:", error);
      }
    } finally {
      browserRef = null;
    }

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

