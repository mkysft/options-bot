const path = require("node:path");
const { spawn } = require("node:child_process");
const { setTimeout: sleep } = require("node:timers/promises");
const { app, BrowserWindow, dialog } = require("electron");

const projectRoot = path.resolve(__dirname, "..");
const isDev = process.env.ELECTRON_DEV === "1" || !app.isPackaged;
const managedBackend = process.env.ELECTRON_MANAGED_BACKEND !== "0";
const bunBinary = (process.env.BUN_BINARY || "bun").trim();
const startupTimeoutMs = Number(process.env.ELECTRON_STARTUP_TIMEOUT_MS || 90_000);
const healthPollMs = Number(process.env.ELECTRON_HEALTH_POLL_MS || 500);
const devUiUrl = (process.env.ELECTRON_UI_URL_DEV || "http://127.0.0.1:5173/ui/").trim();
const appUiUrl = (process.env.ELECTRON_UI_URL_APP || "http://127.0.0.1:8000/ui/").trim();
const healthUrl = (process.env.ELECTRON_HEALTH_URL || "http://127.0.0.1:8000/health").trim();

let mainWindow = null;
let backendProcess = null;
let backendStartupError = null;

const targetUiUrl = isDev ? devUiUrl : appUiUrl;
const targetReadyUrl = managedBackend ? (isDev ? devUiUrl : healthUrl) : targetUiUrl;

function prefixLog(prefix, message) {
  const text = String(message ?? "").trim();
  if (!text) return;
  process.stdout.write(`[electron:${prefix}] ${text}\n`);
}

function spawnBackend() {
  if (!managedBackend) {
    prefixLog("backend", "managed backend disabled; attaching to existing server.");
    return;
  }

  const args = isDev ? ["run", "dev"] : ["run", "start"];
  backendProcess = spawn(bunBinary, args, {
    cwd: projectRoot,
    env: {
      ...process.env,
      ELECTRON_DESKTOP: "1"
    },
    stdio: ["ignore", "pipe", "pipe"]
  });

  backendProcess.stdout.on("data", (chunk) => prefixLog("backend", chunk.toString()));
  backendProcess.stderr.on("data", (chunk) => prefixLog("backend", chunk.toString()));
  backendProcess.on("error", (error) => {
    backendStartupError = error;
    prefixLog("backend", `failed to spawn backend: ${error.message}`);
  });
  backendProcess.on("exit", (code, signal) => {
    prefixLog("backend", `exited (code=${code ?? "null"}, signal=${signal ?? "null"})`);
    backendProcess = null;
  });
}

async function isUrlReady(url) {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 2_500);
    const response = await fetch(url, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal
    });
    clearTimeout(timeout);
    return response.status >= 200 && response.status < 500;
  } catch {
    return false;
  }
}

async function waitForUrl(url, timeoutMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt <= timeoutMs) {
    if (backendStartupError) throw backendStartupError;
    if (await isUrlReady(url)) return;
    await sleep(healthPollMs);
  }
  throw new Error(`Timed out waiting for ${url} after ${timeoutMs}ms`);
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1600,
    height: 980,
    minWidth: 1200,
    minHeight: 760,
    show: false,
    backgroundColor: "#f8fafc",
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      preload: path.join(__dirname, "preload.cjs")
    }
  });

  mainWindow.once("ready-to-show", () => {
    mainWindow.show();
  });

  if (isDev) {
    mainWindow.webContents.openDevTools({ mode: "detach" });
  }

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

function stopBackend() {
  if (!backendProcess) return;
  try {
    backendProcess.kill("SIGTERM");
  } catch {
    // Ignore shutdown errors.
  }
  backendProcess = null;
}

async function bootstrap() {
  spawnBackend();
  await waitForUrl(targetReadyUrl, startupTimeoutMs);
  createWindow();
  if (!mainWindow) return;
  await mainWindow.loadURL(targetUiUrl);
}

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("before-quit", () => {
  stopBackend();
});

app.whenReady().then(async () => {
  try {
    await bootstrap();
  } catch (error) {
    const reason = (error instanceof Error ? error.message : String(error)).trim();
    dialog.showErrorBox(
      "Options Bot Desktop Startup Failed",
      [
        `Could not start the desktop app.`,
        "",
        `Target readiness URL: ${targetReadyUrl}`,
        `Target UI URL: ${targetUiUrl}`,
        "",
        `Reason: ${reason}`
      ].join("\n")
    );
    app.quit();
  }
});

