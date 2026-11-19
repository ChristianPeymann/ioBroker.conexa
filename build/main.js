"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var utils = __toESM(require("@iobroker/adapter-core"));
var crypto = __toESM(require("node:crypto"));
var https = __toESM(require("node:https"));
var import_node_url = require("node:url");
function isGatewayRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function asGatewayRecord(value) {
  return isGatewayRecord(value) ? value : void 0;
}
function asGatewayArray(value) {
  return Array.isArray(value) ? value : [];
}
function asGatewayRecords(value) {
  return asGatewayArray(value).filter(isGatewayRecord);
}
function isCachedUsagePoint(value) {
  if (!isGatewayRecord(value)) {
    return false;
  }
  return typeof value.usagePointId === "string" && typeof value.usagePointName === "string" && typeof value.cleanId === "string" && typeof value.tafNumber === "string" && typeof value.tafState === "string" && typeof value.meterId === "string" && typeof value.meteringPointId === "string" && typeof value.database === "string" && (value.obis === void 0 || typeof value.obis === "string") && typeof value.scaler === "number" && typeof value.unit === "string";
}
function asStateValue(value, fallback = "") {
  if (!value) {
    return fallback;
  }
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  return fallback;
}
function toTimestamp(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : void 0;
  }
  if (typeof value !== "string" || value.trim() === "") {
    return void 0;
  }
  const trimmedValue = value.trim();
  const numericValue = Number(trimmedValue);
  if (/^\d+(?:\.\d+)?$/.test(trimmedValue) && Number.isFinite(numericValue)) {
    return numericValue;
  }
  const timestamp = Date.parse(trimmedValue);
  return Number.isFinite(timestamp) ? timestamp : void 0;
}
function toText(value, fallback = "") {
  var _a;
  if (value === null || value === void 0) {
    return fallback;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return (_a = JSON.stringify(value)) != null ? _a : fallback;
}
function getErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}
class Conexa extends utils.Adapter {
  pollTimer;
  baseUrl = "";
  httpsAgent = new https.Agent({ rejectUnauthorized: false, keepAlive: true });
  usagePointsCache = [];
  lastCatalogUpdate = 0;
  latestReadingTimestamp = 0;
  latestMarketEventTimestamp = 0;
  pollRetryCount = 0;
  readingsRawLogged = false;
  cachedUsagePointMissingObis = false;
  missingObisRefreshAttempted = false;
  /**
   * [DE] Initialisiert den Adapter | [EN] Initializes the adapter
   * - DE: Registriert Handler für die Adapter-Ereignisse.
   * - EN: Registers handlers for adapter events.
   *
   * @param options Adapter-Optionen | Adapter options
   */
  constructor(options = {}) {
    super({
      ...options,
      name: "conexa"
    });
    this.on("ready", () => {
      void this.onReady().catch((error) => {
        this.log.error(`Error initializing the adapter: ${getErrorMessage(error)}`);
      });
    });
    this.on("stateChange", (id, state) => {
      void this.onStateChange(id, state).catch((error) => {
        this.log.error(`Error processing ${id}: ${getErrorMessage(error)}`);
      });
    });
    this.on("unload", this.onUnload.bind(this));
  }
  /**
   * [DE] Startet den Adapter | [EN] Starts the adapter
   * - DE: Prüft die Konfiguration und startet den ersten Gateway-Abruf samt Polling.
   * - EN: Validates configuration and starts the initial gateway request and polling.
   */
  async onReady() {
    var _a;
    await this.setStateAsync("info.connection", false, true);
    await this.ensureGatewayStatsObjects();
    const rawIp = ((_a = this.config.useIpv6 ? this.config.ipV6 : this.config.ipV4) == null ? void 0 : _a.trim()) || "";
    const targetHost = this.config.useIpv6 && !rawIp.startsWith("[") ? `[${rawIp}]` : rawIp;
    if (!targetHost || !this.config.username || !this.config.password) {
      this.log.warn("Please configure the IP address, username, and password in the adapter settings!");
      return;
    }
    this.baseUrl = `https://${targetHost}:443/smgw/m2m/`;
    this.log.info(`Connecting to CONEXA 3.0 at ${this.baseUrl} ...`);
    this.subscribeStates("gateway.triggerSelfTest");
    this.subscribeStates("usagePoints.refreshCatalog");
    await this.loadCatalogFromState();
    const hasNewData = await this.pollGatewayData();
    const intervalMinutes = Math.max(1, Number(this.config.pollInterval) || 15);
    this.log.info(`Polling interval started: every ${intervalMinutes} minutes.`);
    await this.scheduleNextPoll(hasNewData);
  }
  /**
   * Reagiert auf Klicks der Buttons im ioBroker Objektbaum
   *
   * @param id Vollständige ID des geänderten Datenpunkts
   * @param state Neuer Zustand oder null, wenn der Datenpunkt gelöscht wurde
   */
  async onStateChange(id, state) {
    if (!state || state.ack) {
      return;
    }
    if (id.endsWith(".gateway.triggerSelfTest") && state.val === true) {
      this.log.info("Manual self-test requested via button...");
      await this.triggerSelfTest();
      await this.setStateAsync("gateway.triggerSelfTest", false, true);
    } else if (id.endsWith(".usagePoints.refreshCatalog") && state.val === true) {
      this.log.info("Manual tariff catalog refresh requested...");
      await this.discoverUsagePoints(true);
      await this.pollReadings(true);
      await this.setStateAsync("usagePoints.refreshCatalog", false, true);
    }
  }
  /**
   * Sendet einen POST-Request mit Digest-Authentifizierung an die CONEXA 3.0
   * und protokolliert aussagekräftige Verbindungs- und Fehlerdetails.
   *
   * @param payload Request-Daten für die CONEXA-API
   */
  async sendConexaRequest(payload) {
    var _a;
    const startTime = Date.now();
    let requestStep = "Step 1 (initial request/challenge)";
    const method = toText(payload.method);
    try {
      let requestUrl = new import_node_url.URL(this.baseUrl);
      const body = JSON.stringify(payload);
      const isRedirect = (statusCode) => [301, 302, 307, 308].includes(statusCode);
      const maxRedirects = 5;
      this.log.debug(`[API] Sending unauthenticated initial request for method "${method}" to ${this.baseUrl}`);
      let response = await this.requestConexa(requestUrl, body);
      this.log.debug(
        `[API] Initial response for "${method}": status ${response.statusCode} (duration: ${Date.now() - startTime}ms)`
      );
      let redirectCount = 0;
      while (isRedirect(response.statusCode)) {
        this.log.debug(
          `[API] Gateway sent redirect (${response.statusCode}) to Location: ${response.location}`
        );
        if (!response.location) {
          throw new Error(`Gateway redirect (${response.statusCode}) does not contain a Location header.`);
        }
        if (++redirectCount > maxRedirects) {
          throw new Error(`Too many gateway redirects (more than ${maxRedirects}).`);
        }
        requestUrl = new import_node_url.URL(response.location, requestUrl);
        this.baseUrl = requestUrl.toString();
        response = await this.requestConexa(requestUrl, body);
        this.log.debug(`[API] Redirect response for "${method}": status ${response.statusCode}`);
      }
      if (response.statusCode === 200) {
        return this.parseGatewayResponse(response.body);
      }
      if (response.statusCode === 403) {
        throw new Error(
          `Access denied (403 Forbidden). The configured user does not have permission for API method "${method}".`
        );
      }
      if (response.statusCode === 401) {
        this.log.debug(
          `[API] Digest challenge received. WWW-Authenticate header: ${JSON.stringify(response.wwwAuthenticate)}`
        );
      }
      if (response.statusCode !== 401 || !((_a = response.wwwAuthenticate) == null ? void 0 : _a.length)) {
        throw new Error(
          `Unexpected HTTP status ${response.statusCode} while connecting. Response: ${response.body || "No response from gateway"}`
        );
      }
      const authorization = this.createDigestAuthorization(response.wwwAuthenticate, requestUrl);
      this.log.debug(
        `[API] Digest authorization header generated (value will not be logged). Sending authenticated request for "${method}"...`
      );
      requestStep = "Step 2 (authenticated request)";
      const authStartTime = Date.now();
      let authenticatedResponse = await this.requestConexa(requestUrl, body, authorization);
      this.log.debug(
        `[API] Authenticated response for "${method}": status ${authenticatedResponse.statusCode} (duration: ${Date.now() - authStartTime}ms)`
      );
      redirectCount = 0;
      while (isRedirect(authenticatedResponse.statusCode)) {
        this.log.debug(
          `[API] Gateway sent redirect (${authenticatedResponse.statusCode}) to Location: ${authenticatedResponse.location}`
        );
        if (!authenticatedResponse.location) {
          throw new Error(
            `Gateway redirect (${authenticatedResponse.statusCode}) does not contain a Location header.`
          );
        }
        if (++redirectCount > maxRedirects) {
          throw new Error(`Too many gateway redirects (more than ${maxRedirects}).`);
        }
        requestUrl = new import_node_url.URL(authenticatedResponse.location, requestUrl);
        this.baseUrl = requestUrl.toString();
        const redirectAuthorization = this.createDigestAuthorization(response.wwwAuthenticate, requestUrl);
        authenticatedResponse = await this.requestConexa(requestUrl, body, redirectAuthorization);
        this.log.debug(
          `[API] Authenticated redirect response for "${method}": status ${authenticatedResponse.statusCode}`
        );
      }
      if (authenticatedResponse.statusCode === 401) {
        throw new Error(
          "Authentication failed (401 Unauthorized). Please check the username and password in the adapter settings."
        );
      }
      if (authenticatedResponse.statusCode === 403) {
        throw new Error(
          `Access denied (403 Forbidden). The configured user does not have permission for API method "${method}".`
        );
      }
      if (authenticatedResponse.statusCode < 200 || authenticatedResponse.statusCode >= 300) {
        throw new Error(
          `Gateway reported HTTP error status ${authenticatedResponse.statusCode}: ${authenticatedResponse.body || "No details"}`
        );
      }
      return this.parseGatewayResponse(authenticatedResponse.body);
    } catch (error) {
      const message = getErrorMessage(error);
      this.log.error(`[API Error] Method "${method}" failed (${requestStep}): ${message}`);
      throw new Error(`API error (${method}, ${requestStep}): ${message}`);
    }
  }
  /**
   * [DE] Prüft eine Gateway-Antwort | [EN] Validates a gateway response
   * - DE: Parst JSON und stellt sicher, dass die API ein Objekt geliefert hat.
   * - EN: Parses JSON and ensures the API returned an object.
   *
   * @param body JSON-Antworttext | JSON response body
   */
  parseGatewayResponse(body) {
    const response = JSON.parse(body);
    if (!isGatewayRecord(response)) {
      throw new Error("Gateway response is not a JSON object.");
    }
    return response;
  }
  /**
   * Sendet einen JSON-POST-Request an das Gateway und liest die vollständige Antwort.
   *
   * @param requestUrl URL des Gateway-Endpunkts
   * @param body Serialisierter JSON-Request-Body
   * @param authorization Optionaler Digest-Authorization-Header
   */
  requestConexa(requestUrl, body, authorization) {
    return new Promise((resolve, reject) => {
      const bodyBuffer = Buffer.from(body, "utf8");
      const headers = {
        "Content-Type": "application/json",
        "Content-Length": bodyBuffer.byteLength
      };
      if (authorization) {
        headers.Authorization = authorization;
      }
      const request = https.request(
        requestUrl,
        {
          method: "POST",
          headers,
          agent: this.httpsAgent,
          timeout: 15e3
        },
        (response) => {
          const chunks = [];
          const wwwAuthenticate = [];
          for (let index = 0; index < response.rawHeaders.length; index += 2) {
            if (response.rawHeaders[index].toLowerCase() === "www-authenticate") {
              wwwAuthenticate.push(response.rawHeaders[index + 1]);
            }
          }
          response.on("data", (chunk) => {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          });
          response.on("error", reject);
          response.on("end", () => {
            var _a;
            resolve({
              statusCode: (_a = response.statusCode) != null ? _a : 0,
              body: Buffer.concat(chunks).toString("utf8"),
              wwwAuthenticate: wwwAuthenticate.length > 0 ? wwwAuthenticate : this.getAuthenticateHeaders(response.headers["www-authenticate"]),
              location: typeof response.headers.location === "string" ? response.headers.location : void 0
            });
          });
        }
      );
      request.on("error", reject);
      request.setTimeout(15e3, () => {
        request.destroy(new Error("Request timed out after 15000 ms"));
      });
      if (bodyBuffer.byteLength > 0) {
        request.write(bodyBuffer);
      }
      request.end();
    });
  }
  /**
   * Normalisiert WWW-Authenticate-Header, einschließlich zusammengeführter Challenges.
   *
   * @param header WWW-Authenticate-Wert aus den Response-Headern
   */
  getAuthenticateHeaders(header) {
    if (!header) {
      return [];
    }
    return (Array.isArray(header) ? header : [header]).flatMap((value) => value.split(/,\s*(?=Digest\s+)/i));
  }
  /**
   * Erstellt den Digest-Authorization-Header aus der Gateway-Challenge.
   *
   * @param challenges Werte der WWW-Authenticate-Header
   * @param requestUrl URL des Gateway-Endpunkts
   */
  createDigestAuthorization(challenges, requestUrl) {
    var _a;
    const parameterPattern = /(?:^|,\s*)([a-zA-Z][\w-]*)=(?:"((?:\\.|[^"])*)"|([^,\s]+))/g;
    const parsedChallenges = challenges.flatMap((challenge) => {
      var _a2, _b, _c;
      const digestParams = (_a2 = challenge.match(/^\s*Digest\s+(.+)$/i)) == null ? void 0 : _a2[1];
      if (!digestParams) {
        return [];
      }
      const params = {};
      for (const match of digestParams.matchAll(parameterPattern)) {
        params[match[1].toLowerCase()] = ((_b = match[2]) != null ? _b : match[3]).replace(/\\(.)/g, "$1");
      }
      const algorithm2 = (_c = params.algorithm) != null ? _c : "MD5";
      if (!params.realm || !params.nonce || !["MD5", "SHA-256"].includes(algorithm2.toUpperCase())) {
        return [];
      }
      return [{ params, algorithm: algorithm2.toUpperCase() }];
    });
    const selectedChallenge = (_a = parsedChallenges.find((challenge) => challenge.algorithm === "MD5")) != null ? _a : parsedChallenges.find((challenge) => challenge.algorithm === "SHA-256");
    if (!selectedChallenge) {
      throw new Error("No supported Digest challenge (MD5 or SHA-256) received from the gateway.");
    }
    const { realm, nonce, qop, opaque } = selectedChallenge.params;
    const algorithm = selectedChallenge.algorithm;
    const qopAuth = qop == null ? void 0 : qop.split(",").map((value) => value.trim()).find((value) => value.toLowerCase() === "auth");
    if (qop && !qopAuth) {
      throw new Error(`Unsupported Digest qop: ${qop}`);
    }
    const uri = `${requestUrl.pathname}${requestUrl.search}`;
    const hash = (value) => crypto.createHash(algorithm.toLowerCase().replace("-", "")).update(value).digest("hex");
    const ha1 = hash(`${this.config.username}:${realm}:${this.config.password}`);
    const ha2 = hash(`POST:${uri}`);
    const cnonce = crypto.randomBytes(8).toString("hex");
    const nc = "00000001";
    const response = qopAuth ? hash(`${ha1}:${nonce}:${nc}:${cnonce}:${qopAuth}:${ha2}`) : hash(`${ha1}:${nonce}:${ha2}`);
    const quote = (value) => `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
    const authorizationParams = [
      `username=${quote(this.config.username)}`,
      `realm=${quote(realm)}`,
      `nonce=${quote(nonce)}`,
      `uri=${quote(uri)}`,
      `response=${quote(response)}`
    ];
    if (qopAuth) {
      authorizationParams.push(`qop=${quote(qopAuth)}`);
    }
    authorizationParams.push(`algorithm=${quote(algorithm)}`);
    if (qopAuth) {
      authorizationParams.push(`nc=${quote(nc)}`, `cnonce=${quote(cnonce)}`);
    }
    if (opaque) {
      authorizationParams.push(`opaque=${quote(opaque)}`);
    }
    return `Digest ${authorizationParams.join(", ")}`;
  }
  /**
   * Lädt beim Adapterstart den zwischengespeicherten Tarif-Katalog aus den ioBroker-States
   */
  async loadCatalogFromState() {
    var _a;
    try {
      const catalogState = await this.getStateAsync("usagePoints.catalogJson");
      const updatedState = await this.getStateAsync("usagePoints.lastUpdated");
      if (catalogState && typeof catalogState.val === "string" && catalogState.val !== "[]") {
        const parsed = JSON.parse(catalogState.val);
        if (Array.isArray(parsed) && parsed.length > 0 && parsed.every(isCachedUsagePoint)) {
          this.usagePointsCache = parsed;
          this.cachedUsagePointMissingObis = this.usagePointsCache.some((up) => !up.obis);
          this.lastCatalogUpdate = (_a = toTimestamp(updatedState == null ? void 0 : updatedState.val)) != null ? _a : Date.now();
          this.log.info(
            `${this.usagePointsCache.length} tariffs (usage points) loaded from the local cache.`
          );
          for (const up of this.usagePointsCache) {
            if (!up.database) {
              up.database = "origin";
            }
            await this.ensureUsagePointObjects(up);
          }
        }
      }
    } catch (error) {
      this.log.warn(`Could not read the tariff cache; refreshing it from the gateway: ${getErrorMessage(error)}`);
      this.usagePointsCache = [];
    }
  }
  /**
   * Haupt-Abrufroutine (wird im eingestellten Intervall ausgeführt)
   */
  async pollGatewayData() {
    try {
      await this.pollSmgwInfo();
      await this.pollLogs();
      const catalogAgeHours = (Date.now() - this.lastCatalogUpdate) / (1e3 * 60 * 60);
      let isInitialOrDailyRefresh = false;
      const refreshMissingObis = this.cachedUsagePointMissingObis && !this.missingObisRefreshAttempted;
      if (this.usagePointsCache.length === 0 || catalogAgeHours >= 24 || refreshMissingObis) {
        if (refreshMissingObis) {
          this.missingObisRefreshAttempted = true;
          this.cachedUsagePointMissingObis = false;
        }
        await this.discoverUsagePoints(refreshMissingObis);
        isInitialOrDailyRefresh = true;
      }
      const hasNewData = await this.pollReadings(isInitialOrDailyRefresh);
      await this.setStateAsync("info.connection", true, true);
      return hasNewData;
    } catch (error) {
      this.log.error(`Error retrieving data from the gateway: ${getErrorMessage(error)}`);
      await this.setStateAsync("info.connection", false, true);
      return false;
    }
  }
  /**
   * Plant den nächsten Abruf am konfigurierten Messwertraster oder einen kurzen Daten-Retry.
   *
   * @param hasNewData Gibt an, ob sich seit dem vorigen Abruf ein Messzeitpunkt geändert hat
   */
  async scheduleNextPoll(hasNewData) {
    const intervalMs = Math.max(1, Number(this.config.pollInterval) || 15) * 60 * 1e3;
    const correctionOffsetMs = 15e3;
    const retryDelayMs = 3e4;
    let delayMs;
    if (hasNewData) {
      this.pollRetryCount = 0;
    } else if (this.pollRetryCount < 3) {
      this.pollRetryCount++;
      delayMs = retryDelayMs;
    } else {
      this.pollRetryCount = 0;
    }
    if (delayMs === void 0) {
      const referenceTimestamp = Math.max(this.latestReadingTimestamp, this.latestMarketEventTimestamp);
      const now = Date.now();
      if (referenceTimestamp > 0) {
        const elapsed = Math.max(0, now - referenceTimestamp);
        const steps = Math.floor(elapsed / intervalMs) + 1;
        const nextRun = referenceTimestamp + steps * intervalMs + correctionOffsetMs;
        delayMs = Math.max(0, nextRun - now);
      } else {
        delayMs = intervalMs + correctionOffsetMs;
      }
    }
    const nextRunTimestamp = Date.now() + delayMs;
    await this.setStateAsync("gateway.nextPollTime", nextRunTimestamp, true);
    this.log.debug(
      `Next synchronized poll scheduled for ${new Date(nextRunTimestamp).toISOString()} (in ${Math.round(delayMs / 1e3)}s)`
    );
    this.pollTimer = this.setTimeout(async () => {
      this.pollTimer = void 0;
      try {
        const newData = await this.pollGatewayData();
        await this.scheduleNextPoll(newData);
      } catch (error) {
        this.log.error(`Error in adaptive polling: ${getErrorMessage(error)}`);
      }
    }, delayMs);
  }
  /**
   * Legt die zusätzlichen Statusdatenpunkte für Übermittlungen und Polling an.
   */
  async ensureGatewayStatsObjects() {
    const marketTransmissionsName = {
      en: "Transmissions to grid operator",
      de: "\xDCbermittlungen an Netzbetreiber",
      ru: "\u041F\u0435\u0440\u0435\u0434\u0430\u0447\u0438 \u043E\u043F\u0435\u0440\u0430\u0442\u043E\u0440\u0443 \u0441\u0435\u0442\u0435\u0432\u043E\u0439 \u0441\u0438\u0441\u0442\u0435\u043C\u044B",
      pt: "Transmiss\xF5es para o operador da rede",
      nl: "Overdracht aan netbeheerder",
      fr: "Transmissions \xE0 l'op\xE9rateur r\xE9seau",
      it: "Trasmissioni al gestore di rete",
      es: "Transmisiones al operador de red",
      pl: "Przekazywanie do operatora sieci",
      uk: "\u041F\u0435\u0440\u0435\u0434\u0430\u0447\u0456 \u043E\u043F\u0435\u0440\u0430\u0442\u043E\u0440\u0443 \u043C\u0435\u0440\u0435\u0436\u0456",
      "zh-cn": "\u5411\u7535\u7F51\u8FD0\u8425\u5546\u4F20\u8F93"
    };
    await this.setObjectNotExistsAsync("gateway.marketTransmissions", {
      type: "channel",
      common: { name: marketTransmissionsName },
      native: {}
    });
    await this.updateObjectName("gateway.marketTransmissions", marketTransmissionsName);
    const states = [
      {
        id: "gateway.marketTransmissions.totalCount",
        name: {
          en: "Total transmissions to grid operator",
          de: "Gesamtzahl \xDCbermittlungen an Netzbetreiber",
          ru: "\u041E\u0431\u0449\u0435\u0435 \u0447\u0438\u0441\u043B\u043E \u043F\u0435\u0440\u0435\u0434\u0430\u0447 \u043E\u043F\u0435\u0440\u0430\u0442\u043E\u0440\u0443 \u0441\u0435\u0442\u0435\u0432\u043E\u0439 \u0441\u0438\u0441\u0442\u0435\u043C\u044B",
          pt: "N\xFAmero total de transmiss\xF5es para o operador da rede",
          nl: "Totaal aantal overdrachten aan netbeheerder",
          fr: "Nombre total de transmissions \xE0 l'op\xE9rateur r\xE9seau",
          it: "Numero totale di trasmissioni al gestore di rete",
          es: "N\xFAmero total de transmisiones al operador de red",
          pl: "\u0141\u0105czna liczba przekaza\u0144 do operatora sieci",
          uk: "\u0417\u0430\u0433\u0430\u043B\u044C\u043D\u0430 \u043A\u0456\u043B\u044C\u043A\u0456\u0441\u0442\u044C \u043F\u0435\u0440\u0435\u0434\u0430\u0447 \u043E\u043F\u0435\u0440\u0430\u0442\u043E\u0440\u0443 \u043C\u0435\u0440\u0435\u0436\u0456",
          "zh-cn": "\u5411\u7535\u7F51\u8FD0\u8425\u5546\u4F20\u8F93\u603B\u6570"
        },
        type: "number",
        role: "value",
        def: 0
      },
      {
        id: "gateway.marketTransmissions.todayCount",
        name: {
          en: "Transmissions today",
          de: "\xDCbermittlungen heute",
          ru: "\u041F\u0435\u0440\u0435\u0434\u0430\u0447\u0438 \u0441\u0435\u0433\u043E\u0434\u043D\u044F",
          pt: "Transmiss\xF5es hoje",
          nl: "Overdrachten vandaag",
          fr: "Transmissions aujourd'hui",
          it: "Trasmissioni oggi",
          es: "Transmisiones hoy",
          pl: "Przekazania dzisiaj",
          uk: "\u041F\u0435\u0440\u0435\u0434\u0430\u0447\u0456 \u0441\u044C\u043E\u0433\u043E\u0434\u043D\u0456",
          "zh-cn": "\u4ECA\u65E5\u4F20\u8F93"
        },
        type: "number",
        role: "value",
        def: 0
      },
      {
        id: "gateway.marketTransmissions.lastTimestamp",
        name: {
          en: "Last transmission timestamp",
          de: "Letzter \xDCbermittlungs-Zeitpunkt",
          ru: "\u0412\u0440\u0435\u043C\u044F \u043F\u043E\u0441\u043B\u0435\u0434\u043D\u0435\u0439 \u043F\u0435\u0440\u0435\u0434\u0430\u0447\u0438",
          pt: "Data e hora da \xFAltima transmiss\xE3o",
          nl: "Tijdstempel laatste overdracht",
          fr: "Horodatage de la derni\xE8re transmission",
          it: "Ora dell'ultima trasmissione",
          es: "Marca de tiempo de la \xFAltima transmisi\xF3n",
          pl: "Czas ostatniego przekazania",
          uk: "\u0427\u0430\u0441 \u043E\u0441\u0442\u0430\u043D\u043D\u044C\u043E\u0457 \u043F\u0435\u0440\u0435\u0434\u0430\u0447\u0456",
          "zh-cn": "\u4E0A\u6B21\u4F20\u8F93\u65F6\u95F4\u6233"
        },
        type: "number",
        role: "value.time",
        def: 0
      },
      {
        id: "gateway.marketTransmissions.historyJson",
        name: {
          en: "Transmission history (JSON)",
          de: "Historie aller \xDCbermittlungen (JSON)",
          ru: "\u0418\u0441\u0442\u043E\u0440\u0438\u044F \u0432\u0441\u0435\u0445 \u043F\u0435\u0440\u0435\u0434\u0430\u0447 (JSON)",
          pt: "Hist\xF3rico de transmiss\xF5es (JSON)",
          nl: "Geschiedenis van alle overdrachten (JSON)",
          fr: "Historique de toutes les transmissions (JSON)",
          it: "Cronologia di tutte le trasmissioni (JSON)",
          es: "Historial de todas las transmisiones (JSON)",
          pl: "Historia wszystkich przekaza\u0144 (JSON)",
          uk: "\u0406\u0441\u0442\u043E\u0440\u0456\u044F \u0432\u0441\u0456\u0445 \u043F\u0435\u0440\u0435\u0434\u0430\u0447 (JSON)",
          "zh-cn": "\u6240\u6709\u4F20\u8F93\u5386\u53F2 (JSON)"
        },
        type: "string",
        role: "json",
        def: "[]"
      },
      {
        id: "gateway.logTableJson",
        name: {
          en: "Gateway log as structured table (JSON)",
          de: "Gateway-Log als strukturierte Tabelle (JSON)",
          ru: "\u0416\u0443\u0440\u043D\u0430\u043B \u0448\u043B\u044E\u0437\u0430 \u0432 \u0432\u0438\u0434\u0435 \u0441\u0442\u0440\u0443\u043A\u0442\u0443\u0440\u0438\u0440\u043E\u0432\u0430\u043D\u043D\u043E\u0439 \u0442\u0430\u0431\u043B\u0438\u0446\u044B (JSON)",
          pt: "Log do gateway como tabela estruturada (JSON)",
          nl: "Gatewaylog als gestructureerde tabel (JSON)",
          fr: "Journal de la passerelle sous forme de tableau structur\xE9 (JSON)",
          it: "Log del gateway come tabella strutturata (JSON)",
          es: "Registro del gateway como tabla estructurada (JSON)",
          pl: "Dziennik bramki jako tabela strukturalna (JSON)",
          uk: "\u0416\u0443\u0440\u043D\u0430\u043B \u0448\u043B\u044E\u0437\u0443 \u044F\u043A \u0441\u0442\u0440\u0443\u043A\u0442\u0443\u0440\u043E\u0432\u0430\u043D\u0430 \u0442\u0430\u0431\u043B\u0438\u0446\u044F (JSON)",
          "zh-cn": "\u7F51\u5173\u65E5\u5FD7\u7ED3\u6784\u5316\u8868 (JSON)"
        },
        type: "string",
        role: "json",
        def: "[]"
      },
      {
        id: "gateway.nextPollTime",
        name: {
          en: "Next scheduled poll",
          de: "N\xE4chster geplanter Abruf",
          ru: "\u0421\u043B\u0435\u0434\u0443\u044E\u0449\u0438\u0439 \u0437\u0430\u043F\u043B\u0430\u043D\u0438\u0440\u043E\u0432\u0430\u043D\u043D\u044B\u0439 \u043E\u043F\u0440\u043E\u0441",
          pt: "Pr\xF3xima consulta programada",
          nl: "Volgende geplande polling",
          fr: "Prochaine interrogation planifi\xE9e",
          it: "Prossima interrogazione pianificata",
          es: "Pr\xF3xima consulta programada",
          pl: "Nast\u0119pne zaplanowane odczytywanie",
          uk: "\u041D\u0430\u0441\u0442\u0443\u043F\u043D\u0435 \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u0435 \u043E\u043F\u0438\u0442\u0443\u0432\u0430\u043D\u043D\u044F",
          "zh-cn": "\u4E0B\u6B21\u8BA1\u5212\u8F6E\u8BE2"
        },
        type: "number",
        role: "value.time",
        def: 0
      }
    ];
    for (const state of states) {
      await this.setObjectNotExistsAsync(state.id, {
        type: "state",
        common: {
          name: state.name,
          type: state.type,
          role: state.role,
          read: true,
          write: false,
          def: state.def
        },
        native: {}
      });
      await this.updateObjectName(state.id, state.name);
    }
  }
  async updateObjectName(id, name) {
    await this.extendObjectAsync(id, { common: { name } });
  }
  /**
   * Methode 01: smgw-info
   */
  async pollSmgwInfo() {
    const res = await this.sendConexaRequest({ method: "smgw-info" });
    const info = asGatewayRecord(res["smgw-info"]);
    if (!info) {
      return;
    }
    const firmwareInfo = asGatewayRecord(info["firmware-info"]);
    await this.setStateAsync("gateway.id", asStateValue(info["smgw-id"]), true);
    await this.setStateAsync("gateway.state", asStateValue(info["smgw-state"]), true);
    const gatewayTimestamp = toTimestamp(info["smgw-time"]);
    if (gatewayTimestamp === void 0) {
      this.log.warn("Invalid timestamp in smgw-info; gateway.time was not updated.");
    } else {
      await this.setStateAsync("gateway.time", gatewayTimestamp, true);
    }
    await this.setStateAsync("gateway.firmwareVersion", asStateValue(firmwareInfo == null ? void 0 : firmwareInfo.version), true);
    await this.setStateAsync("gateway.firmwareHash", asStateValue(firmwareInfo == null ? void 0 : firmwareInfo.hash), true);
    await this.setStateAsync("gateway.apiVersion", asStateValue(res.version), true);
    if (info.certificates) {
      await this.setStateAsync("gateway.certificatesJson", JSON.stringify(info.certificates), true);
    }
  }
  /**
   * Methode 04: log (inkl. Filter für Event 16001 = Marktteilnehmer-Versand)
   */
  async pollLogs() {
    var _a;
    try {
      const res = await this.sendConexaRequest({ method: "log" });
      const logObj = asGatewayRecord(res.log);
      if (!logObj) {
        return;
      }
      const entries = asGatewayRecords(logObj.entries);
      const recordsCount = Number(logObj.records) || entries.length;
      await this.setStateAsync("logs.recordsCount", recordsCount, true);
      await this.setStateAsync("logs.entriesJson", JSON.stringify(entries), true);
      const logTable = entries.map((entry) => ({
        index: entry.index,
        id: entry["event-id"],
        timestamp: entry.time,
        vendor: entry["vendor-id"],
        message: entry.message
      })).sort((a, b) => {
        const timestampA = Date.parse(toText(a.timestamp));
        const timestampB = Date.parse(toText(b.timestamp));
        if (Number.isFinite(timestampA) && Number.isFinite(timestampB) && timestampA !== timestampB) {
          return timestampB - timestampA;
        }
        const indexA = Number(a.index);
        const indexB = Number(b.index);
        if (Number.isFinite(indexA) && Number.isFinite(indexB)) {
          return indexB - indexA;
        }
        return 0;
      });
      await this.setStateAsync("gateway.logTableJson", JSON.stringify(logTable), true);
      if (entries.length > 0) {
        const latest = entries[entries.length - 1];
        await this.setStateAsync("logs.lastMessage", asStateValue(latest.message), true);
        const latestTimestamp = toTimestamp(latest.time);
        if (latestTimestamp === void 0) {
          this.log.warn("Invalid timestamp in the latest log entry; logs.lastTime was not updated.");
        } else {
          await this.setStateAsync("logs.lastTime", latestTimestamp, true);
        }
        await this.setStateAsync("logs.lastEventId", toText(latest["event-id"]), true);
      }
      const historyState = await this.getStateAsync("gateway.marketTransmissions.historyJson");
      let history = [];
      if (historyState && typeof historyState.val === "string") {
        try {
          const parsedHistory = JSON.parse(historyState.val);
          if (Array.isArray(parsedHistory)) {
            history = parsedHistory.filter(
              (entry) => typeof (entry == null ? void 0 : entry.time) === "string" && typeof (entry == null ? void 0 : entry.message) === "string"
            );
          } else {
            this.log.warn("The grid operator transmission history is not a JSON array; resetting it.");
          }
        } catch (error) {
          this.log.warn(
            `The grid operator transmission history is invalid and will be rebuilt: ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
      const knownTimes = new Set(history.map((entry) => entry.time));
      const marketEvents = entries.filter((entry) => String(entry["event-id"]) === "16001").map((entry) => ({ time: toText(entry.time), message: toText(entry.message) })).filter((entry) => entry.time.length > 0);
      const newMarketEvents = marketEvents.filter((entry) => !knownTimes.has(entry.time));
      history = [...history, ...newMarketEvents].slice(-500);
      const historyWithTimestamps = history.map((entry) => ({ ...entry, timestamp: new Date(entry.time).getTime() })).filter((entry) => Number.isFinite(entry.timestamp)).sort((a, b) => a.timestamp - b.timestamp);
      const lastMarket = historyWithTimestamps[historyWithTimestamps.length - 1];
      this.latestMarketEventTimestamp = (_a = lastMarket == null ? void 0 : lastMarket.timestamp) != null ? _a : 0;
      if (newMarketEvents.length > 0) {
        const newestNewEvent = newMarketEvents.map((entry) => ({ ...entry, timestamp: new Date(entry.time).getTime() })).filter((entry) => Number.isFinite(entry.timestamp)).sort((a, b) => a.timestamp - b.timestamp).at(-1);
        const loggedEvent = newestNewEvent != null ? newestNewEvent : newMarketEvents[newMarketEvents.length - 1];
        this.log.info(`New grid operator transmission (Event 16001) registered at ${loggedEvent.time}`);
      }
      const today = /* @__PURE__ */ new Date();
      const todayCount = history.filter((entry) => {
        const eventDate = new Date(entry.time);
        return Number.isFinite(eventDate.getTime()) && eventDate.getFullYear() === today.getFullYear() && eventDate.getMonth() === today.getMonth() && eventDate.getDate() === today.getDate();
      }).length;
      await this.setStateAsync("gateway.marketTransmissions.totalCount", history.length, true);
      await this.setStateAsync("gateway.marketTransmissions.todayCount", todayCount, true);
      const lastMarketTimestamp = lastMarket ? toTimestamp(lastMarket.time) : 0;
      if (lastMarketTimestamp === void 0) {
        this.log.warn(
          "Invalid timestamp for the latest grid operator transmission; gateway.marketTransmissions.lastTimestamp was not updated."
        );
      } else {
        await this.setStateAsync("gateway.marketTransmissions.lastTimestamp", lastMarketTimestamp, true);
      }
      await this.setStateAsync("gateway.marketTransmissions.historyJson", JSON.stringify(history), true);
      if (lastMarket) {
        const lastTransmissionTimestamp = toTimestamp(lastMarket.time);
        if (lastTransmissionTimestamp === void 0) {
          this.log.warn(
            "Invalid timestamp for the latest grid operator transmission; gateway.lastMarketTransmission was not updated."
          );
        } else {
          await this.setStateAsync("gateway.lastMarketTransmission", lastTransmissionTimestamp, true);
        }
      }
    } catch (error) {
      this.log.debug(`Log request skipped: ${getErrorMessage(error)}`);
    }
  }
  /**
   * Methode 02 (user-info) & Methode 03 (usage-point-info):
   * Liest alle verfügbaren Tarife ein und speichert sie im Katalog-Datenpunkt.
   *
   * @param forceLog Gibt an, ob der Scan im Info-Log angekündigt werden soll
   */
  async discoverUsagePoints(forceLog) {
    var _a, _b, _c;
    if (forceLog) {
      this.log.info("Scanning the gateway for activated tariffs (user-info)...");
    }
    const res = await this.sendConexaRequest({ method: "user-info" });
    const userInfo = asGatewayRecord(res["user-info"]);
    const rawList = asGatewayRecords(userInfo == null ? void 0 : userInfo["usage-points"]);
    const discovered = [];
    const usedCleanIds = /* @__PURE__ */ new Set();
    for (const item of rawList) {
      const upId = item["usage-point-id"];
      const upName = item["usage-point-name"] || upId;
      if (!upId) {
        continue;
      }
      const usagePointId = toText(upId);
      const usagePointName = String(upName);
      const baseCleanId = usagePointName.replace(/[^a-zA-Z0-9_-]/g, "_");
      let cleanId = baseCleanId;
      if (usedCleanIds.has(cleanId.toLowerCase())) {
        const suffix = usagePointId.replace(/[^a-zA-Z0-9_-]/g, "_") || "usagePoint";
        cleanId = `${baseCleanId}_${suffix}`;
        let collisionNumber = 2;
        while (usedCleanIds.has(cleanId.toLowerCase())) {
          cleanId = `${baseCleanId}_${suffix}_${collisionNumber++}`;
        }
      }
      usedCleanIds.add(cleanId.toLowerCase());
      let database = "origin";
      let obis = "";
      let scaler = -1;
      let unit = "30";
      try {
        const detailRes = await this.sendConexaRequest({
          method: "usage-point-info",
          "usage-point-id": usagePointId
        });
        this.log.debug(`[usage-point-info] Raw data for ${usagePointName}: ${JSON.stringify(detailRes)}`);
        const info = asGatewayRecord(detailRes["usage-point-info"]);
        const databases = [
          ...asGatewayRecords(info == null ? void 0 : info.databases),
          ...asGatewayRecords(detailRes.databases)
        ];
        const sortedDatabases = [
          ...databases.filter((entry) => String(entry == null ? void 0 : entry.database).toLowerCase() === "origin"),
          ...databases.filter((entry) => String(entry == null ? void 0 : entry.database).toLowerCase() !== "origin")
        ];
        const databaseWithChannel = sortedDatabases.map((entry) => ({
          entry,
          channel: asGatewayRecords(entry.channels).find(
            (channel2) => typeof channel2.obis === "string" && channel2.obis.length > 0
          )
        })).find((result) => result.channel);
        const directChannels = [...asGatewayRecords(info == null ? void 0 : info.channels), ...asGatewayRecords(detailRes.channels)];
        const channel = (_a = databaseWithChannel == null ? void 0 : databaseWithChannel.channel) != null ? _a : directChannels.find((ch) => ch == null ? void 0 : ch.obis);
        if (databaseWithChannel) {
          database = toText(databaseWithChannel.entry.database, "origin");
        } else if ((_b = sortedDatabases[0]) == null ? void 0 : _b.database) {
          database = toText(sortedDatabases[0].database);
        }
        if (channel) {
          obis = typeof channel.obis === "string" ? channel.obis : "";
          scaler = channel.scaler !== void 0 ? Number(channel.scaler) : -1;
          unit = toText(channel.unit, "30");
        }
      } catch (err) {
        this.log.debug(
          `The usage-point-info detail request for ${usagePointName} did not return a channel: ${getErrorMessage(err)}`
        );
      }
      if (!obis) {
        const obisMatch = usagePointName.match(/(?:^|[_\s])(\d+\.\d+\.\d+)(?:$|[_\s])/);
        if (obisMatch) {
          obis = obisMatch[1];
        }
      }
      const upEntry = {
        usagePointId,
        usagePointName,
        cleanId,
        tafNumber: toText(item["taf-number"]),
        tafState: toText(item["taf-state"]),
        meterId: toText((_c = asGatewayRecords(item.meter)[0]) == null ? void 0 : _c["meter-id"]),
        meteringPointId: toText(item["metering-point-id"]),
        database,
        obis,
        scaler,
        unit
      };
      discovered.push(upEntry);
      await this.ensureUsagePointObjects(upEntry);
    }
    this.usagePointsCache = discovered;
    this.lastCatalogUpdate = Date.now();
    await this.setStateAsync("usagePoints.catalogJson", JSON.stringify(discovered), true);
    await this.setStateAsync("usagePoints.lastUpdated", this.lastCatalogUpdate, true);
    this.log.info(`${discovered.length} tariffs successfully read and stored in the catalog.`);
  }
  /**
   * Legt die Datenpunkte für einen gefundenen Tarif dynamisch unter usagePoints.<Tarifname> an
   *
   * @param up Zwischengespeicherte Tarifdaten für den anzulegenden Datenpunkt
   */
  async ensureUsagePointObjects(up) {
    const basePath = `usagePoints.${up.cleanId}`;
    const channelName = {
      en: `${up.usagePointName} (TAF ${up.tafNumber})`,
      de: `${up.usagePointName} (TAF ${up.tafNumber})`,
      ru: `${up.usagePointName} (TAF ${up.tafNumber})`,
      pt: `${up.usagePointName} (TAF ${up.tafNumber})`,
      nl: `${up.usagePointName} (TAF ${up.tafNumber})`,
      fr: `${up.usagePointName} (TAF ${up.tafNumber})`,
      it: `${up.usagePointName} (TAF ${up.tafNumber})`,
      es: `${up.usagePointName} (TAF ${up.tafNumber})`,
      pl: `${up.usagePointName} (TAF ${up.tafNumber})`,
      uk: `${up.usagePointName} (TAF ${up.tafNumber})`,
      "zh-cn": `${up.usagePointName} (TAF ${up.tafNumber})`
    };
    await this.setObjectNotExistsAsync(basePath, {
      type: "channel",
      common: { name: channelName },
      native: {}
    });
    await this.updateObjectName(basePath, channelName);
    const states = [
      {
        id: "usagePointId",
        name: {
          en: "Usage Point ID",
          de: "Usage-Point-ID",
          ru: "\u0418\u0434\u0435\u043D\u0442\u0438\u0444\u0438\u043A\u0430\u0442\u043E\u0440 \u0442\u043E\u0447\u043A\u0438 \u0443\u0447\u0435\u0442\u0430",
          pt: "ID do ponto de consumo",
          nl: "Usage Point-ID",
          fr: "ID du point de comptage",
          it: "ID del punto di prelievo",
          es: "ID del punto de suministro",
          pl: "ID punktu poboru",
          uk: "\u0406\u0434\u0435\u043D\u0442\u0438\u0444\u0456\u043A\u0430\u0442\u043E\u0440 \u0442\u043E\u0447\u043A\u0438 \u043E\u0431\u043B\u0456\u043A\u0443",
          "zh-cn": "\u7528\u7535\u70B9 ID"
        },
        type: "string",
        role: "text",
        def: ""
      },
      {
        id: "meterId",
        name: {
          en: "Meter ID (meter number)",
          de: "Z\xE4hler-ID (Z\xE4hlernummer)",
          ru: "ID \u0441\u0447\u0435\u0442\u0447\u0438\u043A\u0430 (\u043D\u043E\u043C\u0435\u0440 \u0441\u0447\u0435\u0442\u0447\u0438\u043A\u0430)",
          pt: "ID do contador (n\xFAmero do contador)",
          nl: "Meter-ID (meternummer)",
          fr: "ID du compteur (num\xE9ro du compteur)",
          it: "ID contatore (numero contatore)",
          es: "ID del contador (n\xFAmero de contador)",
          pl: "ID licznika (numer licznika)",
          uk: "\u0406\u0434\u0435\u043D\u0442\u0438\u0444\u0456\u043A\u0430\u0442\u043E\u0440 \u043B\u0456\u0447\u0438\u043B\u044C\u043D\u0438\u043A\u0430 (\u043D\u043E\u043C\u0435\u0440 \u043B\u0456\u0447\u0438\u043B\u044C\u043D\u0438\u043A\u0430)",
          "zh-cn": "\u7535\u8868 ID\uFF08\u7535\u8868\u7F16\u53F7\uFF09"
        },
        type: "string",
        role: "info.serial",
        def: ""
      },
      {
        id: "tafState",
        name: {
          en: "TAF status",
          de: "TAF-Status",
          ru: "\u0421\u0442\u0430\u0442\u0443\u0441 TAF",
          pt: "Estado TAF",
          nl: "TAF-status",
          fr: "Statut TAF",
          it: "Stato TAF",
          es: "Estado TAF",
          pl: "Status TAF",
          uk: "\u0421\u0442\u0430\u0442\u0443\u0441 TAF",
          "zh-cn": "TAF \u72B6\u6001"
        },
        type: "string",
        role: "info.status",
        def: ""
      },
      {
        id: "obis",
        name: {
          en: "OBIS code",
          de: "OBIS-Kennzahl",
          ru: "\u041A\u043E\u0434 OBIS",
          pt: "C\xF3digo OBIS",
          nl: "OBIS-code",
          fr: "Code OBIS",
          it: "Codice OBIS",
          es: "C\xF3digo OBIS",
          pl: "Kod OBIS",
          uk: "\u041A\u043E\u0434 OBIS",
          "zh-cn": "OBIS \u4EE3\u7801"
        },
        type: "string",
        role: "text",
        def: ""
      },
      {
        id: "valueRaw",
        name: {
          en: "Raw meter value",
          de: "Rohwert vom Z\xE4hler",
          ru: "\u041D\u0435\u043E\u0431\u0440\u0430\u0431\u043E\u0442\u0430\u043D\u043D\u043E\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435 \u0441\u0447\u0435\u0442\u0447\u0438\u043A\u0430",
          pt: "Valor bruto do contador",
          nl: "Ruwe meterwaarde",
          fr: "Valeur brute du compteur",
          it: "Valore grezzo del contatore",
          es: "Valor bruto del contador",
          pl: "Surowa warto\u015B\u0107 licznika",
          uk: "\u041D\u0435\u043E\u0431\u0440\u043E\u0431\u043B\u0435\u043D\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u043D\u044F \u043B\u0456\u0447\u0438\u043B\u044C\u043D\u0438\u043A\u0430",
          "zh-cn": "\u7535\u8868\u539F\u59CB\u503C"
        },
        type: "number",
        role: "value",
        def: 0
      },
      {
        id: "valueWh",
        name: {
          en: "Meter reading (Wh)",
          de: "Z\xE4hlerstand (Wh)",
          ru: "\u041F\u043E\u043A\u0430\u0437\u0430\u043D\u0438\u0435 \u0441\u0447\u0435\u0442\u0447\u0438\u043A\u0430 (\u0412\u0442\xB7\u0447)",
          pt: "Leitura do contador (Wh)",
          nl: "Meterstand (Wh)",
          fr: "Index du compteur (Wh)",
          it: "Lettura del contatore (Wh)",
          es: "Lectura del contador (Wh)",
          pl: "Stan licznika (Wh)",
          uk: "\u041F\u043E\u043A\u0430\u0437\u0430\u043D\u043D\u044F \u043B\u0456\u0447\u0438\u043B\u044C\u043D\u0438\u043A\u0430 (\u0412\u0442\xB7\u0433\u043E\u0434)",
          "zh-cn": "\u7535\u8868\u8BFB\u6570 (Wh)"
        },
        type: "number",
        role: "value.energy",
        unit: "Wh",
        def: 0
      },
      {
        id: "valueKWh",
        name: {
          en: "Meter reading (kWh)",
          de: "Z\xE4hlerstand (kWh)",
          ru: "\u041F\u043E\u043A\u0430\u0437\u0430\u043D\u0438\u0435 \u0441\u0447\u0435\u0442\u0447\u0438\u043A\u0430 (\u043A\u0412\u0442\xB7\u0447)",
          pt: "Leitura do contador (kWh)",
          nl: "Meterstand (kWh)",
          fr: "Index du compteur (kWh)",
          it: "Lettura del contatore (kWh)",
          es: "Lectura del contador (kWh)",
          pl: "Stan licznika (kWh)",
          uk: "\u041F\u043E\u043A\u0430\u0437\u0430\u043D\u043D\u044F \u043B\u0456\u0447\u0438\u043B\u044C\u043D\u0438\u043A\u0430 (\u043A\u0412\u0442\xB7\u0433\u043E\u0434)",
          "zh-cn": "\u7535\u8868\u8BFB\u6570 (kWh)"
        },
        type: "number",
        role: "value.energy",
        unit: "kWh",
        def: 0
      },
      {
        id: "deltaWh",
        name: {
          en: "Consumption last interval (Wh)",
          de: "Verbrauch letztes Intervall (Wh)",
          ru: "\u0420\u0430\u0441\u0445\u043E\u0434 \u0437\u0430 \u043F\u043E\u0441\u043B\u0435\u0434\u043D\u0438\u0439 \u0438\u043D\u0442\u0435\u0440\u0432\u0430\u043B (\u0412\u0442\xB7\u0447)",
          pt: "Consumo no \xFAltimo intervalo (Wh)",
          nl: "Verbruik laatste interval (Wh)",
          fr: "Consommation du dernier intervalle (Wh)",
          it: "Consumo dell'ultimo intervallo (Wh)",
          es: "Consumo del \xFAltimo intervalo (Wh)",
          pl: "Zu\u017Cycie w ostatnim przedziale (Wh)",
          uk: "\u0421\u043F\u043E\u0436\u0438\u0432\u0430\u043D\u043D\u044F \u0437\u0430 \u043E\u0441\u0442\u0430\u043D\u043D\u0456\u0439 \u0456\u043D\u0442\u0435\u0440\u0432\u0430\u043B (\u0412\u0442\xB7\u0433\u043E\u0434)",
          "zh-cn": "\u4E0A\u4E00\u95F4\u9694\u8017\u7535\u91CF (Wh)"
        },
        type: "number",
        role: "value.energy",
        unit: "Wh",
        def: 0
      },
      {
        id: "averagePowerW",
        name: {
          en: "Average power last interval (W)",
          de: "Durchschnittsleistung letztes Intervall (W)",
          ru: "\u0421\u0440\u0435\u0434\u043D\u044F\u044F \u043C\u043E\u0449\u043D\u043E\u0441\u0442\u044C \u0437\u0430 \u043F\u043E\u0441\u043B\u0435\u0434\u043D\u0438\u0439 \u0438\u043D\u0442\u0435\u0440\u0432\u0430\u043B (\u0412\u0442)",
          pt: "Pot\xEAncia m\xE9dia no \xFAltimo intervalo (W)",
          nl: "Gemiddeld vermogen laatste interval (W)",
          fr: "Puissance moyenne du dernier intervalle (W)",
          it: "Potenza media dell'ultimo intervallo (W)",
          es: "Potencia media del \xFAltimo intervalo (W)",
          pl: "\u015Arednia moc w ostatnim przedziale (W)",
          uk: "\u0421\u0435\u0440\u0435\u0434\u043D\u044F \u043F\u043E\u0442\u0443\u0436\u043D\u0456\u0441\u0442\u044C \u0437\u0430 \u043E\u0441\u0442\u0430\u043D\u043D\u0456\u0439 \u0456\u043D\u0442\u0435\u0440\u0432\u0430\u043B (\u0412\u0442)",
          "zh-cn": "\u4E0A\u4E00\u95F4\u9694\u5E73\u5747\u529F\u7387 (W)"
        },
        type: "number",
        role: "value.power",
        unit: "W",
        def: 0
      },
      {
        id: "captureTime",
        name: {
          en: "Capture timestamp",
          de: "Zeitpunkt der Erfassung",
          ru: "\u0412\u0440\u0435\u043C\u044F \u0441\u043D\u044F\u0442\u0438\u044F \u043F\u043E\u043A\u0430\u0437\u0430\u043D\u0438\u0439",
          pt: "Data e hora da captura",
          nl: "Tijdstempel van opname",
          fr: "Horodatage de la mesure",
          it: "Data e ora di acquisizione",
          es: "Marca de tiempo de captura",
          pl: "Czas pobrania pomiaru",
          uk: "\u0427\u0430\u0441 \u043E\u0442\u0440\u0438\u043C\u0430\u043D\u043D\u044F \u043F\u043E\u043A\u0430\u0437\u0430\u043D\u044C",
          "zh-cn": "\u91C7\u96C6\u65F6\u95F4\u6233"
        },
        type: "string",
        role: "date",
        def: ""
      },
      {
        id: "historyJson",
        name: {
          en: "History / load profile (JSON)",
          de: "Historie / Lastgang (JSON)",
          ru: "\u0418\u0441\u0442\u043E\u0440\u0438\u044F / \u043F\u0440\u043E\u0444\u0438\u043B\u044C \u043D\u0430\u0433\u0440\u0443\u0437\u043A\u0438 (JSON)",
          pt: "Hist\xF3rico / curva de carga (JSON)",
          nl: "Geschiedenis / belastingsprofiel (JSON)",
          fr: "Historique / courbe de charge (JSON)",
          it: "Cronologia / profilo di carico (JSON)",
          es: "Historial / perfil de carga (JSON)",
          pl: "Historia / profil obci\u0105\u017Cenia (JSON)",
          uk: "\u0406\u0441\u0442\u043E\u0440\u0456\u044F / \u043F\u0440\u043E\u0444\u0456\u043B\u044C \u043D\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0435\u043D\u043D\u044F (JSON)",
          "zh-cn": "\u5386\u53F2\u8BB0\u5F55 / \u8D1F\u8377\u66F2\u7EBF (JSON)"
        },
        type: "string",
        role: "json",
        def: "[]"
      }
    ];
    for (const state of states) {
      const id = `${basePath}.${state.id}`;
      await this.setObjectNotExistsAsync(id, {
        type: "state",
        common: {
          name: state.name,
          type: state.type,
          role: state.role,
          ...state.unit ? { unit: state.unit } : {},
          read: true,
          write: false,
          def: state.def
        },
        native: {}
      });
      await this.updateObjectName(id, state.name);
    }
    await this.setStateAsync(`${basePath}.usagePointId`, up.usagePointId, true);
    await this.setStateAsync(`${basePath}.meterId`, up.meterId, true);
    await this.setStateAsync(`${basePath}.tafState`, up.tafState, true);
    if (up.obis) {
      await this.setStateAsync(`${basePath}.obis`, up.obis, true);
    }
  }
  /**
   * Methode 05: readings
   * Fragt alle gespeicherten Tarife mit exakt dem Bruno-Payload ab.
   *
   * @param fetchFullHistory Gibt an, ob die vollständige Historie statt nur des letzten Werts geladen wird
   */
  async pollReadings(fetchFullHistory = false) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l;
    let catalogRefreshNeeded = false;
    let hasNewReading = false;
    for (const [usagePointIndex, up] of this.usagePointsCache.entries()) {
      try {
        let usedFullHistory = fetchFullHistory;
        const payload = {
          method: "readings",
          "usage-point-id": up.usagePointId,
          database: up.database || "origin",
          "last-reading": !fetchFullHistory
        };
        if (fetchFullHistory) {
          payload["from-time"] = new Date(Date.now() - 24 * 60 * 60 * 1e3).toISOString().slice(0, 19);
        }
        let res;
        const extractReadings = (response) => {
          var _a2;
          let foundChannel;
          let foundReadings = [];
          const findChannel = (channels) => {
            for (const value of channels) {
              const candidate = asGatewayRecord(value);
              const readings = asGatewayRecords(candidate == null ? void 0 : candidate.readings);
              if (candidate && readings.length > 0) {
                foundChannel = candidate;
                foundReadings = readings;
                return true;
              }
            }
            return false;
          };
          const findDatabaseChannels = (databases) => {
            for (const value of databases) {
              const database = asGatewayRecord(value);
              if (findChannel(asGatewayArray(database == null ? void 0 : database.channels))) {
                if (database == null ? void 0 : database.database) {
                  up.database = toText(database.database);
                }
                return true;
              }
            }
            return false;
          };
          const responseReadings = asGatewayRecord(response.readings);
          if (!findChannel(asGatewayArray(responseReadings == null ? void 0 : responseReadings.channels))) {
            if (!findDatabaseChannels(asGatewayArray(responseReadings == null ? void 0 : responseReadings.databases)) && !findChannel(asGatewayArray(response.channels))) {
              findDatabaseChannels(asGatewayArray(response.databases));
            }
          }
          if (foundReadings.length === 0 && Array.isArray(response.readings)) {
            foundReadings = asGatewayRecords(response.readings);
          }
          if (foundReadings.length === 0 && (responseReadings == null ? void 0 : responseReadings.reading) && isGatewayRecord(responseReadings.reading)) {
            foundReadings = [responseReadings.reading];
          }
          if (foundReadings.length === 0 && isGatewayRecord(response.reading)) {
            foundReadings = [response.reading];
          }
          const recordsValue = (_a2 = responseReadings == null ? void 0 : responseReadings.records) != null ? _a2 : response == null ? void 0 : response.records;
          const hasRecords = recordsValue !== void 0 && recordsValue !== null && toText(recordsValue) !== "0";
          const hasChannels = asGatewayArray(responseReadings == null ? void 0 : responseReadings.channels).length > 0 || asGatewayArray(response.channels).length > 0 || asGatewayArray(responseReadings == null ? void 0 : responseReadings.databases).some(
            (database) => {
              var _a3;
              return asGatewayArray((_a3 = asGatewayRecord(database)) == null ? void 0 : _a3.channels).length > 0;
            }
          ) || asGatewayArray(response.databases).some(
            (database) => {
              var _a3;
              return asGatewayArray((_a3 = asGatewayRecord(database)) == null ? void 0 : _a3.channels).length > 0;
            }
          );
          return {
            channel: foundChannel,
            readings: foundReadings,
            hasDataIndicator: hasRecords || hasChannels
          };
        };
        try {
          this.log.debug(`[readings] Sending payload for ${up.usagePointName}: ${JSON.stringify(payload)}`);
          res = await this.sendConexaRequest(payload);
        } catch (error) {
          if (!fetchFullHistory || !/\b400\b/.test(error instanceof Error ? error.message : String(error))) {
            throw error;
          }
          this.log.warn(
            `Full history for tariff ${up.usagePointName} was rejected with HTTP 400; requesting the latest reading instead.`
          );
          usedFullHistory = false;
          delete payload["from-time"];
          payload["last-reading"] = true;
          try {
            this.log.debug(
              `[readings] Sending payload for ${up.usagePointName}: ${JSON.stringify(payload)}`
            );
            res = await this.sendConexaRequest(payload);
          } catch (fallbackError) {
            if (!/\b400\b/.test(
              fallbackError instanceof Error ? fallbackError.message : String(fallbackError)
            )) {
              catalogRefreshNeeded = true;
            }
            throw fallbackError;
          }
        }
        const shouldLogRaw = usagePointIndex === 0 && !this.readingsRawLogged;
        if (shouldLogRaw) {
          this.log.debug(`[readings] Raw data for ${up.usagePointName}: ${JSON.stringify(res)}`);
          this.readingsRawLogged = true;
        }
        let { channel, readings: readingsList, hasDataIndicator } = extractReadings(res);
        if (readingsList.length === 0 && !hasDataIndicator) {
          const fallbackPayloads = [
            {
              method: "readings",
              "usage-point-id": up.usagePointId,
              database: up.database || "origin",
              "from-time": "2020-01-01T00:00:00Z",
              "to-time": (/* @__PURE__ */ new Date()).toISOString().replace(/\.\d{3}Z$/, "Z")
            },
            {
              method: "readings",
              "usage-point-id": up.usagePointId,
              database: up.database || "origin",
              "last-reading": "true"
            }
          ];
          if (up.obis) {
            fallbackPayloads.push({
              method: "readings",
              "usage-point-id": up.usagePointId,
              database: up.database || "origin",
              "last-reading": true,
              channels: [{ obis: up.obis }]
            });
          }
          for (const fallbackPayload of fallbackPayloads) {
            this.log.debug(
              `[readings] Sending payload for ${up.usagePointName}: ${JSON.stringify(fallbackPayload)}`
            );
            try {
              const fallbackResponse = await this.sendConexaRequest(fallbackPayload);
              const extracted = extractReadings(fallbackResponse);
              res = fallbackResponse;
              channel = extracted.channel;
              readingsList = extracted.readings;
              hasDataIndicator = extracted.hasDataIndicator;
              if (readingsList.length > 0 || hasDataIndicator) {
                break;
              }
            } catch (error) {
              this.log.debug(
                `[readings] Fallback for ${up.usagePointName} failed: ${error instanceof Error ? error.message : String(error)}`
              );
            }
          }
        }
        if (readingsList.length === 0) {
          if (!shouldLogRaw) {
            this.log.debug(`[readings] Raw data for ${up.usagePointName}: ${JSON.stringify(res)}`);
          }
          this.log.warn(
            `No readings found in the readings response for ${up.usagePointName}. JSON structure: ${JSON.stringify(res).slice(0, 500)}`
          );
          continue;
        }
        const basePath = `usagePoints.${up.cleanId}`;
        if (typeof (channel == null ? void 0 : channel.obis) === "string" && channel.obis && !up.obis) {
          await this.setStateAsync(`${basePath}.obis`, channel.obis, true);
        }
        const prevWhState = await this.getStateAsync(`${basePath}.valueWh`);
        const prevTimeState = await this.getStateAsync(`${basePath}.captureTime`);
        const parseReadingTimestamp = (time) => {
          if (typeof time === "number" && Number.isFinite(time)) {
            return time < 1e12 ? time * 1e3 : time;
          }
          if (typeof time !== "string") {
            return Number.NaN;
          }
          const value = time.trim();
          if (/^\d{10,13}$/.test(value)) {
            const numericTime = Number(value);
            return value.length <= 10 ? numericTime * 1e3 : numericTime;
          }
          return Date.parse(value);
        };
        for (const reading of readingsList) {
          const readingTime = (_d = (_c = (_b = (_a = reading["capture-time"]) != null ? _a : reading["target-time"]) != null ? _b : reading.time) != null ? _c : reading.timestamp) != null ? _d : "";
          const readingTimestamp = parseReadingTimestamp(readingTime);
          if (Number.isFinite(readingTimestamp)) {
            this.latestReadingTimestamp = Math.max(this.latestReadingTimestamp, readingTimestamp);
          }
        }
        const latest = readingsList[readingsList.length - 1];
        const rawValue = (_f = (_e = latest.value) != null ? _e : latest.val) != null ? _f : latest["reading-value"];
        const rawVal = typeof rawValue === "number" ? rawValue : Number(toText(rawValue).trim());
        const captureTime = (_j = (_i = (_h = (_g = latest["capture-time"]) != null ? _g : latest["target-time"]) != null ? _h : latest.time) != null ? _i : latest.timestamp) != null ? _j : "";
        const newCaptureTime = toText(captureTime);
        if (newCaptureTime) {
          if (!(prevTimeState == null ? void 0 : prevTimeState.val) || String(prevTimeState.val) !== newCaptureTime) {
            hasNewReading = true;
          }
        }
        if (!isNaN(rawVal)) {
          const factor = Math.pow(10, up.scaler);
          const valWh = Math.round(rawVal * factor * 100) / 100;
          const valKWh = Math.round(valWh / 1e3 * 1e4) / 1e4;
          if (readingsList.length >= 2) {
            const prev = readingsList[readingsList.length - 2];
            const prevValue = (_l = (_k = prev.value) != null ? _k : prev.val) != null ? _l : prev["reading-value"];
            const prevRaw = typeof prevValue === "number" ? prevValue : Number(toText(prevValue).trim());
            if (!isNaN(prevRaw) && rawVal >= prevRaw) {
              const deltaWh = Math.round((rawVal - prevRaw) * factor * 100) / 100;
              const avgPowerW = Math.round(deltaWh * 4 * 100) / 100;
              await this.setStateAsync(`${basePath}.deltaWh`, deltaWh, true);
              await this.setStateAsync(`${basePath}.averagePowerW`, avgPowerW, true);
            }
          } else if (prevWhState && typeof prevWhState.val === "number" && prevWhState.val > 0 && (prevTimeState == null ? void 0 : prevTimeState.val) && prevTimeState.val !== newCaptureTime && valWh >= prevWhState.val) {
            const deltaWh = Math.round((valWh - prevWhState.val) * 100) / 100;
            const timeDiffHours = (new Date(newCaptureTime).getTime() - new Date(String(prevTimeState.val)).getTime()) / (1e3 * 60 * 60);
            const avgPowerW = timeDiffHours > 0 ? Math.round(deltaWh / timeDiffHours * 100) / 100 : Math.round(deltaWh * 4 * 100) / 100;
            await this.setStateAsync(`${basePath}.deltaWh`, deltaWh, true);
            await this.setStateAsync(`${basePath}.averagePowerW`, avgPowerW, true);
          }
          await this.setStateAsync(`${basePath}.valueRaw`, rawVal, true);
          await this.setStateAsync(`${basePath}.valueWh`, valWh, true);
          await this.setStateAsync(`${basePath}.valueKWh`, valKWh, true);
          await this.setStateAsync(`${basePath}.captureTime`, newCaptureTime, true);
        }
        if (usedFullHistory || readingsList.length > 1) {
          await this.setStateAsync(`${basePath}.historyJson`, JSON.stringify(readingsList), true);
        }
      } catch (error) {
        const isBadRequest = /\b400\b/.test(error instanceof Error ? error.message : String(error));
        this.log.warn(
          `Retrieval for tariff ${up.usagePointName} failed (${getErrorMessage(error)})${isBadRequest ? "." : ". Marking the catalog for refresh."}`
        );
        if (!isBadRequest) {
          catalogRefreshNeeded = true;
        }
      }
    }
    if (catalogRefreshNeeded) {
      try {
        await this.discoverUsagePoints(true);
      } catch (err) {
        this.log.error(`Automatic catalog refresh failed: ${getErrorMessage(err)}`);
      }
    }
    return hasNewReading;
  }
  /**
   * Methode 06: self-test
   */
  async triggerSelfTest() {
    try {
      const res = await this.sendConexaRequest({ method: "self-test" });
      const st = asGatewayRecord(res["self-test"]);
      if (st) {
        const result = toText(st.result);
        const description = toText(st.description);
        await this.setStateAsync("gateway.selfTestResult", result, true);
        await this.setStateAsync("gateway.selfTestDescription", description, true);
        this.log.info(`Self-test started: ${description} (code: ${result})`);
      }
    } catch (error) {
      this.log.error(`Error starting the self-test: ${getErrorMessage(error)}`);
    }
  }
  /**
   * [DE] Räumt Adapter-Ressourcen auf | [EN] Cleans up adapter resources
   * - DE: Stoppt den Polling-Timer und schließt den HTTPS-Agent.
   * - EN: Stops the polling timer and destroys the HTTPS agent.
   *
   * @param callback Callback des Adapter-Controllers | Adapter controller callback
   */
  onUnload(callback) {
    try {
      if (this.pollTimer) {
        this.clearTimeout(this.pollTimer);
        this.pollTimer = void 0;
      }
      this.httpsAgent.destroy();
    } catch (error) {
      this.log.error(`Error cleaning up the adapter: ${getErrorMessage(error)}`);
    } finally {
      callback();
    }
  }
}
if (require.main !== module) {
  module.exports = (options) => new Conexa(options);
} else {
  (() => new Conexa())();
}
//# sourceMappingURL=main.js.map
