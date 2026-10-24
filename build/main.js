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
var crypto = __toESM(require("crypto"));
var https = __toESM(require("https"));
var import_url = require("url");
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
        this.log.error(`Fehler beim Initialisieren des Adapters: ${getErrorMessage(error)}`);
      });
    });
    this.on("stateChange", (id, state) => {
      void this.onStateChange(id, state).catch((error) => {
        this.log.error(`Fehler bei der Verarbeitung von ${id}: ${getErrorMessage(error)}`);
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
      this.log.warn("Bitte IP-Adresse, Benutzername und Passwort in den Adapter-Einstellungen hinterlegen!");
      return;
    }
    this.baseUrl = `https://${targetHost}:443/smgw/m2m/`;
    this.log.info(`Verbinde mit CONEXA 3.0 unter ${this.baseUrl} ...`);
    this.subscribeStates("gateway.triggerSelfTest");
    this.subscribeStates("usagePoints.refreshCatalog");
    await this.loadCatalogFromState();
    const hasNewData = await this.pollGatewayData();
    const intervalMinutes = Math.max(1, Number(this.config.pollInterval) || 15);
    this.log.info(`Abfrage-Intervall gestartet: alle ${intervalMinutes} Minuten.`);
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
      this.log.info("Manueller Selbsttest \xFCber Button angefordert...");
      await this.triggerSelfTest();
      await this.setStateAsync("gateway.triggerSelfTest", false, true);
    } else if (id.endsWith(".usagePoints.refreshCatalog") && state.val === true) {
      this.log.info("Manuelles Neueinlesen des Tarif-Katalogs angefordert...");
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
    let requestStep = "Schritt 1 (Initial-Request/Challenge)";
    const method = toText(payload.method);
    try {
      let requestUrl = new import_url.URL(this.baseUrl);
      const body = JSON.stringify(payload);
      const isRedirect = (statusCode) => [301, 302, 307, 308].includes(statusCode);
      const maxRedirects = 5;
      this.log.debug(
        `[API] Sende unauthentifizierten Initial-Request f\xFCr Methode "${method}" an ${this.baseUrl}`
      );
      let response = await this.requestConexa(requestUrl, body);
      this.log.debug(
        `[API] Initial-Antwort f\xFCr "${method}": Status ${response.statusCode} (Dauer: ${Date.now() - startTime}ms)`
      );
      let redirectCount = 0;
      while (isRedirect(response.statusCode)) {
        this.log.debug(
          `[API] Gateway sendet Redirect (${response.statusCode}) nach Location: ${response.location}`
        );
        if (!response.location) {
          throw new Error(`Gateway-Redirect (${response.statusCode}) enth\xE4lt keinen Location-Header.`);
        }
        if (++redirectCount > maxRedirects) {
          throw new Error(`Zu viele Gateway-Redirects (mehr als ${maxRedirects}).`);
        }
        requestUrl = new import_url.URL(response.location, requestUrl);
        this.baseUrl = requestUrl.toString();
        response = await this.requestConexa(requestUrl, body);
        this.log.debug(`[API] Redirect-Antwort f\xFCr "${method}": Status ${response.statusCode}`);
      }
      if (response.statusCode === 200) {
        return this.parseGatewayResponse(response.body);
      }
      if (response.statusCode === 403) {
        throw new Error(
          `Zugriff verweigert (403 Forbidden). Der konfigurierte Benutzer hat keine Berechtigung f\xFCr die API-Methode "${method}".`
        );
      }
      if (response.statusCode === 401) {
        this.log.debug(
          `[API] Digest-Challenge empfangen. WWW-Authenticate Header: ${JSON.stringify(response.wwwAuthenticate)}`
        );
      }
      if (response.statusCode !== 401 || !((_a = response.wwwAuthenticate) == null ? void 0 : _a.length)) {
        throw new Error(
          `Unerwarteter HTTP-Status ${response.statusCode} beim Verbindungsaufbau. Antwort: ${response.body || "Keine Antwort vom Gateway"}`
        );
      }
      const authorization = this.createDigestAuthorization(response.wwwAuthenticate, requestUrl);
      this.log.debug(
        `[API] Digest-Authorization-Header generiert (Wert wird nicht protokolliert). Sende authentifizierten Request f\xFCr "${method}"...`
      );
      requestStep = "Schritt 2 (authentifizierter Request)";
      const authStartTime = Date.now();
      let authenticatedResponse = await this.requestConexa(requestUrl, body, authorization);
      this.log.debug(
        `[API] Authentifizierte Antwort f\xFCr "${method}": Status ${authenticatedResponse.statusCode} (Dauer: ${Date.now() - authStartTime}ms)`
      );
      redirectCount = 0;
      while (isRedirect(authenticatedResponse.statusCode)) {
        this.log.debug(
          `[API] Gateway sendet Redirect (${authenticatedResponse.statusCode}) nach Location: ${authenticatedResponse.location}`
        );
        if (!authenticatedResponse.location) {
          throw new Error(
            `Gateway-Redirect (${authenticatedResponse.statusCode}) enth\xE4lt keinen Location-Header.`
          );
        }
        if (++redirectCount > maxRedirects) {
          throw new Error(`Zu viele Gateway-Redirects (mehr als ${maxRedirects}).`);
        }
        requestUrl = new import_url.URL(authenticatedResponse.location, requestUrl);
        this.baseUrl = requestUrl.toString();
        const redirectAuthorization = this.createDigestAuthorization(response.wwwAuthenticate, requestUrl);
        authenticatedResponse = await this.requestConexa(requestUrl, body, redirectAuthorization);
        this.log.debug(
          `[API] Authentifizierte Redirect-Antwort f\xFCr "${method}": Status ${authenticatedResponse.statusCode}`
        );
      }
      if (authenticatedResponse.statusCode === 401) {
        throw new Error(
          "Authentifizierung fehlgeschlagen (401 Unauthorized). Bitte \xFCberpr\xFCfen Sie Benutzername und Passwort in den Adapter-Einstellungen."
        );
      }
      if (authenticatedResponse.statusCode === 403) {
        throw new Error(
          `Zugriff verweigert (403 Forbidden). Der konfigurierte Benutzer hat keine Berechtigung f\xFCr die API-Methode "${method}".`
        );
      }
      if (authenticatedResponse.statusCode < 200 || authenticatedResponse.statusCode >= 300) {
        throw new Error(
          `Gateway meldet HTTP-Fehlerstatus ${authenticatedResponse.statusCode}: ${authenticatedResponse.body || "Keine Details"}`
        );
      }
      return this.parseGatewayResponse(authenticatedResponse.body);
    } catch (error) {
      const message = getErrorMessage(error);
      this.log.error(`[API-Fehler] Methode "${method}" fehlgeschlagen (${requestStep}): ${message}`);
      throw new Error(`API-Fehler (${method}, ${requestStep}): ${message}`);
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
      throw new Error("Gateway-Antwort ist kein JSON-Objekt.");
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
      throw new Error("Keine unterst\xFCtzte Digest-Challenge (MD5 oder SHA-256) vom Gateway erhalten.");
    }
    const { realm, nonce, qop, opaque } = selectedChallenge.params;
    const algorithm = selectedChallenge.algorithm;
    const qopAuth = qop == null ? void 0 : qop.split(",").map((value) => value.trim()).find((value) => value.toLowerCase() === "auth");
    if (qop && !qopAuth) {
      throw new Error(`Nicht unterst\xFCtzte Digest-qop: ${qop}`);
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
    try {
      const catalogState = await this.getStateAsync("usagePoints.catalogJson");
      const updatedState = await this.getStateAsync("usagePoints.lastUpdated");
      if (catalogState && typeof catalogState.val === "string" && catalogState.val !== "[]") {
        const parsed = JSON.parse(catalogState.val);
        if (Array.isArray(parsed) && parsed.length > 0 && parsed.every(isCachedUsagePoint)) {
          this.usagePointsCache = parsed;
          this.cachedUsagePointMissingObis = this.usagePointsCache.some((up) => !up.obis);
          this.lastCatalogUpdate = (updatedState == null ? void 0 : updatedState.val) ? new Date(String(updatedState.val)).getTime() : Date.now();
          this.log.info(`${this.usagePointsCache.length} Tarife (Usage Points) aus lokalem Cache geladen.`);
          for (const up of this.usagePointsCache) {
            if (!up.database) {
              up.database = "origin";
            }
            await this.ensureUsagePointObjects(up);
          }
        }
      }
    } catch (error) {
      this.log.warn(`Konnte Tarif-Cache nicht lesen, lese neu vom Gateway ein: ${getErrorMessage(error)}`);
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
      this.log.error(`Fehler beim Abruf vom Gateway: ${getErrorMessage(error)}`);
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
    const nextRunIso = new Date(Date.now() + delayMs).toISOString();
    await this.setStateAsync("gateway.nextPollTime", nextRunIso, true);
    this.log.debug(`N\xE4chster synchronisierter Abruf geplant f\xFCr ${nextRunIso} (in ${Math.round(delayMs / 1e3)}s)`);
    this.pollTimer = this.setTimeout(async () => {
      this.pollTimer = void 0;
      try {
        const newData = await this.pollGatewayData();
        await this.scheduleNextPoll(newData);
      } catch (error) {
        this.log.error(`Fehler im adaptiven Polling: ${getErrorMessage(error)}`);
      }
    }, delayMs);
  }
  /**
   * Legt die zusätzlichen Statusdatenpunkte für Übermittlungen und Polling an.
   */
  async ensureGatewayStatsObjects() {
    await this.setObjectNotExistsAsync("gateway", {
      type: "channel",
      common: { name: "Gateway" },
      native: {}
    });
    await this.setObjectNotExistsAsync("gateway.marketTransmissions", {
      type: "channel",
      common: { name: "\xDCbermittlungen an Netzbetreiber" },
      native: {}
    });
    const states = [
      {
        id: "gateway.marketTransmissions.totalCount",
        name: "Gesamtzahl \xDCbermittlungen an Netzbetreiber",
        type: "number",
        role: "value",
        def: 0
      },
      {
        id: "gateway.marketTransmissions.todayCount",
        name: "\xDCbermittlungen heute",
        type: "number",
        role: "value",
        def: 0
      },
      {
        id: "gateway.marketTransmissions.lastTimestamp",
        name: "Letzter \xDCbermittlungs-Zeitpunkt",
        type: "string",
        role: "value.time",
        def: ""
      },
      {
        id: "gateway.marketTransmissions.historyJson",
        name: "Historie aller \xDCbermittlungen (JSON)",
        type: "string",
        role: "json",
        def: "[]"
      },
      {
        id: "gateway.logTableJson",
        name: "Gateway-Log als strukturierte Tabelle (JSON)",
        type: "string",
        role: "json",
        def: "[]"
      },
      {
        id: "gateway.nextPollTime",
        name: "N\xE4chster geplanter Abruf",
        type: "string",
        role: "value.time",
        def: ""
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
    }
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
    await this.setStateAsync("gateway.time", asStateValue(info["smgw-time"]), true);
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
    var _a, _b;
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
        await this.setStateAsync("logs.lastTime", asStateValue(latest.time), true);
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
            this.log.warn("Historie der Netzbetreiber-\xDCbermittlungen ist kein JSON-Array; starte sie neu.");
          }
        } catch (error) {
          this.log.warn(
            `Historie der Netzbetreiber-\xDCbermittlungen ist ung\xFCltig und wird neu aufgebaut: ${error instanceof Error ? error.message : String(error)}`
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
        this.log.info(`Neue \xDCbermittlung an Netzbetreiber (Event 16001) registriert um ${loggedEvent.time}`);
      }
      const today = /* @__PURE__ */ new Date();
      const todayCount = history.filter((entry) => {
        const eventDate = new Date(entry.time);
        return Number.isFinite(eventDate.getTime()) && eventDate.getFullYear() === today.getFullYear() && eventDate.getMonth() === today.getMonth() && eventDate.getDate() === today.getDate();
      }).length;
      await this.setStateAsync("gateway.marketTransmissions.totalCount", history.length, true);
      await this.setStateAsync("gateway.marketTransmissions.todayCount", todayCount, true);
      await this.setStateAsync("gateway.marketTransmissions.lastTimestamp", (_b = lastMarket == null ? void 0 : lastMarket.time) != null ? _b : "", true);
      await this.setStateAsync("gateway.marketTransmissions.historyJson", JSON.stringify(history), true);
      if (lastMarket) {
        await this.setStateAsync(
          "gateway.lastMarketTransmission",
          `${lastMarket.time} - ${lastMarket.message}`,
          true
        );
      }
    } catch (error) {
      this.log.debug(`Log-Abfrage \xFCbersprungen: ${getErrorMessage(error)}`);
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
      this.log.info("Scanne Gateway nach freigeschalteten Tarifen (user-info)...");
    }
    const res = await this.sendConexaRequest({ method: "user-info" });
    const userInfo = asGatewayRecord(res["user-info"]);
    const rawList = asGatewayRecords(userInfo == null ? void 0 : userInfo["usage-points"]);
    const discovered = [];
    for (const item of rawList) {
      const upId = item["usage-point-id"];
      const upName = item["usage-point-name"] || upId;
      if (!upId) {
        continue;
      }
      const usagePointId = toText(upId);
      const usagePointName = String(upName);
      const cleanId = usagePointName.replace(/[^a-zA-Z0-9_-]/g, "_");
      let database = "origin";
      let obis = "";
      let scaler = -1;
      let unit = "30";
      try {
        const detailRes = await this.sendConexaRequest({
          method: "usage-point-info",
          "usage-point-id": usagePointId
        });
        this.log.debug(`[usage-point-info] Rohdaten f\xFCr ${usagePointName}: ${JSON.stringify(detailRes)}`);
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
          `Detailabfrage usage-point-info f\xFCr ${usagePointName} lieferte keinen Kanal: ${getErrorMessage(err)}`
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
    await this.setStateAsync("usagePoints.lastUpdated", (/* @__PURE__ */ new Date()).toISOString(), true);
    this.log.info(`${discovered.length} Tarife erfolgreich eingelesen und im Katalog gespeichert.`);
  }
  /**
   * Legt die Datenpunkte für einen gefundenen Tarif dynamisch unter usagePoints.<Tarifname> an
   *
   * @param up Zwischengespeicherte Tarifdaten für den anzulegenden Datenpunkt
   */
  async ensureUsagePointObjects(up) {
    const basePath = `usagePoints.${up.cleanId}`;
    await this.setObjectNotExistsAsync(basePath, {
      type: "channel",
      common: {
        name: `${up.usagePointName} (TAF ${up.tafNumber})`
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.usagePointId`, {
      type: "state",
      common: { name: "Usage Point ID", type: "string", role: "text", read: true, write: false, def: "" },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.meterId`, {
      type: "state",
      common: {
        name: "Meter ID (Z\xE4hlernummer)",
        type: "string",
        role: "info.serial",
        read: true,
        write: false,
        def: ""
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.tafState`, {
      type: "state",
      common: { name: "TAF Status", type: "string", role: "info.status", read: true, write: false, def: "" },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.obis`, {
      type: "state",
      common: { name: "OBIS Kennzahl", type: "string", role: "text", read: true, write: false, def: "" },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.valueRaw`, {
      type: "state",
      common: { name: "Rohwert vom Z\xE4hler", type: "number", role: "value", read: true, write: false, def: 0 },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.valueWh`, {
      type: "state",
      common: {
        name: "Z\xE4hlerstand (Wh)",
        type: "number",
        role: "value.energy",
        unit: "Wh",
        read: true,
        write: false,
        def: 0
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.valueKWh`, {
      type: "state",
      common: {
        name: "Z\xE4hlerstand (kWh)",
        type: "number",
        role: "value.energy",
        unit: "kWh",
        read: true,
        write: false,
        def: 0
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.deltaWh`, {
      type: "state",
      common: {
        name: "Verbrauch letztes Intervall (Wh)",
        type: "number",
        role: "value.energy.consumption",
        unit: "Wh",
        read: true,
        write: false,
        def: 0
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.averagePowerW`, {
      type: "state",
      common: {
        name: "Durchschnittsleistung letztes Intervall (W)",
        type: "number",
        role: "value.power",
        unit: "W",
        read: true,
        write: false,
        def: 0
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.captureTime`, {
      type: "state",
      common: {
        name: "Zeitpunkt der Erfassung",
        type: "string",
        role: "value.time",
        read: true,
        write: false,
        def: ""
      },
      native: {}
    });
    await this.setObjectNotExistsAsync(`${basePath}.historyJson`, {
      type: "state",
      common: {
        name: "Historie / Lastgang (JSON)",
        type: "string",
        role: "json",
        read: true,
        write: false,
        def: "[]"
      },
      native: {}
    });
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
          this.log.debug(`[readings] Sende Payload f\xFCr ${up.usagePointName}: ${JSON.stringify(payload)}`);
          res = await this.sendConexaRequest(payload);
        } catch (error) {
          if (!fetchFullHistory || !/\b400\b/.test(error instanceof Error ? error.message : String(error))) {
            throw error;
          }
          this.log.warn(
            `Vollhistorie f\xFCr Tarif ${up.usagePointName} mit HTTP 400 abgelehnt; frage stattdessen den letzten Messwert ab.`
          );
          usedFullHistory = false;
          delete payload["from-time"];
          payload["last-reading"] = true;
          try {
            this.log.debug(`[readings] Sende Payload f\xFCr ${up.usagePointName}: ${JSON.stringify(payload)}`);
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
          this.log.debug(`[readings] Rohdaten f\xFCr ${up.usagePointName}: ${JSON.stringify(res)}`);
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
              `[readings] Sende Payload f\xFCr ${up.usagePointName}: ${JSON.stringify(fallbackPayload)}`
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
                `[readings] Fallback f\xFCr ${up.usagePointName} fehlgeschlagen: ${error instanceof Error ? error.message : String(error)}`
              );
            }
          }
        }
        if (readingsList.length === 0) {
          if (!shouldLogRaw) {
            this.log.debug(`[readings] Rohdaten f\xFCr ${up.usagePointName}: ${JSON.stringify(res)}`);
          }
          this.log.warn(
            `Keine Messwerte in readings-Antwort f\xFCr ${up.usagePointName} gefunden. JSON-Struktur: ${JSON.stringify(res).slice(0, 500)}`
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
          `Abruf f\xFCr Tarif ${up.usagePointName} fehlgeschlagen (${getErrorMessage(error)})${isBadRequest ? "." : ". Markiere Katalog zur Aktualisierung."}`
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
        this.log.error(`Automatische Katalog-Aktualisierung fehlgeschlagen: ${getErrorMessage(err)}`);
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
        this.log.info(`Selbsttest gestartet: ${description} (Code: ${result})`);
      }
    } catch (error) {
      this.log.error(`Fehler beim Starten des Selbsttests: ${getErrorMessage(error)}`);
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
      this.log.error(`Fehler beim Aufr\xE4umen des Adapters: ${getErrorMessage(error)}`);
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
