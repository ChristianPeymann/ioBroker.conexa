import * as utils from "@iobroker/adapter-core";
import * as crypto from "node:crypto";
import * as https from "node:https";
import { URL } from "node:url";

interface CachedUsagePoint {
	usagePointId: string;
	usagePointName: string;
	cleanId: string;
	tafNumber: string;
	tafState: string;
	meterId: string;
	meteringPointId: string;
	database: string;
	obis?: string;
	scaler: number;
	unit: string;
}

type GatewayRecord = Record<string, unknown>;
type GatewayRequest = Record<string, unknown>;

/**
 * [DE] Prüft Gateway-Objekte | [EN] Validates gateway objects
 * - DE: Stellt sicher, dass ein unbekannter Wert ein nicht-null JSON-Objekt ist.
 * - EN: Ensures an unknown value is a non-null JSON object.
 *
 * @param value Zu prüfender Wert | Value to validate
 */
function isGatewayRecord(value: unknown): value is GatewayRecord {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * [DE] Liest Gateway-Objekte sicher | [EN] Reads gateway objects safely
 * - DE: Gibt ein validiertes Objekt zurück oder `undefined`.
 * - EN: Returns a validated object or `undefined`.
 *
 * @param value Zu prüfender Wert | Value to validate
 */
function asGatewayRecord(value: unknown): GatewayRecord | undefined {
	return isGatewayRecord(value) ? value : undefined;
}

/**
 * [DE] Liest Gateway-Listen sicher | [EN] Reads gateway lists safely
 * - DE: Gibt ein Array unbekannter Einträge oder eine leere Liste zurück.
 * - EN: Returns an array of unknown entries or an empty list.
 *
 * @param value Zu prüfender Wert | Value to validate
 */
function asGatewayArray(value: unknown): unknown[] {
	return Array.isArray(value) ? value : [];
}

/**
 * [DE] Filtert Gateway-Objekte | [EN] Filters gateway objects
 * - DE: Entfernt ungültige Einträge aus einer Gateway-Liste.
 * - EN: Removes invalid entries from a gateway list.
 *
 * @param value Zu prüfender Wert | Value to validate
 */
function asGatewayRecords(value: unknown): GatewayRecord[] {
	return asGatewayArray(value).filter(isGatewayRecord);
}

/**
 * [DE] Prüft einen Cache-Tarif | [EN] Validates a cached usage point
 * - DE: Prüft die Pflichtfelder eines zwischengespeicherten Tarifs.
 * - EN: Checks the required fields of a cached usage point.
 *
 * @param value Zu prüfender Cache-Eintrag | Cache entry to validate
 */
function isCachedUsagePoint(value: unknown): value is CachedUsagePoint {
	if (!isGatewayRecord(value)) {
		return false;
	}
	return (
		typeof value.usagePointId === "string" &&
		typeof value.usagePointName === "string" &&
		typeof value.cleanId === "string" &&
		typeof value.tafNumber === "string" &&
		typeof value.tafState === "string" &&
		typeof value.meterId === "string" &&
		typeof value.meteringPointId === "string" &&
		typeof value.database === "string" &&
		(value.obis === undefined || typeof value.obis === "string") &&
		typeof value.scaler === "number" &&
		typeof value.unit === "string"
	);
}

/**
 * [DE] Formatiert einen State-Wert | [EN] Formats a state value
 * - DE: Erhält primitive Werte und ersetzt komplexe Werte durch einen Standardwert.
 * - EN: Preserves primitive values and replaces complex values with a fallback.
 *
 * @param value Zu formatierender Wert | Value to format
 * @param fallback Ersatzwert | Fallback value
 */
function asStateValue(value: unknown, fallback: ioBroker.StateValue = ""): ioBroker.StateValue {
	if (!value) {
		return fallback;
	}
	if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
		return value;
	}
	return fallback;
}

function toTimestamp(value: unknown): number | undefined {
	if (typeof value === "number") {
		return Number.isFinite(value) ? value : undefined;
	}
	if (typeof value !== "string" || value.trim() === "") {
		return undefined;
	}

	const trimmedValue = value.trim();
	const numericValue = Number(trimmedValue);
	if (/^\d+(?:\.\d+)?$/.test(trimmedValue) && Number.isFinite(numericValue)) {
		return numericValue;
	}

	const timestamp = Date.parse(trimmedValue);
	return Number.isFinite(timestamp) ? timestamp : undefined;
}

/**
 * [DE] Wandelt einen Wert in Text um | [EN] Converts a value to text
 * - DE: Erhält primitive Gateway-Werte und serialisiert komplexe Werte sicher.
 * - EN: Preserves primitive gateway values and safely serializes complex values.
 *
 * @param value Zu formatierender Wert | Value to format
 * @param fallback Ersatztext | Fallback text
 */
function toText(value: unknown, fallback = ""): string {
	if (value === null || value === undefined) {
		return fallback;
	}
	if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
		return String(value);
	}
	return JSON.stringify(value) ?? fallback;
}

/**
 * [DE] Formatiert einen Fehler | [EN] Formats an error
 * - DE: Liefert eine lesbare Meldung auch für nicht-standardisierte Fehlerwerte.
 * - EN: Produces a readable message for non-standard error values.
 *
 * @param error Fehlerwert | Error value
 */
function getErrorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

class Conexa extends utils.Adapter {
	private pollTimer: ioBroker.Timeout | undefined;
	private baseUrl = "";
	private httpsAgent = new https.Agent({ rejectUnauthorized: false, keepAlive: true });
	private usagePointsCache: CachedUsagePoint[] = [];
	private lastCatalogUpdate = 0;
	private latestReadingTimestamp = 0;
	private latestMarketEventTimestamp = 0;
	private pollRetryCount = 0;
	private readingsRawLogged = false;
	private cachedUsagePointMissingObis = false;
	private missingObisRefreshAttempted = false;

	/**
	 * [DE] Initialisiert den Adapter | [EN] Initializes the adapter
	 * - DE: Registriert Handler für die Adapter-Ereignisse.
	 * - EN: Registers handlers for adapter events.
	 *
	 * @param options Adapter-Optionen | Adapter options
	 */
	public constructor(options: Partial<utils.AdapterOptions> = {}) {
		super({
			...options,
			name: "conexa",
		});
		this.on("ready", () => {
			void this.onReady().catch(error => {
				this.log.error(`Error initializing the adapter: ${getErrorMessage(error)}`);
			});
		});
		this.on("stateChange", (id, state) => {
			void this.onStateChange(id, state).catch(error => {
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
	private async onReady(): Promise<void> {
		await this.setStateAsync("info.connection", false, true);
		await this.ensureGatewayStatsObjects();

		const rawIp = (this.config.useIpv6 ? this.config.ipV6 : this.config.ipV4)?.trim() || "";
		const targetHost = this.config.useIpv6 && !rawIp.startsWith("[") ? `[${rawIp}]` : rawIp;

		if (!targetHost || !this.config.username || !this.config.password) {
			this.log.warn("Please configure the IP address, username, and password in the adapter settings!");
			return;
		}

		this.baseUrl = `https://${targetHost}:443/smgw/m2m/`;
		this.log.info(`Connecting to CONEXA 3.0 at ${this.baseUrl} ...`);

		// Buttons abonnieren (Selbsttest & Tarif-Katalog neu einlesen)
		this.subscribeStates("gateway.triggerSelfTest");
		this.subscribeStates("usagePoints.refreshCatalog");

		// Vorhandenen Tarif-Katalog aus dem Datenpunkt laden (falls vorhanden)
		await this.loadCatalogFromState();

		// Ersten kompletten Abruf starten
		const hasNewData = await this.pollGatewayData();

		// Poll-Intervall aus der Config (in Minuten, Standard: 15 Min, mindestens 1 Min)
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
	private async onStateChange(id: string, state: ioBroker.State | null | undefined): Promise<void> {
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
	private async sendConexaRequest(payload: GatewayRequest): Promise<GatewayRecord> {
		const startTime = Date.now();
		let requestStep = "Schritt 1 (Initial-Request/Challenge)";
		const method = toText(payload.method);
		try {
			let requestUrl = new URL(this.baseUrl);
			const body = JSON.stringify(payload);
			/**
			 * [DE] Prüft Redirect-Statuscodes | [EN] Checks redirect status codes
			 * - DE: Erkennt unterstützte HTTP-Redirects des Gateways.
			 * - EN: Detects supported gateway HTTP redirects.
			 *
			 * @param statusCode HTTP-Statuscode | HTTP status code
			 */
			const isRedirect = (statusCode: number): boolean => [301, 302, 307, 308].includes(statusCode);
			const maxRedirects = 5;

			this.log.debug(`[API] Sending unauthenticated initial request for method "${method}" to ${this.baseUrl}`);
			let response = await this.requestConexa(requestUrl, body);
			this.log.debug(
				`[API] Initial response for "${method}": status ${response.statusCode} (duration: ${Date.now() - startTime}ms)`,
			);

			let redirectCount = 0;
			while (isRedirect(response.statusCode)) {
				this.log.debug(
					`[API] Gateway sent redirect (${response.statusCode}) to Location: ${response.location}`,
				);
				if (!response.location) {
					throw new Error(`Gateway-Redirect (${response.statusCode}) enthält keinen Location-Header.`);
				}
				if (++redirectCount > maxRedirects) {
					throw new Error(`Zu viele Gateway-Redirects (mehr als ${maxRedirects}).`);
				}
				requestUrl = new URL(response.location, requestUrl);
				this.baseUrl = requestUrl.toString();
				response = await this.requestConexa(requestUrl, body);
				this.log.debug(`[API] Redirect response for "${method}": status ${response.statusCode}`);
			}

			if (response.statusCode === 200) {
				return this.parseGatewayResponse(response.body);
			}

			if (response.statusCode === 403) {
				throw new Error(
					`Zugriff verweigert (403 Forbidden). Der konfigurierte Benutzer hat keine Berechtigung für die API-Methode "${method}".`,
				);
			}

			if (response.statusCode === 401) {
				this.log.debug(
					`[API] Digest challenge received. WWW-Authenticate header: ${JSON.stringify(response.wwwAuthenticate)}`,
				);
			}

			if (response.statusCode !== 401 || !response.wwwAuthenticate?.length) {
				throw new Error(
					`Unerwarteter HTTP-Status ${response.statusCode} beim Verbindungsaufbau. Antwort: ${
						response.body || "Keine Antwort vom Gateway"
					}`,
				);
			}

			const authorization = this.createDigestAuthorization(response.wwwAuthenticate, requestUrl);
			this.log.debug(
				`[API] Digest authorization header generated (value will not be logged). Sending authenticated request for "${method}"...`,
			);

			requestStep = "Schritt 2 (authentifizierter Request)";
			const authStartTime = Date.now();
			let authenticatedResponse = await this.requestConexa(requestUrl, body, authorization);
			this.log.debug(
				`[API] Authenticated response for "${method}": status ${authenticatedResponse.statusCode} (duration: ${Date.now() - authStartTime}ms)`,
			);

			redirectCount = 0;
			while (isRedirect(authenticatedResponse.statusCode)) {
				this.log.debug(
					`[API] Gateway sent redirect (${authenticatedResponse.statusCode}) to Location: ${authenticatedResponse.location}`,
				);
				if (!authenticatedResponse.location) {
					throw new Error(
						`Gateway-Redirect (${authenticatedResponse.statusCode}) enthält keinen Location-Header.`,
					);
				}
				if (++redirectCount > maxRedirects) {
					throw new Error(`Zu viele Gateway-Redirects (mehr als ${maxRedirects}).`);
				}
				requestUrl = new URL(authenticatedResponse.location, requestUrl);
				this.baseUrl = requestUrl.toString();
				const redirectAuthorization = this.createDigestAuthorization(response.wwwAuthenticate, requestUrl);
				authenticatedResponse = await this.requestConexa(requestUrl, body, redirectAuthorization);
				this.log.debug(
					`[API] Authenticated redirect response for "${method}": status ${authenticatedResponse.statusCode}`,
				);
			}

			if (authenticatedResponse.statusCode === 401) {
				throw new Error(
					"Authentifizierung fehlgeschlagen (401 Unauthorized). Bitte überprüfen Sie Benutzername und Passwort in den Adapter-Einstellungen.",
				);
			}

			if (authenticatedResponse.statusCode === 403) {
				throw new Error(
					`Zugriff verweigert (403 Forbidden). Der konfigurierte Benutzer hat keine Berechtigung für die API-Methode "${method}".`,
				);
			}

			if (authenticatedResponse.statusCode < 200 || authenticatedResponse.statusCode >= 300) {
				throw new Error(
					`Gateway meldet HTTP-Fehlerstatus ${authenticatedResponse.statusCode}: ${
						authenticatedResponse.body || "Keine Details"
					}`,
				);
			}

			return this.parseGatewayResponse(authenticatedResponse.body);
		} catch (error) {
			const message = getErrorMessage(error);
			this.log.error(`[API Error] Method "${method}" failed (${requestStep}): ${message}`);
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
	private parseGatewayResponse(body: string): GatewayRecord {
		const response: unknown = JSON.parse(body);
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
	private requestConexa(
		requestUrl: URL,
		body: string,
		authorization?: string,
	): Promise<{ statusCode: number; body: string; wwwAuthenticate?: string[]; location?: string }> {
		return new Promise((resolve, reject) => {
			const bodyBuffer = Buffer.from(body, "utf8");
			const headers: Record<string, string | number> = {
				"Content-Type": "application/json",
				"Content-Length": bodyBuffer.byteLength,
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
					timeout: 15000,
				},
				response => {
					const chunks: Buffer[] = [];
					const wwwAuthenticate: string[] = [];
					for (let index = 0; index < response.rawHeaders.length; index += 2) {
						if (response.rawHeaders[index].toLowerCase() === "www-authenticate") {
							wwwAuthenticate.push(response.rawHeaders[index + 1]);
						}
					}
					response.on("data", (chunk: Buffer | string) => {
						chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
					});
					response.on("error", reject);
					response.on("end", () => {
						resolve({
							statusCode: response.statusCode ?? 0,
							body: Buffer.concat(chunks).toString("utf8"),
							wwwAuthenticate:
								wwwAuthenticate.length > 0
									? wwwAuthenticate
									: this.getAuthenticateHeaders(response.headers["www-authenticate"]),
							location:
								typeof response.headers.location === "string" ? response.headers.location : undefined,
						});
					});
				},
			);

			request.on("error", reject);
			request.setTimeout(15000, () => {
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
	private getAuthenticateHeaders(header: string | string[] | undefined): string[] {
		if (!header) {
			return [];
		}
		return (Array.isArray(header) ? header : [header]).flatMap(value => value.split(/,\s*(?=Digest\s+)/i));
	}

	/**
	 * Erstellt den Digest-Authorization-Header aus der Gateway-Challenge.
	 *
	 * @param challenges Werte der WWW-Authenticate-Header
	 * @param requestUrl URL des Gateway-Endpunkts
	 */
	private createDigestAuthorization(challenges: string[], requestUrl: URL): string {
		const parameterPattern = /(?:^|,\s*)([a-zA-Z][\w-]*)=(?:"((?:\\.|[^"])*)"|([^,\s]+))/g;
		const parsedChallenges = challenges.flatMap(challenge => {
			const digestParams = challenge.match(/^\s*Digest\s+(.+)$/i)?.[1];
			if (!digestParams) {
				return [];
			}
			const params: Record<string, string> = {};
			for (const match of digestParams.matchAll(parameterPattern)) {
				params[match[1].toLowerCase()] = (match[2] ?? match[3]).replace(/\\(.)/g, "$1");
			}
			const algorithm = params.algorithm ?? "MD5";
			if (!params.realm || !params.nonce || !["MD5", "SHA-256"].includes(algorithm.toUpperCase())) {
				return [];
			}
			return [{ params, algorithm: algorithm.toUpperCase() }];
		});
		const selectedChallenge =
			parsedChallenges.find(challenge => challenge.algorithm === "MD5") ??
			parsedChallenges.find(challenge => challenge.algorithm === "SHA-256");
		if (!selectedChallenge) {
			throw new Error("Keine unterstützte Digest-Challenge (MD5 oder SHA-256) vom Gateway erhalten.");
		}

		const { realm, nonce, qop, opaque } = selectedChallenge.params;
		const algorithm = selectedChallenge.algorithm;

		const qopAuth = qop
			?.split(",")
			.map(value => value.trim())
			.find(value => value.toLowerCase() === "auth");
		if (qop && !qopAuth) {
			throw new Error(`Nicht unterstützte Digest-qop: ${qop}`);
		}

		const uri = `${requestUrl.pathname}${requestUrl.search}`;
		const hash = (value: string): string =>
			crypto.createHash(algorithm.toLowerCase().replace("-", "")).update(value).digest("hex");
		const ha1 = hash(`${this.config.username}:${realm}:${this.config.password}`);
		const ha2 = hash(`POST:${uri}`);
		const cnonce = crypto.randomBytes(8).toString("hex");
		const nc = "00000001";
		const response = qopAuth
			? hash(`${ha1}:${nonce}:${nc}:${cnonce}:${qopAuth}:${ha2}`)
			: hash(`${ha1}:${nonce}:${ha2}`);
		const quote = (value: string): string => `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
		const authorizationParams = [
			`username=${quote(this.config.username)}`,
			`realm=${quote(realm)}`,
			`nonce=${quote(nonce)}`,
			`uri=${quote(uri)}`,
			`response=${quote(response)}`,
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
	private async loadCatalogFromState(): Promise<void> {
		try {
			const catalogState = await this.getStateAsync("usagePoints.catalogJson");
			const updatedState = await this.getStateAsync("usagePoints.lastUpdated");

			if (catalogState && typeof catalogState.val === "string" && catalogState.val !== "[]") {
				const parsed: unknown = JSON.parse(catalogState.val);
				if (Array.isArray(parsed) && parsed.length > 0 && parsed.every(isCachedUsagePoint)) {
					this.usagePointsCache = parsed;
					this.cachedUsagePointMissingObis = this.usagePointsCache.some(up => !up.obis);
					this.lastCatalogUpdate = toTimestamp(updatedState?.val) ?? Date.now();
					this.log.info(
						`${this.usagePointsCache.length} tariffs (usage points) loaded from the local cache.`,
					);
					for (const up of this.usagePointsCache) {
						if (!up.database) {
							up.database = "origin";
						}
						await this.ensureUsagePointObjects(up);
					}
				}
			}
		} catch (error: unknown) {
			this.log.warn(`Could not read the tariff cache; refreshing it from the gateway: ${getErrorMessage(error)}`);
			this.usagePointsCache = [];
		}
	}

	/**
	 * Haupt-Abrufroutine (wird im eingestellten Intervall ausgeführt)
	 */
	private async pollGatewayData(): Promise<boolean> {
		try {
			// 1. Gateway-Info abrufen (Methode 01: smgw-info)
			await this.pollSmgwInfo();

			// 2. Logs & letzte Netzbetreiber-Übertragung prüfen (Methode 04: log)
			await this.pollLogs();

			// 3. Prüfen, ob Tarif-Katalog leer oder älter als 24 Stunden ist
			const catalogAgeHours = (Date.now() - this.lastCatalogUpdate) / (1000 * 60 * 60);
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

			// 4. Messwerte für alle Tarife abrufen (Methode 05: readings)
			const hasNewData = await this.pollReadings(isInitialOrDailyRefresh);

			// Verbindung erfolgreich -> Ampel auf Grün
			await this.setStateAsync("info.connection", true, true);
			return hasNewData;
		} catch (error: unknown) {
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
	private async scheduleNextPoll(hasNewData: boolean): Promise<void> {
		const intervalMs = Math.max(1, Number(this.config.pollInterval) || 15) * 60 * 1000;
		const correctionOffsetMs = 15_000;
		const retryDelayMs = 30_000;
		let delayMs: number | undefined;

		if (hasNewData) {
			this.pollRetryCount = 0;
		} else if (this.pollRetryCount < 3) {
			this.pollRetryCount++;
			delayMs = retryDelayMs;
		} else {
			this.pollRetryCount = 0;
		}

		if (delayMs === undefined) {
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
			`Next synchronized poll scheduled for ${new Date(nextRunTimestamp).toISOString()} (in ${Math.round(delayMs / 1000)}s)`,
		);

		this.pollTimer = this.setTimeout(async () => {
			this.pollTimer = undefined;
			try {
				const newData = await this.pollGatewayData();
				await this.scheduleNextPoll(newData);
			} catch (error: unknown) {
				this.log.error(`Error in adaptive polling: ${getErrorMessage(error)}`);
			}
		}, delayMs);
	}

	/**
	 * Legt die zusätzlichen Statusdatenpunkte für Übermittlungen und Polling an.
	 */
	private async ensureGatewayStatsObjects(): Promise<void> {
		await this.setObjectNotExistsAsync("gateway", {
			type: "channel",
			common: {
				name: {
					en: "Gateway",
					de: "Gateway",
					ru: "Шлюз",
					pt: "Gateway",
					nl: "Gateway",
					fr: "Passerelle",
					it: "Gateway",
					es: "Pasarela",
					pl: "Bramka",
					uk: "Шлюз",
					"zh-cn": "网关",
				},
			},
			native: {},
		});
		await this.setObjectNotExistsAsync("gateway.marketTransmissions", {
			type: "channel",
			common: {
				name: {
					en: "Transmissions to grid operator",
					de: "Übermittlungen an Netzbetreiber",
					ru: "Передачи оператору сетевой системы",
					pt: "Transmissões para o operador da rede",
					nl: "Overdracht aan netbeheerder",
					fr: "Transmissions à l'opérateur réseau",
					it: "Trasmissioni al gestore di rete",
					es: "Transmisiones al operador de red",
					pl: "Przekazywanie do operatora sieci",
					uk: "Передачі оператору мережі",
					"zh-cn": "向电网运营商传输",
				},
			},
			native: {},
		});

		const states: Array<{
			id: string;
			name: {
				en: string;
				de: string;
				ru: string;
				pt: string;
				nl: string;
				fr: string;
				it: string;
				es: string;
				pl: string;
				uk: string;
				"zh-cn": string;
			};
			type: "number" | "string";
			role: string;
			def: number | string;
		}> = [
			{
				id: "gateway.marketTransmissions.totalCount",
				name: {
					en: "Total transmissions to grid operator",
					de: "Gesamtzahl Übermittlungen an Netzbetreiber",
					ru: "Общее число передач оператору сетевой системы",
					pt: "Número total de transmissões para o operador da rede",
					nl: "Totaal aantal overdrachten aan netbeheerder",
					fr: "Nombre total de transmissions à l'opérateur réseau",
					it: "Numero totale di trasmissioni al gestore di rete",
					es: "Número total de transmisiones al operador de red",
					pl: "Łączna liczba przekazań do operatora sieci",
					uk: "Загальна кількість передач оператору мережі",
					"zh-cn": "向电网运营商传输总数",
				},
				type: "number",
				role: "value",
				def: 0,
			},
			{
				id: "gateway.marketTransmissions.todayCount",
				name: {
					en: "Transmissions today",
					de: "Übermittlungen heute",
					ru: "Передачи сегодня",
					pt: "Transmissões hoje",
					nl: "Overdrachten vandaag",
					fr: "Transmissions aujourd'hui",
					it: "Trasmissioni oggi",
					es: "Transmisiones hoy",
					pl: "Przekazania dzisiaj",
					uk: "Передачі сьогодні",
					"zh-cn": "今日传输",
				},
				type: "number",
				role: "value",
				def: 0,
			},
			{
				id: "gateway.marketTransmissions.lastTimestamp",
				name: {
					en: "Last transmission timestamp",
					de: "Letzter Übermittlungs-Zeitpunkt",
					ru: "Время последней передачи",
					pt: "Data e hora da última transmissão",
					nl: "Tijdstempel laatste overdracht",
					fr: "Horodatage de la dernière transmission",
					it: "Ora dell'ultima trasmissione",
					es: "Marca de tiempo de la última transmisión",
					pl: "Czas ostatniego przekazania",
					uk: "Час останньої передачі",
					"zh-cn": "上次传输时间戳",
				},
				type: "number",
				role: "value.time",
				def: 0,
			},
			{
				id: "gateway.marketTransmissions.historyJson",
				name: {
					en: "Transmission history (JSON)",
					de: "Historie aller Übermittlungen (JSON)",
					ru: "История всех передач (JSON)",
					pt: "Histórico de transmissões (JSON)",
					nl: "Geschiedenis van alle overdrachten (JSON)",
					fr: "Historique de toutes les transmissions (JSON)",
					it: "Cronologia di tutte le trasmissioni (JSON)",
					es: "Historial de todas las transmisiones (JSON)",
					pl: "Historia wszystkich przekazań (JSON)",
					uk: "Історія всіх передач (JSON)",
					"zh-cn": "所有传输历史 (JSON)",
				},
				type: "string",
				role: "json",
				def: "[]",
			},
			{
				id: "gateway.logTableJson",
				name: {
					en: "Gateway log as structured table (JSON)",
					de: "Gateway-Log als strukturierte Tabelle (JSON)",
					ru: "Журнал шлюза в виде структурированной таблицы (JSON)",
					pt: "Log do gateway como tabela estruturada (JSON)",
					nl: "Gatewaylog als gestructureerde tabel (JSON)",
					fr: "Journal de la passerelle sous forme de tableau structuré (JSON)",
					it: "Log del gateway come tabella strutturata (JSON)",
					es: "Registro del gateway como tabla estructurada (JSON)",
					pl: "Dziennik bramki jako tabela strukturalna (JSON)",
					uk: "Журнал шлюзу як структурована таблиця (JSON)",
					"zh-cn": "网关日志结构化表 (JSON)",
				},
				type: "string",
				role: "json",
				def: "[]",
			},
			{
				id: "gateway.nextPollTime",
				name: {
					en: "Next scheduled poll",
					de: "Nächster geplanter Abruf",
					ru: "Следующий запланированный опрос",
					pt: "Próxima consulta programada",
					nl: "Volgende geplande polling",
					fr: "Prochaine interrogation planifiée",
					it: "Prossima interrogazione pianificata",
					es: "Próxima consulta programada",
					pl: "Następne zaplanowane odczytywanie",
					uk: "Наступне заплановане опитування",
					"zh-cn": "下次计划轮询",
				},
				type: "number",
				role: "value.time",
				def: 0,
			},
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
					def: state.def,
				},
				native: {},
			});
		}
	}

	/**
	 * Methode 01: smgw-info
	 */
	private async pollSmgwInfo(): Promise<void> {
		const res = await this.sendConexaRequest({ method: "smgw-info" });
		const info = asGatewayRecord(res["smgw-info"]);
		if (!info) {
			return;
		}

		const firmwareInfo = asGatewayRecord(info["firmware-info"]);
		await this.setStateAsync("gateway.id", asStateValue(info["smgw-id"]), true);
		await this.setStateAsync("gateway.state", asStateValue(info["smgw-state"]), true);
		const gatewayTimestamp = toTimestamp(info["smgw-time"]);
		if (gatewayTimestamp === undefined) {
			this.log.warn("Invalid timestamp in smgw-info; gateway.time was not updated.");
		} else {
			await this.setStateAsync("gateway.time", gatewayTimestamp, true);
		}
		await this.setStateAsync("gateway.firmwareVersion", asStateValue(firmwareInfo?.version), true);
		await this.setStateAsync("gateway.firmwareHash", asStateValue(firmwareInfo?.hash), true);
		await this.setStateAsync("gateway.apiVersion", asStateValue(res.version), true);

		if (info.certificates) {
			await this.setStateAsync("gateway.certificatesJson", JSON.stringify(info.certificates), true);
		}
	}

	/**
	 * Methode 04: log (inkl. Filter für Event 16001 = Marktteilnehmer-Versand)
	 */
	private async pollLogs(): Promise<void> {
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
			const logTable = entries
				.map(entry => ({
					index: entry.index,
					id: entry["event-id"],
					timestamp: entry.time,
					vendor: entry["vendor-id"],
					message: entry.message,
				}))
				.sort((a, b) => {
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
				if (latestTimestamp === undefined) {
					this.log.warn("Invalid timestamp in the latest log entry; logs.lastTime was not updated.");
				} else {
					await this.setStateAsync("logs.lastTime", latestTimestamp, true);
				}
				await this.setStateAsync("logs.lastEventId", toText(latest["event-id"]), true);
			}

			const historyState = await this.getStateAsync("gateway.marketTransmissions.historyJson");
			let history: Array<{ time: string; message: string }> = [];
			if (historyState && typeof historyState.val === "string") {
				try {
					const parsedHistory: unknown = JSON.parse(historyState.val);
					if (Array.isArray(parsedHistory)) {
						history = parsedHistory.filter(
							(entry): entry is { time: string; message: string } =>
								typeof entry?.time === "string" && typeof entry?.message === "string",
						);
					} else {
						this.log.warn("The grid operator transmission history is not a JSON array; resetting it.");
					}
				} catch (error) {
					this.log.warn(
						`The grid operator transmission history is invalid and will be rebuilt: ${
							error instanceof Error ? error.message : String(error)
						}`,
					);
				}
			}

			const knownTimes = new Set(history.map(entry => entry.time));
			const marketEvents = entries
				.filter(entry => String(entry["event-id"]) === "16001")
				.map(entry => ({ time: toText(entry.time), message: toText(entry.message) }))
				.filter(entry => entry.time.length > 0);
			const newMarketEvents = marketEvents.filter(entry => !knownTimes.has(entry.time));
			history = [...history, ...newMarketEvents].slice(-500);
			const historyWithTimestamps = history
				.map(entry => ({ ...entry, timestamp: new Date(entry.time).getTime() }))
				.filter(entry => Number.isFinite(entry.timestamp))
				.sort((a, b) => a.timestamp - b.timestamp);
			const lastMarket = historyWithTimestamps[historyWithTimestamps.length - 1];
			this.latestMarketEventTimestamp = lastMarket?.timestamp ?? 0;

			if (newMarketEvents.length > 0) {
				const newestNewEvent = newMarketEvents
					.map(entry => ({ ...entry, timestamp: new Date(entry.time).getTime() }))
					.filter(entry => Number.isFinite(entry.timestamp))
					.sort((a, b) => a.timestamp - b.timestamp)
					.at(-1);
				const loggedEvent = newestNewEvent ?? newMarketEvents[newMarketEvents.length - 1];
				this.log.info(`New grid operator transmission (Event 16001) registered at ${loggedEvent.time}`);
			}

			const today = new Date();
			const todayCount = history.filter(entry => {
				const eventDate = new Date(entry.time);
				return (
					Number.isFinite(eventDate.getTime()) &&
					eventDate.getFullYear() === today.getFullYear() &&
					eventDate.getMonth() === today.getMonth() &&
					eventDate.getDate() === today.getDate()
				);
			}).length;

			await this.setStateAsync("gateway.marketTransmissions.totalCount", history.length, true);
			await this.setStateAsync("gateway.marketTransmissions.todayCount", todayCount, true);
			const lastMarketTimestamp = lastMarket ? toTimestamp(lastMarket.time) : 0;
			if (lastMarketTimestamp === undefined) {
				this.log.warn(
					"Invalid timestamp for the latest grid operator transmission; gateway.marketTransmissions.lastTimestamp was not updated.",
				);
			} else {
				await this.setStateAsync("gateway.marketTransmissions.lastTimestamp", lastMarketTimestamp, true);
			}
			await this.setStateAsync("gateway.marketTransmissions.historyJson", JSON.stringify(history), true);
			if (lastMarket) {
				const lastTransmissionTimestamp = toTimestamp(lastMarket.time);
				if (lastTransmissionTimestamp === undefined) {
					this.log.warn(
						"Invalid timestamp for the latest grid operator transmission; gateway.lastMarketTransmission was not updated.",
					);
				} else {
					await this.setStateAsync("gateway.lastMarketTransmission", lastTransmissionTimestamp, true);
				}
			}
		} catch (error: unknown) {
			this.log.debug(`Log request skipped: ${getErrorMessage(error)}`);
		}
	}

	/**
	 * Methode 02 (user-info) & Methode 03 (usage-point-info):
	 * Liest alle verfügbaren Tarife ein und speichert sie im Katalog-Datenpunkt.
	 *
	 * @param forceLog Gibt an, ob der Scan im Info-Log angekündigt werden soll
	 */
	private async discoverUsagePoints(forceLog: boolean): Promise<void> {
		if (forceLog) {
			this.log.info("Scanning the gateway for activated tariffs (user-info)...");
		}

		const res = await this.sendConexaRequest({ method: "user-info" });
		const userInfo = asGatewayRecord(res["user-info"]);
		const rawList = asGatewayRecords(userInfo?.["usage-points"]);

		const discovered: CachedUsagePoint[] = [];

		for (const item of rawList) {
			const upId = item["usage-point-id"];
			const upName = item["usage-point-name"] || upId;
			if (!upId) {
				continue;
			}
			const usagePointId = toText(upId);
			const usagePointName = String(upName);

			// Punkte und Sonderzeichen im Namen durch Unterstriche ersetzen für saubere ioBroker-IDs (z.B. TAF1_1_8_0)
			const cleanId = usagePointName.replace(/[^a-zA-Z0-9_-]/g, "_");

			let database = "origin";
			let obis = "";
			let scaler = -1; // Standard bei CONEXA Wh mit 1 Nachkommastelle
			let unit = "30";

			// Detail-Infos (Database, OBIS & Scaler) über Methode 03 (usage-point-info) abrufen
			try {
				const detailRes = await this.sendConexaRequest({
					method: "usage-point-info",
					"usage-point-id": usagePointId,
				});
				this.log.debug(`[usage-point-info] Raw data for ${usagePointName}: ${JSON.stringify(detailRes)}`);
				const info = asGatewayRecord(detailRes["usage-point-info"]);
				const databases = [
					...asGatewayRecords(info?.databases),
					...asGatewayRecords(detailRes.databases),
				] as GatewayRecord[];
				const sortedDatabases = [
					...databases.filter(entry => String(entry?.database).toLowerCase() === "origin"),
					...databases.filter(entry => String(entry?.database).toLowerCase() !== "origin"),
				];
				const databaseWithChannel = sortedDatabases
					.map(entry => ({
						entry,
						channel: asGatewayRecords(entry.channels).find(
							channel => typeof channel.obis === "string" && channel.obis.length > 0,
						),
					}))
					.find(result => result.channel);
				const directChannels = [...asGatewayRecords(info?.channels), ...asGatewayRecords(detailRes.channels)];
				const channel = databaseWithChannel?.channel ?? directChannels.find(ch => ch?.obis);
				if (databaseWithChannel) {
					database = toText(databaseWithChannel.entry.database, "origin");
				} else if (sortedDatabases[0]?.database) {
					database = toText(sortedDatabases[0].database);
				}
				if (channel) {
					obis = typeof channel.obis === "string" ? channel.obis : "";
					scaler = channel.scaler !== undefined ? Number(channel.scaler) : -1;
					unit = toText(channel.unit, "30");
				}
			} catch (err: unknown) {
				this.log.debug(
					`The usage-point-info detail request for ${usagePointName} did not return a channel: ${getErrorMessage(err)}`,
				);
			}
			if (!obis) {
				const obisMatch = usagePointName.match(/(?:^|[_\s])(\d+\.\d+\.\d+)(?:$|[_\s])/);
				if (obisMatch) {
					obis = obisMatch[1];
				}
			}

			const upEntry: CachedUsagePoint = {
				usagePointId,
				usagePointName,
				cleanId,
				tafNumber: toText(item["taf-number"]),
				tafState: toText(item["taf-state"]),
				meterId: toText(asGatewayRecords(item.meter)[0]?.["meter-id"]),
				meteringPointId: toText(item["metering-point-id"]),
				database,
				obis,
				scaler,
				unit,
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
	private async ensureUsagePointObjects(up: CachedUsagePoint): Promise<void> {
		const basePath = `usagePoints.${up.cleanId}`;

		await this.setObjectNotExistsAsync(basePath, {
			type: "channel",
			common: {
				name: {
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
					"zh-cn": `${up.usagePointName} (TAF ${up.tafNumber})`,
				},
			},
			native: {},
		});

		await this.setObjectNotExistsAsync(`${basePath}.usagePointId`, {
			type: "state",
			common: { name: "Usage Point ID", type: "string", role: "text", read: true, write: false, def: "" },
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.meterId`, {
			type: "state",
			common: {
				name: {
					en: "Meter ID (meter number)",
					de: "Zähler-ID (Zählernummer)",
					ru: "ID счетчика (номер счетчика)",
					pt: "ID do contador (número do contador)",
					nl: "Meter-ID (meternummer)",
					fr: "ID du compteur (numéro du compteur)",
					it: "ID contatore (numero contatore)",
					es: "ID del contador (número de contador)",
					pl: "ID licznika (numer licznika)",
					uk: "Ідентифікатор лічильника (номер лічильника)",
					"zh-cn": "电表 ID（电表编号）",
				},
				type: "string",
				role: "info.serial",
				read: true,
				write: false,
				def: "",
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.tafState`, {
			type: "state",
			common: {
				name: {
					en: "TAF status",
					de: "TAF-Status",
					ru: "Статус TAF",
					pt: "Estado TAF",
					nl: "TAF-status",
					fr: "Statut TAF",
					it: "Stato TAF",
					es: "Estado TAF",
					pl: "Status TAF",
					uk: "Статус TAF",
					"zh-cn": "TAF 状态",
				},
				type: "string",
				role: "info.status",
				read: true,
				write: false,
				def: "",
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.obis`, {
			type: "state",
			common: {
				name: {
					en: "OBIS code",
					de: "OBIS-Kennzahl",
					ru: "Код OBIS",
					pt: "Código OBIS",
					nl: "OBIS-code",
					fr: "Code OBIS",
					it: "Codice OBIS",
					es: "Código OBIS",
					pl: "Kod OBIS",
					uk: "Код OBIS",
					"zh-cn": "OBIS 代码",
				},
				type: "string",
				role: "text",
				read: true,
				write: false,
				def: "",
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.valueRaw`, {
			type: "state",
			common: {
				name: {
					en: "Raw meter value",
					de: "Rohwert vom Zähler",
					ru: "Необработанное значение счетчика",
					pt: "Valor bruto do contador",
					nl: "Ruwe meterwaarde",
					fr: "Valeur brute du compteur",
					it: "Valore grezzo del contatore",
					es: "Valor bruto del contador",
					pl: "Surowa wartość licznika",
					uk: "Необроблене значення лічильника",
					"zh-cn": "电表原始值",
				},
				type: "number",
				role: "value",
				read: true,
				write: false,
				def: 0,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.valueWh`, {
			type: "state",
			common: {
				name: {
					en: "Meter reading (Wh)",
					de: "Zählerstand (Wh)",
					ru: "Показание счетчика (Вт·ч)",
					pt: "Leitura do contador (Wh)",
					nl: "Meterstand (Wh)",
					fr: "Index du compteur (Wh)",
					it: "Lettura del contatore (Wh)",
					es: "Lectura del contador (Wh)",
					pl: "Stan licznika (Wh)",
					uk: "Показання лічильника (Вт·год)",
					"zh-cn": "电表读数 (Wh)",
				},
				type: "number",
				role: "value.energy",
				unit: "Wh",
				read: true,
				write: false,
				def: 0,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.valueKWh`, {
			type: "state",
			common: {
				name: {
					en: "Meter reading (kWh)",
					de: "Zählerstand (kWh)",
					ru: "Показание счетчика (кВт·ч)",
					pt: "Leitura do contador (kWh)",
					nl: "Meterstand (kWh)",
					fr: "Index du compteur (kWh)",
					it: "Lettura del contatore (kWh)",
					es: "Lectura del contador (kWh)",
					pl: "Stan licznika (kWh)",
					uk: "Показання лічильника (кВт·год)",
					"zh-cn": "电表读数 (kWh)",
				},
				type: "number",
				role: "value.energy",
				unit: "kWh",
				read: true,
				write: false,
				def: 0,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.deltaWh`, {
			type: "state",
			common: {
				name: {
					en: "Consumption last interval (Wh)",
					de: "Verbrauch letztes Intervall (Wh)",
					ru: "Расход за последний интервал (Вт·ч)",
					pt: "Consumo no último intervalo (Wh)",
					nl: "Verbruik laatste interval (Wh)",
					fr: "Consommation du dernier intervalle (Wh)",
					it: "Consumo dell'ultimo intervallo (Wh)",
					es: "Consumo del último intervalo (Wh)",
					pl: "Zużycie w ostatnim przedziale (Wh)",
					uk: "Споживання за останній інтервал (Вт·год)",
					"zh-cn": "上一间隔耗电量 (Wh)",
				},
				type: "number",
				role: "value.energy",
				unit: "Wh",
				read: true,
				write: false,
				def: 0,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.averagePowerW`, {
			type: "state",
			common: {
				name: {
					en: "Average power last interval (W)",
					de: "Durchschnittsleistung letztes Intervall (W)",
					ru: "Средняя мощность за последний интервал (Вт)",
					pt: "Potência média no último intervalo (W)",
					nl: "Gemiddeld vermogen laatste interval (W)",
					fr: "Puissance moyenne du dernier intervalle (W)",
					it: "Potenza media dell'ultimo intervallo (W)",
					es: "Potencia media del último intervalo (W)",
					pl: "Średnia moc w ostatnim przedziale (W)",
					uk: "Середня потужність за останній інтервал (Вт)",
					"zh-cn": "上一间隔平均功率 (W)",
				},
				type: "number",
				role: "value.power",
				unit: "W",
				read: true,
				write: false,
				def: 0,
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.captureTime`, {
			type: "state",
			common: {
				name: {
					en: "Capture timestamp",
					de: "Zeitpunkt der Erfassung",
					ru: "Время снятия показаний",
					pt: "Data e hora da captura",
					nl: "Tijdstempel van opname",
					fr: "Horodatage de la mesure",
					it: "Data e ora di acquisizione",
					es: "Marca de tiempo de captura",
					pl: "Czas pobrania pomiaru",
					uk: "Час отримання показань",
					"zh-cn": "采集时间戳",
				},
				type: "string",
				role: "date",
				read: true,
				write: false,
				def: "",
			},
			native: {},
		});
		await this.setObjectNotExistsAsync(`${basePath}.historyJson`, {
			type: "state",
			common: {
				name: {
					en: "History / load profile (JSON)",
					de: "Historie / Lastgang (JSON)",
					ru: "История / профиль нагрузки (JSON)",
					pt: "Histórico / curva de carga (JSON)",
					nl: "Geschiedenis / belastingsprofiel (JSON)",
					fr: "Historique / courbe de charge (JSON)",
					it: "Cronologia / profilo di carico (JSON)",
					es: "Historial / perfil de carga (JSON)",
					pl: "Historia / profil obciążenia (JSON)",
					uk: "Історія / профіль навантаження (JSON)",
					"zh-cn": "历史记录 / 负荷曲线 (JSON)",
				},
				type: "string",
				role: "json",
				read: true,
				write: false,
				def: "[]",
			},
			native: {},
		});

		// Stammdaten direkt schreiben
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
	private async pollReadings(fetchFullHistory = false): Promise<boolean> {
		let catalogRefreshNeeded = false;
		let hasNewReading = false;

		for (const [usagePointIndex, up] of this.usagePointsCache.entries()) {
			try {
				let usedFullHistory = fetchFullHistory;
				// Exakter Payload wie in Bruno getestet
				const payload: GatewayRequest = {
					method: "readings",
					"usage-point-id": up.usagePointId,
					database: up.database || "origin",
					"last-reading": !fetchFullHistory,
				};
				if (fetchFullHistory) {
					payload["from-time"] = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 19);
				}

				let res: GatewayRecord;
				/**
				 * [DE] Extrahiert Messwerte aus der Antwort | [EN] Extracts readings from the response
				 * - DE: Durchsucht alternative Channel- und Datenbankstrukturen.
				 * - EN: Searches alternative channel and database structures.
				 *
				 * @param response Gateway-Antwort | Gateway response
				 */
				const extractReadings = (
					response: GatewayRecord,
				): { channel: GatewayRecord | undefined; readings: GatewayRecord[]; hasDataIndicator: boolean } => {
					let foundChannel: GatewayRecord | undefined;
					let foundReadings: GatewayRecord[] = [];
					/**
					 * [DE] Findet Messwerte in Channels | [EN] Finds readings in channels
					 * - DE: Wählt den ersten Channel mit Messwerten aus.
					 * - EN: Selects the first channel containing readings.
					 *
					 * @param channels Zu prüfende Channels | Channels to inspect
					 */
					const findChannel = (channels: unknown[]): boolean => {
						for (const value of channels) {
							const candidate = asGatewayRecord(value);
							const readings = asGatewayRecords(candidate?.readings);
							if (candidate && readings.length > 0) {
								foundChannel = candidate;
								foundReadings = readings;
								return true;
							}
						}
						return false;
					};
					/**
					 * [DE] Durchsucht Datenbank-Channels | [EN] Searches database channels
					 * - DE: Findet Messwerte und aktualisiert die erkannte Datenbank.
					 * - EN: Finds readings and updates the detected database.
					 *
					 * @param databases Zu prüfende Datenbanken | Databases to inspect
					 */
					const findDatabaseChannels = (databases: unknown[]): boolean => {
						for (const value of databases) {
							const database = asGatewayRecord(value);
							if (findChannel(asGatewayArray(database?.channels))) {
								if (database?.database) {
									up.database = toText(database.database);
								}
								return true;
							}
						}
						return false;
					};
					const responseReadings = asGatewayRecord(response.readings);
					if (!findChannel(asGatewayArray(responseReadings?.channels))) {
						if (
							!findDatabaseChannels(asGatewayArray(responseReadings?.databases)) &&
							!findChannel(asGatewayArray(response.channels))
						) {
							findDatabaseChannels(asGatewayArray(response.databases));
						}
					}
					if (foundReadings.length === 0 && Array.isArray(response.readings)) {
						foundReadings = asGatewayRecords(response.readings);
					}
					if (
						foundReadings.length === 0 &&
						responseReadings?.reading &&
						isGatewayRecord(responseReadings.reading)
					) {
						foundReadings = [responseReadings.reading];
					}
					if (foundReadings.length === 0 && isGatewayRecord(response.reading)) {
						foundReadings = [response.reading];
					}
					const recordsValue = responseReadings?.records ?? response?.records;
					const hasRecords =
						recordsValue !== undefined && recordsValue !== null && toText(recordsValue) !== "0";
					const hasChannels =
						asGatewayArray(responseReadings?.channels).length > 0 ||
						asGatewayArray(response.channels).length > 0 ||
						asGatewayArray(responseReadings?.databases).some(
							database => asGatewayArray(asGatewayRecord(database)?.channels).length > 0,
						) ||
						asGatewayArray(response.databases).some(
							database => asGatewayArray(asGatewayRecord(database)?.channels).length > 0,
						);
					return {
						channel: foundChannel,
						readings: foundReadings,
						hasDataIndicator: hasRecords || hasChannels,
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
						`Full history for tariff ${up.usagePointName} was rejected with HTTP 400; requesting the latest reading instead.`,
					);
					usedFullHistory = false;
					delete payload["from-time"];
					payload["last-reading"] = true;
					try {
						this.log.debug(
							`[readings] Sending payload for ${up.usagePointName}: ${JSON.stringify(payload)}`,
						);
						res = await this.sendConexaRequest(payload);
					} catch (fallbackError) {
						if (
							!/\b400\b/.test(
								fallbackError instanceof Error ? fallbackError.message : String(fallbackError),
							)
						) {
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
					const fallbackPayloads: GatewayRequest[] = [
						{
							method: "readings",
							"usage-point-id": up.usagePointId,
							database: up.database || "origin",
							"from-time": "2020-01-01T00:00:00Z",
							"to-time": new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
						},
						{
							method: "readings",
							"usage-point-id": up.usagePointId,
							database: up.database || "origin",
							"last-reading": "true",
						},
					];
					if (up.obis) {
						fallbackPayloads.push({
							method: "readings",
							"usage-point-id": up.usagePointId,
							database: up.database || "origin",
							"last-reading": true,
							channels: [{ obis: up.obis }],
						});
					}

					for (const fallbackPayload of fallbackPayloads) {
						this.log.debug(
							`[readings] Sending payload for ${up.usagePointName}: ${JSON.stringify(fallbackPayload)}`,
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
								`[readings] Fallback for ${up.usagePointName} failed: ${
									error instanceof Error ? error.message : String(error)
								}`,
							);
						}
					}
				}

				if (readingsList.length === 0) {
					if (!shouldLogRaw) {
						this.log.debug(`[readings] Raw data for ${up.usagePointName}: ${JSON.stringify(res)}`);
					}
					this.log.warn(
						`No readings found in the readings response for ${up.usagePointName}. JSON structure: ${JSON.stringify(res).slice(0, 500)}`,
					);
					continue;
				}

				const basePath = `usagePoints.${up.cleanId}`;
				if (typeof channel?.obis === "string" && channel.obis && !up.obis) {
					await this.setStateAsync(`${basePath}.obis`, channel.obis, true);
				}

				// Vorherigen Zustand aus ioBroker lesen (für Delta-Berechnung bei last-reading: true)
				const prevWhState = await this.getStateAsync(`${basePath}.valueWh`);
				const prevTimeState = await this.getStateAsync(`${basePath}.captureTime`);

				// Aktuellsten Messwert bestimmen
				/**
				 * [DE] Konvertiert Messzeitpunkte | [EN] Converts reading timestamps
				 * - DE: Unterstützt ISO-Zeitstempel sowie Sekunden- und Millisekundenwerte.
				 * - EN: Supports ISO timestamps and values in seconds or milliseconds.
				 *
				 * @param time Messzeitpunkt | Reading timestamp
				 */
				const parseReadingTimestamp = (time: unknown): number => {
					if (typeof time === "number" && Number.isFinite(time)) {
						return time < 1_000_000_000_000 ? time * 1000 : time;
					}
					if (typeof time !== "string") {
						return Number.NaN;
					}
					const value = time.trim();
					if (/^\d{10,13}$/.test(value)) {
						const numericTime = Number(value);
						return value.length <= 10 ? numericTime * 1000 : numericTime;
					}
					return Date.parse(value);
				};
				for (const reading of readingsList) {
					const readingTime =
						reading["capture-time"] ?? reading["target-time"] ?? reading.time ?? reading.timestamp ?? "";
					const readingTimestamp = parseReadingTimestamp(readingTime);
					if (Number.isFinite(readingTimestamp)) {
						this.latestReadingTimestamp = Math.max(this.latestReadingTimestamp, readingTimestamp);
					}
				}
				const latest = readingsList[readingsList.length - 1];
				const rawValue = latest.value ?? latest.val ?? latest["reading-value"];
				const rawVal = typeof rawValue === "number" ? rawValue : Number(toText(rawValue).trim());
				const captureTime =
					latest["capture-time"] ?? latest["target-time"] ?? latest.time ?? latest.timestamp ?? "";
				const newCaptureTime = toText(captureTime);
				if (newCaptureTime) {
					if (!prevTimeState?.val || String(prevTimeState.val) !== newCaptureTime) {
						hasNewReading = true;
					}
				}

				if (!isNaN(rawVal)) {
					const factor = Math.pow(10, up.scaler); // Bei scaler -1 => * 0.1
					const valWh = Math.round(rawVal * factor * 100) / 100;
					const valKWh = Math.round((valWh / 1000) * 10000) / 10000;

					// 1. Fall: Mehrere Werte in der Antwort (z.B. beim Vollabruf 05.1)
					if (readingsList.length >= 2) {
						const prev = readingsList[readingsList.length - 2];
						const prevValue = prev.value ?? prev.val ?? prev["reading-value"];
						const prevRaw = typeof prevValue === "number" ? prevValue : Number(toText(prevValue).trim());
						if (!isNaN(prevRaw) && rawVal >= prevRaw) {
							const deltaWh = Math.round((rawVal - prevRaw) * factor * 100) / 100;
							const avgPowerW = Math.round(deltaWh * 4 * 100) / 100;
							await this.setStateAsync(`${basePath}.deltaWh`, deltaWh, true);
							await this.setStateAsync(`${basePath}.averagePowerW`, avgPowerW, true);
						}
					} else if (
						prevWhState &&
						typeof prevWhState.val === "number" &&
						prevWhState.val > 0 &&
						prevTimeState?.val &&
						prevTimeState.val !== newCaptureTime &&
						valWh >= prevWhState.val
					) {
						const deltaWh = Math.round((valWh - prevWhState.val) * 100) / 100;
						const timeDiffHours =
							(new Date(newCaptureTime).getTime() - new Date(String(prevTimeState.val)).getTime()) /
							(1000 * 60 * 60);
						const avgPowerW =
							timeDiffHours > 0
								? Math.round((deltaWh / timeDiffHours) * 100) / 100
								: Math.round(deltaWh * 4 * 100) / 100;

						await this.setStateAsync(`${basePath}.deltaWh`, deltaWh, true);
						await this.setStateAsync(`${basePath}.averagePowerW`, avgPowerW, true);
					}

					await this.setStateAsync(`${basePath}.valueRaw`, rawVal, true);
					await this.setStateAsync(`${basePath}.valueWh`, valWh, true);
					await this.setStateAsync(`${basePath}.valueKWh`, valKWh, true);
					await this.setStateAsync(`${basePath}.captureTime`, newCaptureTime, true);
				}

				// Wenn die volle Historie geladen wurde, JSON-State aktualisieren
				if (usedFullHistory || readingsList.length > 1) {
					await this.setStateAsync(`${basePath}.historyJson`, JSON.stringify(readingsList), true);
				}
			} catch (error: unknown) {
				const isBadRequest = /\b400\b/.test(error instanceof Error ? error.message : String(error));
				this.log.warn(
					`Retrieval for tariff ${up.usagePointName} failed (${getErrorMessage(error)})${
						isBadRequest ? "." : ". Marking the catalog for refresh."
					}`,
				);
				if (!isBadRequest) {
					catalogRefreshNeeded = true;
				}
			}
		}

		// Fallback: Wenn ein Tarif nicht mehr existiert oder einen Fehler warf -> Katalog einmalig neu vom Gateway holen
		if (catalogRefreshNeeded) {
			try {
				await this.discoverUsagePoints(true);
			} catch (err: unknown) {
				this.log.error(`Automatic catalog refresh failed: ${getErrorMessage(err)}`);
			}
		}
		return hasNewReading;
	}

	/**
	 * Methode 06: self-test
	 */
	private async triggerSelfTest(): Promise<void> {
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
		} catch (error: unknown) {
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
	private onUnload(callback: () => void): void {
		try {
			if (this.pollTimer) {
				this.clearTimeout(this.pollTimer);
				this.pollTimer = undefined;
			}
			this.httpsAgent.destroy();
		} catch (error: unknown) {
			this.log.error(`Error cleaning up the adapter: ${getErrorMessage(error)}`);
		} finally {
			callback();
		}
	}
}

if (require.main !== module) {
	/**
	 * [DE] Erstellt die Adapter-Instanz | [EN] Creates the adapter instance
	 * - DE: Stellt die Factory für den ioBroker-Adapter bereit.
	 * - EN: Exposes the factory for the ioBroker adapter.
	 *
	 * @param options Adapter-Optionen | Adapter options
	 */
	module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new Conexa(options);
} else {
	(() => new Conexa())();
}
