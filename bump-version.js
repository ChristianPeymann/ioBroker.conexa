const fs = require("fs");
const path = require("path");

function incrementVersion(version) {
	const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
	if (!match) {
		throw new Error(`Ungültige Versionsnummer "${version}". Erwartet wird MAJOR.MINOR.PATCH.`);
	}

	const patch = Number(match[3]);
	if (!Number.isSafeInteger(patch) || patch === Number.MAX_SAFE_INTEGER) {
		throw new Error(`PATCH-Version kann nicht sicher erhöht werden: "${version}".`);
	}

	return `${match[1]}.${match[2]}.${patch + 1}`;
}

function replaceVersion(source, propertyPattern, version, filePath) {
	const match = propertyPattern.exec(source);
	if (!match) {
		throw new Error(`Versionseintrag in ${filePath} wurde nicht gefunden.`);
	}
	return `${source.slice(0, match.index)}${match[1]}${JSON.stringify(version)}${source.slice(match.index + match[0].length)}`;
}

const pkgPath = path.join(__dirname, "package.json");
const ioPkgPath = path.join(__dirname, "io-package.json");

const pkgSource = fs.readFileSync(pkgPath, "utf8");
const pkg = JSON.parse(pkgSource);
if (typeof pkg.version !== "string") {
	throw new Error("package.json enthält keine gültige Versionsnummer.");
}
const newVersion = incrementVersion(pkg.version);
const newPkgSource = replaceVersion(pkgSource, /^(\s*"version"\s*:\s*)"(?:[^"\\]|\\.)*"/m, newVersion, "package.json");

let newIoPkgSource;
if (fs.existsSync(ioPkgPath)) {
	const ioPkgSource = fs.readFileSync(ioPkgPath, "utf8");
	const ioPkg = JSON.parse(ioPkgSource);
	if (!ioPkg.common || ioPkg.common.version !== pkg.version) {
		throw new Error("Die Version in io-package.json stimmt nicht mit package.json überein.");
	}

	const commonIndex = ioPkgSource.search(/"common"\s*:\s*\{/);
	if (commonIndex === -1) {
		throw new Error('Der Abschnitt "common" wurde in io-package.json nicht gefunden.');
	}
	const commonSource = ioPkgSource.slice(commonIndex);
	const updatedCommonSource = replaceVersion(
		commonSource,
		/^(\s*"version"\s*:\s*)"(?:[^"\\]|\\.)*"/m,
		newVersion,
		"io-package.json",
	);
	newIoPkgSource = ioPkgSource.slice(0, commonIndex) + updatedCommonSource;
}

fs.writeFileSync(pkgPath, newPkgSource, "utf8");
if (newIoPkgSource !== undefined) {
	fs.writeFileSync(ioPkgPath, newIoPkgSource, "utf8");
}

console.log(`---> Version automatisch hochgezählt auf: ${newVersion}`);
