// ============================================================================
// CSV Utilities
// ============================================================================
// Purpose:
// - Parse simple CSV uploads without adding a third-party dependency.
// - Support comma- and semicolon-delimited files exported from spreadsheet apps.
// - Build downloadable CSV buffers with correct escaping.
// ============================================================================

function escapeCsvCell(value) {
    const normalized = String(value ?? '');
    return `"${normalized.replace(/"/g, '""')}"`;
}

function buildCsvBuffer(headers = [], rows = []) {
    const safeHeaders = Array.isArray(headers) ? headers : [];
    const safeRows = Array.isArray(rows) ? rows : [];
    const lines = [];

    if (safeHeaders.length > 0) {
        lines.push(safeHeaders.map(escapeCsvCell).join(','));
    }

    safeRows.forEach((row) => {
        const safeRow = Array.isArray(row) ? row : [];
        lines.push(safeRow.map(escapeCsvCell).join(','));
    });

    return Buffer.from(lines.join('\n'), 'utf8');
}

function normalizeCsvHeader(value) {
    return String(value ?? '')
        .replace(/^\uFEFF/, '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function countDelimiterOccurrences(line, delimiter) {
    let inQuotes = false;
    let count = 0;

    for (let index = 0; index < line.length; index += 1) {
        const char = line[index];

        if (char === '"') {
            if (inQuotes && line[index + 1] === '"') {
                index += 1;
                continue;
            }

            inQuotes = !inQuotes;
            continue;
        }

        if (!inQuotes && char === delimiter) {
            count += 1;
        }
    }

    return count;
}

function detectDelimiter(text) {
    const firstMeaningfulLine = String(text ?? '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .find(Boolean) || '';

    const commaCount = countDelimiterOccurrences(firstMeaningfulLine, ',');
    const semicolonCount = countDelimiterOccurrences(firstMeaningfulLine, ';');

    return semicolonCount > commaCount ? ';' : ',';
}

function parseCsvRows(text, options = {}) {
    const delimiter = options.delimiter || detectDelimiter(text);
    const source = String(text ?? '');
    const rows = [];
    let currentRow = [];
    let currentCell = '';
    let inQuotes = false;

    for (let index = 0; index < source.length; index += 1) {
        const char = source[index];

        if (inQuotes) {
            if (char === '"') {
                if (source[index + 1] === '"') {
                    currentCell += '"';
                    index += 1;
                } else {
                    inQuotes = false;
                }
            } else {
                currentCell += char;
            }
            continue;
        }

        if (char === '"') {
            inQuotes = true;
            continue;
        }

        if (char === delimiter) {
            currentRow.push(currentCell);
            currentCell = '';
            continue;
        }

        if (char === '\n') {
            currentRow.push(currentCell);
            rows.push(currentRow);
            currentRow = [];
            currentCell = '';
            continue;
        }

        if (char === '\r') {
            continue;
        }

        currentCell += char;
    }

    if (inQuotes) {
        throw new Error('CSV contains an unterminated quoted value.');
    }

    if (currentCell.length > 0 || currentRow.length > 0) {
        currentRow.push(currentCell);
        rows.push(currentRow);
    }

    return rows
        .map((row) => row.map((cell) => String(cell ?? '').trim()))
        .filter((row) => row.some((cell) => cell !== ''));
}

function parseCsvObjects(text) {
    const rows = parseCsvRows(text);

    if (rows.length === 0) {
        throw new Error('CSV file is empty.');
    }

    const rawHeaders = rows[0];
    const headers = rawHeaders.map(normalizeCsvHeader);

    if (headers.some((header) => !header)) {
        throw new Error('CSV header row contains an empty column name.');
    }

    const headerSet = new Set(headers);
    if (headerSet.size !== headers.length) {
        throw new Error('CSV header row contains duplicate column names.');
    }

    return rows.slice(1).map((row, rowIndex) => {
        if (row.length > headers.length) {
            throw new Error(`CSV row ${rowIndex + 2} has more columns than the header row.`);
        }

        const object = {};
        headers.forEach((header, headerIndex) => {
            object[header] = String(row[headerIndex] ?? '').trim();
        });
        return object;
    });
}

module.exports = {
    buildCsvBuffer,
    parseCsvObjects
};
