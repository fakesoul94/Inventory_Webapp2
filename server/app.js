// ============================================================================
// Inventory API Server
// ============================================================================
// Purpose:
// - Expose REST endpoints used by the dashboard UI.
// - Handle request validation and consistent API error payloads.
// - Delegate data access to dbService to keep route handlers focused.
//
// File map:
// 1) Server bootstrap and middleware
// 2) Shared helpers (validation + error formatting)
// 3) Parts and BOM routes
// 4) Product and buildability routes
// 5) Finished product routes
// 6) Stock movement and stock snapshot routes
// 7) HTTP server startup
// ============================================================================

const express = require('express');
const app = express();
const cors = require('cors');
const dotenv = require('dotenv');
const path = require('path');
const {
  buildCsvBuffer,
  parseCsvObjects
} = require('./csvUtils');
const {
  ValidationError,
  normalizeTextInput,
  normalizeUsername,
  normalizePassword,
  parsePositiveId,
  parsePositiveInteger,
  parsePositiveNumber,
  parseNonNegativeNumber,
  parseNonZeroNumber,
  parseUserRole,
  parseStockMovementType,
  parseAppLogLevel,
  parseIsoDate,
  parseReportingYear,
  normalizeUsages,
  normalizeSearchQuery
} = require('./inputValidation');

dotenv.config({ path: path.join(__dirname, '.env') });
app.set('trust proxy', 1);

const dbService = require('./dbService');
const db = dbService.getDbServiceInstance();
const {
  USER_ROLES,
  SESSION_COOKIE_NAME,
  buildExpiredSessionCookie,
  buildPublicUser,
  buildSessionCookie,
  createPasswordHash,
  createSessionToken,
  getDefaultSeedUsers,
  parseCookies,
  verifyPassword,
  verifySessionToken
} = require('./authService');

// ------------------------------------------------------------------------
// Section 1: Server Bootstrap and Middleware
// ------------------------------------------------------------------------
// CORS stays disabled by default because the browser app is served by this same
// process. Configure explicit cross-origin access only when a separate frontend
// origin needs to call the API.
const configuredCorsOrigins = String(process.env.CORS_ALLOWED_ORIGINS || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

if (configuredCorsOrigins.length > 0) {
  const allowedOriginSet = new Set(configuredCorsOrigins);
  app.use(cors({
    origin(origin, callback) {
      if (!origin || allowedOriginSet.has(origin)) {
        callback(null, true);
        return;
      }

      callback(new Error('Not allowed by CORS'));
    },
    credentials: true
  }));
}

// Parse JSON and URL-encoded request bodies.
app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: false }));

// Serve the frontend files so /client/index.html is directly reachable.
app.use('/client', express.static(path.join(__dirname, '..', 'client')));

// Convenience route: opening the server root loads the client app.
app.get('/', (_request, response) => {
  response.redirect('/client/index.html');
});

// ------------------------------------------------------------------------
// Section 2: Shared Helpers
// ------------------------------------------------------------------------
const APP_LOG_LEVELS = Object.freeze({
  DEBUG: 'debug',
  INFO: 'info',
  WARN: 'warn',
  ERROR: 'error',
  CRITICAL: 'critical'
});
const APP_LOG_LEVEL_OPTIONS = Object.values(APP_LOG_LEVELS);

const getConsoleMethodForLogLevel = (level) => {
  if (level === APP_LOG_LEVELS.CRITICAL || level === APP_LOG_LEVELS.ERROR) {
    return 'error';
  }

  if (level === APP_LOG_LEVELS.WARN) {
    return 'warn';
  }

  if (level === APP_LOG_LEVELS.DEBUG && typeof console.debug === 'function') {
    return 'debug';
  }

  return 'log';
};

const safeSerializeLogDetails = (details) => {
  if (details == null) {
    return null;
  }

  if (typeof details === 'string') {
    return details.slice(0, 8000);
  }

  try {
    return JSON.stringify(details).slice(0, 8000);
  } catch (_error) {
    return String(details).slice(0, 8000);
  }
};

const getClientIpAddress = (request) => {
  const forwarded = String(request?.headers?.['x-forwarded-for'] || '')
    .split(',')[0]
    .trim();

  return forwarded || request.ip || request.socket?.remoteAddress || 'unknown';
};

const writeAppLogEntry = ({
  level = APP_LOG_LEVELS.INFO,
  category = 'system',
  message,
  details = null,
  request = null,
  user = null
}) => {
  const normalizedLevel = APP_LOG_LEVEL_OPTIONS.includes(level) ? level : APP_LOG_LEVELS.INFO;
  const normalizedCategory = String(category || 'system').slice(0, 40);
  const normalizedMessage = String(message || '').trim().slice(0, 255);

  if (!normalizedMessage) {
    return;
  }

  const actor = user || request?.authUserRecord || request?.authUser || null;
  const payload = {
    level: normalizedLevel,
    category: normalizedCategory,
    message: normalizedMessage,
    details_json: safeSerializeLogDetails(details),
    actor_user_id: actor?.user_id == null ? null : Number(actor.user_id),
    actor_username: actor?.username ? String(actor.username).slice(0, 60) : null,
    request_method: request?.method ? String(request.method).slice(0, 10) : null,
    request_path: request?.path ? String(request.path).slice(0, 255) : null,
    ip_address: request ? String(getClientIpAddress(request)).slice(0, 120) : null
  };

  const consoleMethod = getConsoleMethodForLogLevel(normalizedLevel);
  console[consoleMethod](
    `[${normalizedLevel.toUpperCase()}] [${normalizedCategory}] ${normalizedMessage}`,
    payload.details_json || ''
  );

  db.createAppLog(payload)
    .catch((error) => {
      console.error('Failed to persist app log entry:', error?.message || error);
    });
};

// Shared API error responder so all handlers return a predictable shape.
const handleError = (response, err, message = 'Internal server error') => {
  writeAppLogEntry({
    level: APP_LOG_LEVELS.ERROR,
    category: 'system',
    message,
    request: response?.req,
    details: {
      error_name: err?.name || 'Error',
      error_message: err?.message || 'Unknown error',
      stack: err?.stack || null
    }
  });

  response.status(500).json({
    success: false,
    error: message,
    detail: err?.message || 'Unknown error'
  });
};

const handleValidationError = (response, err) => {
  if (err instanceof ValidationError) {
    writeAppLogEntry({
      level: APP_LOG_LEVELS.WARN,
      category: 'validation',
      message: err.message,
      request: response?.req
    });

    return response.status(400).json({
      success: false,
      error: err.message
    });
  }

  throw err;
};

const respondUnauthorized = (response, message = 'Authentication required') => response.status(401).json({
  success: false,
  error: message
});

const respondForbidden = (response, message = 'You do not have permission to perform this action') => response.status(403).json({
  success: false,
  error: message
});

const PASSWORD_CHANGE_REQUIRED_ERROR = 'Password change required before continuing';
const PASSWORD_CHANGE_REQUIRED_PATHS = new Set([
  '/auth/me',
  '/auth/logout',
  '/auth/change-password'
]);
const LOGIN_RATE_LIMIT_WINDOW_MS = Math.max(60_000, Number(process.env.LOGIN_RATE_LIMIT_WINDOW_MS || (1000 * 60 * 15)));
const LOGIN_RATE_LIMIT_MAX_ATTEMPTS = Math.max(1, Number(process.env.LOGIN_RATE_LIMIT_MAX_ATTEMPTS || 5));
const CSV_IMPORT_MAX_BYTES = 5 * 1024 * 1024;
const loginRateLimitStore = new Map();

const shouldUseSecureCookies = (request) => {
  const forceSecure = String(process.env.COOKIE_SECURE || '').trim().toLowerCase() === 'true';
  if (forceSecure) {
    return true;
  }

  const forwardedProto = String(request?.headers?.['x-forwarded-proto'] || '')
    .split(',')[0]
    .trim()
    .toLowerCase();

  return Boolean(request?.secure) || forwardedProto === 'https';
};

const getCookieOptionsForRequest = (request) => ({
  secure: shouldUseSecureCookies(request)
});

const clearSessionCookie = (response, request) => {
  response.setHeader('Set-Cookie', buildExpiredSessionCookie(getCookieOptionsForRequest(request)));
};

const refreshAuthenticatedSession = (response, userRecord, request) => {
  const session = createSessionToken(userRecord);
  response.setHeader('Set-Cookie', buildSessionCookie(session.token, session.expiresAt, getCookieOptionsForRequest(request)));
  return session;
};

const respondWithSession = (response, userRecord, request) => {
  const session = refreshAuthenticatedSession(response, userRecord, request);

  return response.json({
    success: true,
    data: {
      user: buildPublicUser(userRecord),
      session_expires_at: session.expiresAt
    }
  });
};

const getLoginRateLimitKey = (request, username) => `${getClientIpAddress(request)}::${String(username || '').toLowerCase()}`;

const pruneExpiredLoginRateLimits = (now = Date.now()) => {
  loginRateLimitStore.forEach((entry, key) => {
    const windowExpired = now > (entry.window_started_at + LOGIN_RATE_LIMIT_WINDOW_MS);
    const blockExpired = !entry.blocked_until || now >= entry.blocked_until;

    if (windowExpired && blockExpired) {
      loginRateLimitStore.delete(key);
    }
  });
};

const getLoginRateLimitEntry = (key, now = Date.now()) => {
  pruneExpiredLoginRateLimits(now);

  const entry = loginRateLimitStore.get(key);
  if (!entry) {
    return null;
  }

  if (now > (entry.window_started_at + LOGIN_RATE_LIMIT_WINDOW_MS) && (!entry.blocked_until || now >= entry.blocked_until)) {
    loginRateLimitStore.delete(key);
    return null;
  }

  return entry;
};

const getBlockedSecondsRemaining = (entry, now = Date.now()) => {
  const blockedUntil = Number(entry?.blocked_until || 0);
  if (!blockedUntil || blockedUntil <= now) {
    return 0;
  }

  return Math.max(1, Math.ceil((blockedUntil - now) / 1000));
};

const recordFailedLoginAttempt = (key, now = Date.now()) => {
  const existingEntry = getLoginRateLimitEntry(key, now);
  const shouldResetWindow = !existingEntry || now > (existingEntry.window_started_at + LOGIN_RATE_LIMIT_WINDOW_MS);
  const entry = shouldResetWindow
    ? {
      attempts: 0,
      window_started_at: now,
      blocked_until: 0
    }
    : existingEntry;

  entry.attempts += 1;
  if (entry.attempts >= LOGIN_RATE_LIMIT_MAX_ATTEMPTS) {
    entry.blocked_until = now + LOGIN_RATE_LIMIT_WINDOW_MS;
  }

  loginRateLimitStore.set(key, entry);
  return entry;
};

const clearFailedLoginAttempts = (key) => {
  loginRateLimitStore.delete(key);
};

const respondTooManyLoginAttempts = (response, blockedSeconds) => {
  response.setHeader('Retry-After', String(blockedSeconds));
  return response.status(429).json({
    success: false,
    error: `Too many login attempts. Try again in ${blockedSeconds} seconds.`
  });
};

const requireAuthenticatedUser = (request, response, next) => {
  if (!request.authUser) {
    return respondUnauthorized(response);
  }

  return next();
};

const requireRole = (...allowedRoles) => (request, response, next) => {
  if (!request.authUser) {
    return respondUnauthorized(response);
  }

  if (!allowedRoles.includes(request.authUser.role)) {
    writeAppLogEntry({
      level: APP_LOG_LEVELS.WARN,
      category: 'auth',
      message: 'Forbidden request blocked',
      request,
      details: {
        required_roles: allowedRoles,
        current_role: request.authUser.role
      }
    });
    return respondForbidden(response);
  }

  return next();
};

const requireAdmin = requireRole(USER_ROLES.ADMIN);
const canManageProductionScheduleOrder = (requestUser, scheduleOrder) => {
  if (!requestUser || !scheduleOrder) {
    return false;
  }

  if (requestUser.role === USER_ROLES.ADMIN) {
    return true;
  }

  const requestUserId = Number(requestUser.user_id);
  const ownerUserId = Number(scheduleOrder.created_by_user_id);
  return Number.isInteger(requestUserId)
    && requestUserId > 0
    && requestUserId === ownerUserId;
};
const TRANSACTION_MOVEMENT_TYPES = new Set(['IN', 'OUT', 'OUT - Production']);

// PDF output uses a tiny hand-rolled plain-text generator instead of a third-party
// library. These helpers sanitize text so generated documents stay valid even when
// names contain accents, symbols, or characters that need PDF escaping.
const sanitizePdfText = (value) => String(value ?? '')
  .normalize('NFKD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[^\x20-\x7E]/g, '?');

// Escapes PDF control characters inside literal text objects.
const escapePdfText = (value) => sanitizePdfText(value)
  .replace(/\\/g, '\\\\')
  .replace(/\(/g, '\\(')
  .replace(/\)/g, '\\)');

// Keeps exported numeric values compact while still preserving real decimals
// when they matter. Example: 150.0000 -> 150, 5.5000 -> 5.5.
const formatCompactNumber = (value) => {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return '-';
  if (Number.isInteger(numericValue)) return String(numericValue);
  return numericValue.toFixed(3).replace(/\.?0+$/, '');
};

// PDF tables reuse the same compact numeric formatting as CSV exports so the
// numbers stay readable across both output formats.
const formatPdfNumber = (value) => formatCompactNumber(value);

// Pads/truncates text so table-like rows line up when rendered in Courier inside the PDF.
const fitPdfCell = (value, width, alignment = 'left') => {
  const safeValue = sanitizePdfText(value || '-');
  const truncated = safeValue.length > width
    ? `${safeValue.slice(0, Math.max(0, width - 3))}...`
    : safeValue;

  return alignment === 'right'
    ? truncated.padStart(width, ' ')
    : truncated.padEnd(width, ' ');
};

// Splits plain-text content into roughly page-sized chunks for the minimalist PDF builder.
const chunkLines = (lines, size) => {
  const chunks = [];
  for (let index = 0; index < lines.length; index += size) {
    chunks.push(lines.slice(index, index + size));
  }
  return chunks;
};

const sanitizeFilenamePart = (value, fallback = 'report') => {
  const normalized = String(value ?? '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60);

  return normalized || fallback;
};

// Core PDF builder shared by the exported reports.
// It writes a monospace document so the server can generate useful reports without
// bringing in a full document-generation dependency. Layout options allow wide
// reports, such as Transactions, to switch to landscape and tighter typography.
const buildPlainTextPdf = (lines, options = {}) => {
  const {
    pageWidth = 595,
    pageHeight = 842,
    fontSize = 9,
    lineHeight = 12,
    marginX = 36,
    marginTop = 36,
    marginBottom = 36
  } = options;

  const safeLines = Array.isArray(lines) && lines.length > 0 ? lines : [''];
  const linesPerPage = Math.max(1, Math.floor((pageHeight - marginTop - marginBottom) / lineHeight));
  const pageChunks = chunkLines(safeLines, linesPerPage);
  const pageCount = pageChunks.length;
  const fontObjectNumber = 3 + (pageCount * 2);
  const objects = new Map();
  const pageObjectNumbers = [];

  objects.set(1, '<< /Type /Catalog /Pages 2 0 R >>');

  pageChunks.forEach((pageLines, index) => {
    const pageObjectNumber = 3 + (index * 2);
    const contentObjectNumber = pageObjectNumber + 1;
    const streamLines = [
      'BT',
      `/F1 ${fontSize} Tf`,
      `${lineHeight} TL`,
      `${marginX} ${pageHeight - marginTop} Td`
    ];

    pageLines.forEach((line, lineIndex) => {
      if (lineIndex === 0) {
        streamLines.push(`(${escapePdfText(line)}) Tj`);
      } else {
        streamLines.push('T*');
        streamLines.push(`(${escapePdfText(line)}) Tj`);
      }
    });

    streamLines.push('ET');

    const contentStream = streamLines.join('\n');
    pageObjectNumbers.push(pageObjectNumber);
    objects.set(
      pageObjectNumber,
      `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${pageWidth} ${pageHeight}] /Resources << /Font << /F1 ${fontObjectNumber} 0 R >> >> /Contents ${contentObjectNumber} 0 R >>`
    );
    objects.set(
      contentObjectNumber,
      `<< /Length ${Buffer.byteLength(contentStream, 'utf8')} >>\nstream\n${contentStream}\nendstream`
    );
  });

  objects.set(2, `<< /Type /Pages /Kids [${pageObjectNumbers.map((number) => `${number} 0 R`).join(' ')}] /Count ${pageCount} >>`);
  objects.set(fontObjectNumber, '<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>');

  let pdfContent = '%PDF-1.4\n';
  const offsets = new Map();
  const objectNumbers = Array.from(objects.keys()).sort((a, b) => a - b);

  objectNumbers.forEach((objectNumber) => {
    offsets.set(objectNumber, Buffer.byteLength(pdfContent, 'utf8'));
    pdfContent += `${objectNumber} 0 obj\n${objects.get(objectNumber)}\nendobj\n`;
  });

  const xrefOffset = Buffer.byteLength(pdfContent, 'utf8');
  pdfContent += `xref\n0 ${fontObjectNumber + 1}\n`;
  pdfContent += '0000000000 65535 f \n';

  for (let objectNumber = 1; objectNumber <= fontObjectNumber; objectNumber += 1) {
    const offset = String(offsets.get(objectNumber) || 0).padStart(10, '0');
    pdfContent += `${offset} 00000 n \n`;
  }

  pdfContent += `trailer\n<< /Size ${fontObjectNumber + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF`;
  return Buffer.from(pdfContent, 'utf8');
};

// Builds the PDF used by the Production Planner feature.
// The report intentionally includes only missing materials so it is actionable
// as a procurement/shortage document rather than a full BOM export.
const buildProductionPlannerPdf = (planData) => {
  const missingMaterials = Array.isArray(planData?.missing_materials) ? planData.missing_materials : [];
  const generatedAt = new Date().toISOString().replace('T', ' ').slice(0, 16);
  const lines = [
    'Production Planner - Missing Materials',
    '',
    `Product: ${sanitizePdfText(planData?.product_name || '-')}`,
    `Quantity to produce: ${formatPdfNumber(planData?.requested_quantity)}`,
    `Maximum buildable from current stock: ${formatPdfNumber(planData?.max_buildable)}`,
    `Generated at: ${generatedAt}`,
    ''
  ];

  if (missingMaterials.length === 0) {
    lines.push('No missing materials for this production plan.');
  } else {
    const header = [
      fitPdfCell('Part', 34),
      fitPdfCell('Missing Qty', 12, 'right'),
      fitPdfCell('Tricoma Nr', 18),
      fitPdfCell('Reorder Qty', 12, 'right')
    ].join(' | ');

    lines.push(`Missing parts: ${missingMaterials.length}`);
    lines.push('');
    lines.push(header);
    lines.push('-'.repeat(header.length));

    missingMaterials.forEach((row) => {
      lines.push([
        fitPdfCell(row?.part_name || '-', 34),
        fitPdfCell(formatPdfNumber(row?.shortage_quantity), 12, 'right'),
        fitPdfCell(row?.tricoma_nr || '-', 18),
        fitPdfCell(formatPdfNumber(row?.reorder_quantity), 12, 'right')
      ].join(' | '));
    });
  }

  return buildPlainTextPdf(lines);
};

// Builds the PDF used from the Current Stock screen to support reorder decisions.
// This report is based on low-stock rows from the live stock snapshot.
const buildLowStockReorderPdf = (rows) => {
  const lowStockRows = Array.isArray(rows) ? rows : [];
  const generatedAt = new Date().toISOString().replace('T', ' ').slice(0, 16);
  const lines = [
    'Low Stock Reorder List',
    '',
    `Generated at: ${generatedAt}`,
    `Low-stock parts: ${lowStockRows.length}`,
    ''
  ];

  if (lowStockRows.length === 0) {
    lines.push('No low-stock parts at the moment.');
    return buildPlainTextPdf(lines);
  }

  const header = [
    fitPdfCell('Part', 30),
    fitPdfCell('Current', 10, 'right'),
    fitPdfCell('Reorder Lvl', 12, 'right'),
    fitPdfCell('Reorder Qty', 12, 'right'),
    fitPdfCell('Tricoma Nr', 16)
  ].join(' | ');

  lines.push(header);
  lines.push('-'.repeat(header.length));

  lowStockRows.forEach((row) => {
    lines.push([
      fitPdfCell(row?.part_name || '-', 30),
      fitPdfCell(formatPdfNumber(row?.tracked_stock), 10, 'right'),
      fitPdfCell(formatPdfNumber(row?.reorder_level), 12, 'right'),
      fitPdfCell(formatPdfNumber(row?.reorder_quantity), 12, 'right'),
      fitPdfCell(row?.tricoma_nr || '-', 16)
    ].join(' | '));
  });

  return buildPlainTextPdf(lines);
};

const buildCsvCell = (value) => {
  const normalized = String(value ?? '');
  return `"${normalized.replace(/"/g, '""')}"`;
};

const parseCsvTextInput = (value) => {
  const normalized = String(value ?? '');

  if (!normalized.trim()) {
    throw new ValidationError('csv_text is required');
  }

  if (Buffer.byteLength(normalized, 'utf8') > CSV_IMPORT_MAX_BYTES) {
    throw new ValidationError('csv_text must be <= 5 MB');
  }

  return normalized;
};

const parseCsvObjectsInput = (csvText) => {
  try {
    return parseCsvObjects(csvText);
  } catch (error) {
    throw new ValidationError(error?.message || 'Invalid CSV file');
  }
};

const getCsvValue = (row, aliases = []) => {
  const safeRow = row && typeof row === 'object' ? row : {};

  for (const alias of aliases) {
    const value = safeRow?.[alias];
    if (value != null && String(value).trim() !== '') {
      return String(value).trim();
    }
  }

  return '';
};

const normalizeOptionalCsvText = (value, options = {}) => {
  const trimmed = String(value ?? '').trim();
  if (!trimmed) {
    return '';
  }

  return normalizeTextInput(trimmed, options);
};

const wrapCsvRowValidation = (rowIndex, work) => {
  try {
    return work();
  } catch (error) {
    if (error instanceof ValidationError) {
      throw new ValidationError(`CSV row ${rowIndex + 2}: ${error.message}`);
    }

    throw error;
  }
};

const normalizeProductsCsvRows = (rows) => {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new ValidationError('Products CSV must contain at least one data row');
  }

  return rows.map((row, rowIndex) => wrapCsvRowValidation(rowIndex, () => {
    const productIdValue = getCsvValue(row, ['product_id', 'id']);
    const productNameValue = getCsvValue(row, ['product_name', 'name', 'product']);

    return {
      product_id: productIdValue ? parsePositiveInteger(productIdValue, 'product_id') : null,
      product_name: normalizeTextInput(productNameValue, { fieldName: 'product_name', maxLength: 120 })
    };
  }));
};

const normalizePartsCsvRows = (rows) => {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new ValidationError('Parts CSV must contain at least one data row');
  }

  return rows.map((row, rowIndex) => wrapCsvRowValidation(rowIndex, () => {
    const partIdValue = getCsvValue(row, ['part_id', 'id']);
    const reorderLevelValue = getCsvValue(row, ['reorder_level']);
    const reorderQuantityValue = getCsvValue(row, ['reorder_quantity']);

    return {
      part_id: partIdValue ? parsePositiveInteger(partIdValue, 'part_id') : null,
      part_name: normalizeTextInput(getCsvValue(row, ['part_name', 'name', 'part']), {
        fieldName: 'part_name',
        maxLength: 120
      }),
      tricoma_nr: normalizeTextInput(getCsvValue(row, ['tricoma_nr', 'tricoma', 'tricoma_number']), {
        fieldName: 'tricoma_nr',
        maxLength: 80
      }),
      reorder_level: parseNonNegativeNumber(reorderLevelValue, 'reorder_level'),
      reorder_quantity: parsePositiveNumber(reorderQuantityValue, 'reorder_quantity')
    };
  }));
};

const normalizeBomCsvRows = (rows) => {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new ValidationError('BOM CSV must contain at least one data row');
  }

  return rows.map((row, rowIndex) => wrapCsvRowValidation(rowIndex, () => {
    const productIdValue = getCsvValue(row, ['product_id']);
    const partIdValue = getCsvValue(row, ['part_id']);
    const productNameValue = normalizeOptionalCsvText(getCsvValue(row, ['product_name', 'product']), {
      fieldName: 'product_name',
      maxLength: 120
    });
    const partNameValue = normalizeOptionalCsvText(getCsvValue(row, ['part_name', 'part']), {
      fieldName: 'part_name',
      maxLength: 120
    });
    const tricomaValue = normalizeOptionalCsvText(getCsvValue(row, ['tricoma_nr', 'tricoma', 'tricoma_number']), {
      fieldName: 'tricoma_nr',
      maxLength: 80
    });
    const quantityValue = getCsvValue(row, ['quantity_per_product', 'bom_qty', 'qty_per_product', 'quantity', 'qty']);

    if (!productIdValue && !productNameValue) {
      throw new ValidationError('Either product_id or product_name is required');
    }

    if (!partIdValue && !tricomaValue && !partNameValue) {
      throw new ValidationError('Either part_id, tricoma_nr, or part_name is required');
    }

    return {
      product_id: productIdValue ? parsePositiveInteger(productIdValue, 'product_id') : null,
      product_name: productNameValue,
      part_id: partIdValue ? parsePositiveInteger(partIdValue, 'part_id') : null,
      part_name: partNameValue,
      tricoma_nr: tricomaValue,
      quantity_per_product: parsePositiveNumber(quantityValue, 'quantity_per_product')
    };
  }));
};

const formatTransactionTimestamp = (value) => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value ?? '');
  }

  return date.toISOString().replace('T', ' ').slice(0, 19);
};

const buildTransactionsCsv = (rows) => {
  const records = Array.isArray(rows) ? rows : [];
  const lines = [
    [
      'Part Name',
      'Quantity',
      'Type',
      'User',
      'Produced Product',
      'Date',
      'Source'
    ].map(buildCsvCell).join(',')
  ];

  records.forEach((row) => {
    lines.push([
      row?.part_name || '',
      formatPdfNumber(row?.stock_movement_qty),
      row?.movement_type || '',
      row?.created_by || '',
      row?.source_product_name || '',
      formatTransactionTimestamp(row?.stock_movement_date),
      row?.transaction_source || ''
    ].map(buildCsvCell).join(','));
  });

  return Buffer.from(lines.join('\n'), 'utf8');
};

const buildProductsCatalogCsv = (rows) => buildCsvBuffer(
  ['product_id', 'product_name'],
  (Array.isArray(rows) ? rows : []).map((row) => [
    row?.product_id ?? '',
    row?.product_name ?? ''
  ])
);

const buildPartsCatalogCsv = (rows) => buildCsvBuffer(
  ['part_id', 'part_name', 'tricoma_nr', 'reorder_level', 'reorder_quantity'],
  (Array.isArray(rows) ? rows : []).map((row) => [
    row?.part_id ?? '',
    row?.part_name ?? '',
    row?.tricoma_nr ?? '',
    row?.reorder_level == null || row?.reorder_level === ''
      ? ''
      : formatCompactNumber(row.reorder_level),
    row?.reorder_quantity == null || row?.reorder_quantity === ''
      ? ''
      : formatCompactNumber(row.reorder_quantity)
  ])
);

const buildBomCatalogCsv = (rows) => buildCsvBuffer(
  ['product_id', 'product_name', 'part_id', 'part_name', 'tricoma_nr', 'quantity_per_product'],
  (Array.isArray(rows) ? rows : []).map((row) => [
    row?.product_id ?? '',
    row?.product_name ?? '',
    row?.part_id ?? '',
    row?.part_name ?? '',
    row?.tricoma_nr ?? '',
    row?.quantity_per_product ?? ''
  ])
);

const buildTransactionsPdf = (rows, filters = {}) => {
  const records = Array.isArray(rows) ? rows : [];
  const generatedAt = new Date().toISOString().replace('T', ' ').slice(0, 16);
  const activeFilters = [];

  if (filters?.query) activeFilters.push(`Search: ${sanitizePdfText(filters.query)}`);
  if (filters?.movement_type) activeFilters.push(`Type: ${sanitizePdfText(filters.movement_type)}`);
  if (filters?.created_by) activeFilters.push(`User: ${sanitizePdfText(filters.created_by)}`);
  if (filters?.source_product_name) activeFilters.push(`Product: ${sanitizePdfText(filters.source_product_name)}`);
  if (filters?.date_from || filters?.date_to) {
    activeFilters.push(`Date range: ${sanitizePdfText(filters.date_from || '-')} to ${sanitizePdfText(filters.date_to || '-')}`);
  }

  const lines = [
    'Transactions Report',
    '',
    `Generated at: ${generatedAt}`,
    `Rows: ${records.length}`,
    activeFilters.length > 0 ? `Filters: ${activeFilters.join(' | ')}` : 'Filters: none',
    ''
  ];

  if (records.length === 0) {
    lines.push('No transactions match the selected filters.');
    return buildPlainTextPdf(lines);
  }

  const header = [
    fitPdfCell('Part', 32),
    fitPdfCell('Qty', 8, 'right'),
    fitPdfCell('Type', 18),
    fitPdfCell('User', 12),
    fitPdfCell('Product', 24),
    fitPdfCell('Date', 19)
  ].join(' | ');

  lines.push(header);
  lines.push('-'.repeat(header.length));

  records.forEach((row) => {
    lines.push([
      fitPdfCell(row?.part_name || '-', 32),
      fitPdfCell(formatPdfNumber(row?.stock_movement_qty), 8, 'right'),
      fitPdfCell(row?.movement_type || '-', 18),
      fitPdfCell(row?.created_by || '-', 12),
      fitPdfCell(row?.source_product_name || '-', 24),
      fitPdfCell(formatTransactionTimestamp(row?.stock_movement_date), 19)
    ].join(' | '));
  });

  return buildPlainTextPdf(lines, {
    pageWidth: 842,
    pageHeight: 595,
    fontSize: 8,
    lineHeight: 10,
    marginX: 28,
    marginTop: 28,
    marginBottom: 28
  });
};

const normalizeOptionalStockMovementFilterText = (value, fieldName) => normalizeTextInput(value, {
  fieldName,
  allowEmpty: true,
  maxLength: 120,
  collapseWhitespace: true
});

const parseStockMovementFilters = (source = {}) => {
  const query = normalizeOptionalStockMovementFilterText(source.query, 'query');
  const movementType = parseStockMovementType(source.movement_type, { allowEmpty: true });
  const createdBy = normalizeOptionalStockMovementFilterText(source.created_by, 'created_by');
  const sourceProductName = normalizeOptionalStockMovementFilterText(source.source_product_name, 'source_product_name');
  const dateFrom = parseIsoDate(source.date_from, { fieldName: 'date_from', allowEmpty: true });
  const dateTo = parseIsoDate(source.date_to, { fieldName: 'date_to', allowEmpty: true });

  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw new ValidationError('date_from must be before or equal to date_to');
  }

  if (movementType && !TRANSACTION_MOVEMENT_TYPES.has(movementType)) {
    throw new ValidationError('movement_type must be IN, OUT, or OUT - Production');
  }

  return {
    query,
    movement_type: movementType,
    created_by: createdBy,
    source_product_name: sourceProductName,
    date_from: dateFrom,
    date_to: dateTo
  };
};

const parseAnalyticsYear = (value) => parseReportingYear(value, {
  fieldName: 'year',
  allowEmpty: true,
  defaultValue: new Date().getFullYear()
});

const parseAppLogFilters = (source) => {
  const rawQuery = String(source?.query || '').trim();
  const query = rawQuery ? normalizeSearchQuery(rawQuery, 'query') : '';
  const level = parseAppLogLevel(source?.level || '', {
    fieldName: 'level',
    allowEmpty: true
  });
  const dateFrom = parseIsoDate(source?.date_from || '', {
    fieldName: 'date_from',
    allowEmpty: true
  });
  const dateTo = parseIsoDate(source?.date_to || '', {
    fieldName: 'date_to',
    allowEmpty: true
  });

  return {
    query,
    level,
    date_from: dateFrom,
    date_to: dateTo
  };
};

// Initialize performance indexes, auth tables, audit columns, and default users
// before the API starts serving protected requests.
const startupPromise = (async () => {
  await db.ensurePerformanceIndexes();
  await db.ensureAuthSchema();
  await db.ensureAppLogSchema();
  const productionSnapshotResult = await db.ensureProductionSnapshotSchema();
  await db.ensureProductionScheduleSchema();

  if (Number(productionSnapshotResult?.backfilled_runs || 0) > 0) {
    writeAppLogEntry({
      level: APP_LOG_LEVELS.INFO,
      category: 'system',
      message: `Backfilled ${productionSnapshotResult.backfilled_runs} production snapshot run(s).`,
      details: {
        backfilled_runs: Number(productionSnapshotResult.backfilled_runs)
      }
    });
  }

  const hasUsers = await db.hasAnyUsers();
  if (!hasUsers) {
    const seedUsers = getDefaultSeedUsers().map((user) => ({
      ...user,
      password_hash: createPasswordHash(user.password),
      must_change_password: true
    }));
    const seedResult = await db.seedUsersIfEmpty(seedUsers);

    if (seedResult?.created) {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.WARN,
        category: 'auth',
        message: 'Seeded initial admin and powermoon users from environment credentials.',
        details: {
          seeded_users: Number(seedResult.count || 0),
          must_change_password: true
        }
      });
    }
  }

  writeAppLogEntry({
    level: APP_LOG_LEVELS.INFO,
    category: 'system',
    message: 'Server startup tasks completed successfully.'
  });
})().catch((err) => {
  console.error('Failed to initialize server startup tasks:', err);
  throw err;
});

app.use((request, response, next) => {
  startupPromise
    .then(() => next())
    .catch((err) => handleError(response, err, 'Server startup failed'));
});

// Session parsing runs for every API request so route guards can rely on
// request.authUser without each handler re-checking cookies manually.
app.use((request, response, next) => {
  const cookies = parseCookies(request.headers.cookie || '');
  const sessionToken = cookies[SESSION_COOKIE_NAME];

  if (!sessionToken) {
    request.authUser = null;
    request.authUserRecord = null;
    return next();
  }

  const payload = verifySessionToken(sessionToken);
  if (!payload) {
    request.authUser = null;
    request.authUserRecord = null;
    clearSessionCookie(response, request);
    return next();
  }

  return db.getUserById(payload.user_id)
    .then((userRecord) => {
      if (!userRecord || Number(userRecord.session_version) !== Number(payload.session_version)) {
        request.authUser = null;
        request.authUserRecord = null;
        clearSessionCookie(response, request);
        return next();
      }

      request.authUserRecord = userRecord;
      request.authUser = buildPublicUser(userRecord);
      request.authSession = refreshAuthenticatedSession(response, userRecord, request);
      return next();
    })
    .catch((err) => handleError(response, err, 'Failed to verify session'));
});

app.use((request, response, next) => {
  if (!request.authUserRecord || !request.authUserRecord.must_change_password) {
    return next();
  }

  if (PASSWORD_CHANGE_REQUIRED_PATHS.has(request.path)) {
    return next();
  }

  writeAppLogEntry({
    level: APP_LOG_LEVELS.WARN,
    category: 'auth',
    message: PASSWORD_CHANGE_REQUIRED_ERROR,
    request,
    details: {
      user_id: request.authUser?.user_id,
      username: request.authUser?.username
    }
  });

  return response.status(403).json({
    success: false,
    error: PASSWORD_CHANGE_REQUIRED_ERROR
  });
});

// ------------------------------------------------------------------------
// Section 3: Authentication and User Management Routes
// ------------------------------------------------------------------------
app.post('/auth/login', (request, response) => {
  let username;
  let password;
  try {
    username = normalizeUsername(request.body?.username, 'username');
    password = normalizePassword(request.body?.password, { fieldName: 'password' });
  } catch (error) {
    return handleValidationError(response, error);
  }

  const rateLimitKey = getLoginRateLimitKey(request, username);
  const existingRateLimitEntry = getLoginRateLimitEntry(rateLimitKey);
  const blockedSecondsBeforeAttempt = getBlockedSecondsRemaining(existingRateLimitEntry);
  if (blockedSecondsBeforeAttempt > 0) {
    writeAppLogEntry({
      level: APP_LOG_LEVELS.WARN,
      category: 'auth',
      message: 'Login blocked by rate limiting',
      request,
      details: {
        username,
        blocked_seconds: blockedSecondsBeforeAttempt
      }
    });
    return respondTooManyLoginAttempts(response, blockedSecondsBeforeAttempt);
  }

  return db.getUserByUsername(username)
    .then((userRecord) => {
      if (!userRecord || !verifyPassword(password, userRecord.password_hash)) {
        const rateLimitEntry = recordFailedLoginAttempt(rateLimitKey);
        const blockedSecondsAfterAttempt = getBlockedSecondsRemaining(rateLimitEntry);

        writeAppLogEntry({
          level: APP_LOG_LEVELS.WARN,
          category: 'auth',
          message: 'Failed login attempt',
          request,
          details: {
            username,
            attempts: Number(rateLimitEntry?.attempts || 0),
            blocked_seconds: blockedSecondsAfterAttempt
          }
        });

        if (blockedSecondsAfterAttempt > 0) {
          return respondTooManyLoginAttempts(response, blockedSecondsAfterAttempt);
        }

        return response.status(401).json({
          success: false,
          error: 'Invalid username or password'
        });
      }

      clearFailedLoginAttempts(rateLimitKey);
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'auth',
        message: 'User signed in',
        request,
        user: userRecord
      });
      return respondWithSession(response, userRecord, request);
    })
    .catch((err) => handleError(response, err, 'Failed to sign in'));
});

app.post('/auth/logout', (request, response) => {
  if (request.authUser) {
    writeAppLogEntry({
      level: APP_LOG_LEVELS.INFO,
      category: 'auth',
      message: 'User signed out',
      request
    });
  }

  clearSessionCookie(response, request);
  return response.json({ success: true });
});

app.get('/auth/me', requireAuthenticatedUser, (request, response) => response.json({
  success: true,
  data: {
    user: request.authUser
  }
}));

app.put('/auth/change-password', requireAuthenticatedUser, (request, response) => {
  let currentPassword;
  let newPassword;
  try {
    currentPassword = normalizePassword(request.body?.current_password, { fieldName: 'current_password' });
    newPassword = normalizePassword(request.body?.new_password, { fieldName: 'new_password' });
  } catch (error) {
    return handleValidationError(response, error);
  }

  if (currentPassword === newPassword) {
    return response.status(400).json({
      success: false,
      error: 'new_password must be different from current_password'
    });
  }

  return db.getUserById(request.authUser.user_id)
    .then((userRecord) => {
      if (!userRecord || !verifyPassword(currentPassword, userRecord.password_hash)) {
        return response.status(400).json({
          success: false,
          error: 'Current password is incorrect'
        });
      }

      return db.updateUserPassword(userRecord.user_id, createPasswordHash(newPassword))
        .then((result) => {
          if (!result?.user_found || !result.user) {
            return respondUnauthorized(response);
          }

          writeAppLogEntry({
            level: APP_LOG_LEVELS.INFO,
            category: 'auth',
            message: 'Password changed successfully',
            request,
            user: result.user
          });

          return respondWithSession(response, result.user, request);
        });
    })
    .catch((err) => handleError(response, err, 'Failed to change password'));
});

app.get('/users', requireAdmin, (request, response) => {
  db.listUsers()
    .then((data) => response.json({ success: true, data }))
    .catch((err) => handleError(response, err, 'Failed to fetch users'));
});

app.get('/admin/logs', requireAdmin, (request, response) => {
  let filters;
  try {
    filters = parseAppLogFilters(request.query);
  } catch (error) {
    return handleValidationError(response, error);
  }

  return db.listAppLogs(filters)
    .then((data) => response.json({
      success: true,
      data,
      meta: {
        levels: APP_LOG_LEVEL_OPTIONS
      }
    }))
    .catch((err) => handleError(response, err, 'Failed to fetch application logs'));
});

app.post('/users', requireAdmin, (request, response) => {
  let username;
  let displayName;
  let role;
  let password;
  try {
    username = normalizeUsername(request.body?.username, 'username');
    displayName = normalizeTextInput(request.body?.display_name || username, {
      fieldName: 'display_name',
      maxLength: 120
    });
    role = parseUserRole(request.body?.role, 'role');
    password = normalizePassword(request.body?.password, { fieldName: 'password' });
  } catch (error) {
    return handleValidationError(response, error);
  }

  return db.createUser({
    username,
    display_name: displayName,
    role,
    password_hash: createPasswordHash(password),
    must_change_password: true
  })
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'users',
        message: `Created user ${username}`,
        request,
        details: {
          user_id: data?.user_id,
          role
        }
      });

      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to create user'));
});

app.put('/users/:id', requireAdmin, (request, response) => {
  let userId;
  let username;
  let displayName;
  let role;
  let password;
  try {
    userId = parsePositiveId(request.params.id, 'user id');
    username = normalizeUsername(request.body?.username, 'username');
    displayName = normalizeTextInput(request.body?.display_name || username, {
      fieldName: 'display_name',
      maxLength: 120
    });
    role = parseUserRole(request.body?.role, 'role');
    password = normalizePassword(request.body?.password, {
      fieldName: 'password',
      allowEmpty: true
    });
  } catch (error) {
    return handleValidationError(response, error);
  }

  return db.updateUser(userId, {
    username,
    display_name: displayName,
    role,
    password_hash: password ? createPasswordHash(password) : null,
    must_change_password: password
      ? Number(userId) !== Number(request.authUser.user_id)
      : null
  })
    .then((result) => {
      if (!result?.user_found || !result.user) {
        return response.status(404).json({ success: false, error: 'User not found' });
      }

      if (Number(result.user.user_id) === Number(request.authUser.user_id) && password) {
        const session = createSessionToken(result.user);
        response.setHeader('Set-Cookie', buildSessionCookie(session.token, session.expiresAt, getCookieOptionsForRequest(request)));
      }

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'users',
        message: `Updated user ${username}`,
        request,
        details: {
          user_id: Number(result.user.user_id),
          role,
          password_changed: Boolean(password)
        }
      });

      return response.json({
        success: true,
        data: buildPublicUser(result.user)
      });
    })
    .catch((err) => handleError(response, err, 'Failed to update user'));
});

app.delete('/users/:id', requireAdmin, (request, response) => {
  let userId;
  try {
    userId = parsePositiveId(request.params.id, 'user id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  if (Number(userId) === Number(request.authUser.user_id)) {
    return response.status(400).json({
      success: false,
      error: 'You cannot delete the user currently signed in'
    });
  }

  return db.getUserById(userId)
    .then((targetUser) => {
      if (!targetUser) {
        return response.status(404).json({ success: false, error: 'User not found' });
      }

      if (targetUser.role === USER_ROLES.ADMIN) {
        return db.countUsersByRole(USER_ROLES.ADMIN)
          .then((adminCount) => {
            if (adminCount <= 1) {
              return response.status(400).json({
                success: false,
                error: 'At least one admin user must remain'
              });
            }

            return db.deleteUser(userId)
              .then(() => {
                writeAppLogEntry({
                  level: APP_LOG_LEVELS.WARN,
                  category: 'users',
                  message: `Deleted user ${targetUser.username}`,
                  request,
                  details: {
                    user_id: Number(targetUser.user_id),
                    role: targetUser.role
                  }
                });

                return response.json({ success: true });
              });
          });
      }

      return db.deleteUser(userId)
        .then(() => {
          writeAppLogEntry({
            level: APP_LOG_LEVELS.WARN,
            category: 'users',
            message: `Deleted user ${targetUser.username}`,
            request,
            details: {
              user_id: Number(targetUser.user_id),
              role: targetUser.role
            }
          });

          return response.json({ success: true });
        });
    })
    .catch((err) => handleError(response, err, 'Failed to delete user'));
});

// ------------------------------------------------------------------------
// Section 4: Parts and BOM Routes
// ------------------------------------------------------------------------
// These endpoints power admin forms and stock-related part workflows.

// Returns parts list used by movement/admin dropdowns.
app.get('/parts', requireAuthenticatedUser, (request, response) => {
  const result = db.getAllData();

  result
    .then((data) => response.json({ data: data }))
    .catch((err) => handleError(response, err, 'Failed to fetch parts'));
});

// Creates one part and its initial BOM usage mappings.
app.post('/parts', requireAdmin, (request, response) => {
  const {
    part_name,
    tricoma_nr,
    reorder_level,
    reorder_quantity,
    usages
  } = request.body;

  let normalizedPartName;
  let normalizedTricoma;
  let parsedReorderLevel;
  let parsedReorderQuantity;
  let normalizedUsages;
  try {
    normalizedPartName = normalizeTextInput(part_name, { fieldName: 'part_name', maxLength: 120 });
    normalizedTricoma = normalizeTextInput(tricoma_nr, { fieldName: 'tricoma_nr', maxLength: 80 });
    parsedReorderLevel = parseNonNegativeNumber(reorder_level, 'reorder_level');
    parsedReorderQuantity = parsePositiveNumber(reorder_quantity, 'reorder_quantity');
    normalizedUsages = normalizeUsages(usages, { requireNonEmpty: true });
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.insertPartWithUsages({
    part_name: normalizedPartName,
    tricoma_nr: normalizedTricoma,
    reorder_level: parsedReorderLevel,
    reorder_quantity: parsedReorderQuantity,
    usages: normalizedUsages
  });

  result
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: `Created part ${normalizedPartName}`,
        request,
        details: {
          part_id: data?.part_id,
          tricoma_nr: normalizedTricoma,
          usage_count: normalizedUsages.length
        }
      });

      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to create part'));
});

// Deletes one part and dependent rows in related tables.
app.delete('/parts/:id', requireAdmin, (request, response) => {
  let partId;
  try {
    partId = parsePositiveId(request.params.id, 'part id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.deletePartWithRelations(partId);

  result
    .then((data) => {
      if (!data?.part_deleted) {
        return response.status(404).json({ success: false, error: 'Part not found' });
      }
      writeAppLogEntry({
        level: APP_LOG_LEVELS.WARN,
        category: 'inventory',
        message: `Deleted part ${data?.part_name || partId}`,
        request,
        details: {
          part_id: partId
        }
      });
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to delete part'));
});

// Returns editable detail fields for one part.
app.get('/parts/:id/details', requireAdmin, (request, response) => {
  let partId;
  try {
    partId = parsePositiveId(request.params.id, 'part id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getPartDetails(partId);

  result
    .then((data) => {
      if (!data) {
        return response.status(404).json({ success: false, error: 'Part not found' });
      }
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to fetch part details'));
});

// Updates editable detail fields for one part.
app.put('/parts/:id/details', requireAdmin, (request, response) => {
  const {
    part_name,
    tricoma_nr,
    reorder_level,
    reorder_quantity
  } = request.body;

  let partId;
  let normalizedPartName;
  let normalizedTricoma;
  let parsedReorderLevel;
  let parsedReorderQuantity;
  try {
    partId = parsePositiveId(request.params.id, 'part id');
    normalizedPartName = normalizeTextInput(part_name, { fieldName: 'part_name', maxLength: 120 });
    normalizedTricoma = normalizeTextInput(tricoma_nr, { fieldName: 'tricoma_nr', maxLength: 80 });
    parsedReorderLevel = parseNonNegativeNumber(reorder_level, 'reorder_level');
    parsedReorderQuantity = parsePositiveNumber(reorder_quantity, 'reorder_quantity');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.updatePartDetails(partId, {
    part_name: normalizedPartName,
    tricoma_nr: normalizedTricoma,
    reorder_level: parsedReorderLevel,
    reorder_quantity: parsedReorderQuantity
  });

  result
    .then((data) => {
      if (!data?.part_found) {
        return response.status(404).json({ success: false, error: 'Part not found' });
      }
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: `Updated part details for ${normalizedPartName}`,
        request,
        details: {
          part_id: partId,
          tricoma_nr: normalizedTricoma
        }
      });
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to update part details'));
});

// Returns BOM relations for one part.
app.get('/parts/:id/bom-relations', requireAdmin, (request, response) => {
  let partId;
  try {
    partId = parsePositiveId(request.params.id, 'part id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getPartBomRelations(partId);

  result
    .then((data) => {
      if (!data) {
        return response.status(404).json({ success: false, error: 'Part not found' });
      }
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to fetch part bom relations'));
});

// Replaces all BOM relations for one part.
app.put('/parts/:id/bom-relations', requireAdmin, (request, response) => {
  const { usages } = request.body;

  let partId;
  let normalizedUsages;
  try {
    partId = parsePositiveId(request.params.id, 'part id');
    normalizedUsages = normalizeUsages(usages);
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.updatePartBomRelations(partId, normalizedUsages);

  result
    .then((data) => {
      if (!data?.part_found) {
        return response.status(404).json({ success: false, error: 'Part not found' });
      }
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: `Updated BOM relations for part ${partId}`,
        request,
        details: {
          part_id: partId,
          usage_count: normalizedUsages.length
        }
      });
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to update part bom relations'));
});

// ------------------------------------------------------------------------
// Section 4: Products and Buildability Routes
// ------------------------------------------------------------------------
// Returns product list used by multiple dropdowns.
app.get('/products', requireAuthenticatedUser, (request, response) => {
  const result = db.getAllProducts();

  result
    .then((data) => response.json({ data: data }))
    .catch((err) => handleError(response, err, 'Failed to fetch products'));
});

// Creates one product from admin input.
app.post('/products', requireAdmin, (request, response) => {
  const { product_name } = request.body;
  let cleanName;
  try {
    cleanName = normalizeTextInput(product_name, { fieldName: 'product_name', maxLength: 120 });
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.insertProduct(cleanName);

  result
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: `Created product ${cleanName}`,
        request,
        details: {
          product_id: data?.product_id
        }
      });

      return response.json({ success: true, data: data });
    })
    .catch((err) => handleError(response, err, 'Failed to create product'));
});

// Exports the current product catalog for batch editing in CSV form.
app.get('/admin/csv/products/export', requireAdmin, (request, response) => {
  db.getProductsCatalogForCsv()
    .then((rows) => {
      const csvBuffer = buildProductsCatalogCsv(rows);
      const datePart = new Date().toISOString().slice(0, 10);
      const filename = `products-catalog-${sanitizeFilenamePart(datePart, 'today')}.csv`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported products CSV',
        request,
        details: {
          filename,
          row_count: Array.isArray(rows) ? rows.length : 0
        }
      });

      response.setHeader('Content-Type', 'text/csv; charset=utf-8');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(csvBuffer.length));
      return response.send(csvBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export products CSV'));
});

// Imports product rows so admins can create and rename products in bulk.
app.post('/admin/csv/products/import', requireAdmin, (request, response) => {
  let csvText;
  let parsedRows;
  let normalizedRows;
  try {
    csvText = parseCsvTextInput(request.body?.csv_text);
    parsedRows = parseCsvObjectsInput(csvText);
    normalizedRows = normalizeProductsCsvRows(parsedRows);
  } catch (error) {
    return handleValidationError(response, error);
  }

  db.importProductsCatalogRows(normalizedRows)
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: 'Imported products CSV',
        request,
        details: data
      });

      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to import products CSV'));
});

// Exports the full parts catalog, including reorder metadata.
app.get('/admin/csv/parts/export', requireAdmin, (request, response) => {
  db.getPartsCatalogForCsv()
    .then((rows) => {
      const csvBuffer = buildPartsCatalogCsv(rows);
      const datePart = new Date().toISOString().slice(0, 10);
      const filename = `parts-catalog-${sanitizeFilenamePart(datePart, 'today')}.csv`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported parts CSV',
        request,
        details: {
          filename,
          row_count: Array.isArray(rows) ? rows.length : 0
        }
      });

      response.setHeader('Content-Type', 'text/csv; charset=utf-8');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(csvBuffer.length));
      return response.send(csvBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export parts CSV'));
});

// Imports part catalog rows without requiring individual BOM usage mappings.
app.post('/admin/csv/parts/import', requireAdmin, (request, response) => {
  let csvText;
  let parsedRows;
  let normalizedRows;
  try {
    csvText = parseCsvTextInput(request.body?.csv_text);
    parsedRows = parseCsvObjectsInput(csvText);
    normalizedRows = normalizePartsCsvRows(parsedRows);
  } catch (error) {
    return handleValidationError(response, error);
  }

  db.importPartsCatalogRows(normalizedRows)
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: 'Imported parts CSV',
        request,
        details: data
      });

      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to import parts CSV'));
});

// Exports the BOM so product-part relations can be edited in bulk.
app.get('/admin/csv/bom/export', requireAdmin, (request, response) => {
  db.getBomCatalogForCsv()
    .then((rows) => {
      const csvBuffer = buildBomCatalogCsv(rows);
      const datePart = new Date().toISOString().slice(0, 10);
      const filename = `bom-relations-${sanitizeFilenamePart(datePart, 'today')}.csv`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported BOM CSV',
        request,
        details: {
          filename,
          row_count: Array.isArray(rows) ? rows.length : 0
        }
      });

      response.setHeader('Content-Type', 'text/csv; charset=utf-8');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(csvBuffer.length));
      return response.send(csvBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export BOM CSV'));
});

// Imports BOM rows and automatically creates or updates product-part relations.
app.post('/admin/csv/bom/import', requireAdmin, (request, response) => {
  let csvText;
  let parsedRows;
  let normalizedRows;
  try {
    csvText = parseCsvTextInput(request.body?.csv_text);
    parsedRows = parseCsvObjectsInput(csvText);
    normalizedRows = normalizeBomCsvRows(parsedRows);
  } catch (error) {
    return handleValidationError(response, error);
  }

  db.importBomCatalogRows(normalizedRows)
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: 'Imported BOM CSV',
        request,
        details: data
      });

      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to import BOM CSV'));
});

// Returns per-product buildability metrics (max buildable + limiting parts).
app.get('/products/:id/buildability', requireAuthenticatedUser, (request, response) => {
  let productId;
  try {
    productId = parsePositiveId(request.params.id, 'product id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getProductBuildability(productId);

  result
    .then((data) => {
      if (!data) {
        return response.status(404).json({ success: false, error: 'Product not found' });
      }
      writeAppLogEntry({
        level: APP_LOG_LEVELS.DEBUG,
        category: 'production',
        message: 'Fetched product buildability',
        request,
        details: {
          product_id: productId,
          max_buildable: data?.max_buildable
        }
      });
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to fetch product buildability'));
});

// Returns the material plan for a requested product quantity.
app.post('/production-planner/calculate', requireAuthenticatedUser, (request, response) => {
  let parsedProductId;
  let parsedQuantity;
  try {
    parsedProductId = parsePositiveInteger(request.body?.product_id, 'product_id');
    parsedQuantity = parsePositiveNumber(request.body?.quantity, 'quantity');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getProductionPlan(parsedProductId, parsedQuantity);

  result
    .then((data) => {
      if (!data) {
        return response.status(404).json({ success: false, error: 'Product not found' });
      }
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to calculate production plan'));
});

// Generates a PDF report for the missing materials in a production plan.
app.post('/production-planner/export-pdf', requireAuthenticatedUser, (request, response) => {
  let parsedProductId;
  let parsedQuantity;
  try {
    parsedProductId = parsePositiveInteger(request.body?.product_id, 'product_id');
    parsedQuantity = parsePositiveNumber(request.body?.quantity, 'quantity');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getProductionPlan(parsedProductId, parsedQuantity);

  result
    .then((data) => {
      if (!data) {
        return response.status(404).json({ success: false, error: 'Product not found' });
      }

      const pdfBuffer = buildProductionPlannerPdf(data);
      const filename = `missing-materials-${sanitizeFilenamePart(data.product_name, 'product')}-qty-${sanitizeFilenamePart(data.requested_quantity, 'plan')}.pdf`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported production planner PDF',
        request,
        details: {
          product_id: parsedProductId,
          quantity: parsedQuantity,
          filename
        }
      });

      response.setHeader('Content-Type', 'application/pdf');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(pdfBuffer.length));
      return response.send(pdfBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export production planner PDF'));
});

// Returns the shared queue of pending productions and the virtual stock
// allocation that results when earlier due orders reserve material first.
app.get('/production-schedule', requireAuthenticatedUser, (request, response) => {
  db.getProductionScheduleOverview()
    .then((result) => response.json({
      success: true,
      data: result?.orders || [],
      meta: result?.meta || {}
    }))
    .catch((err) => handleError(response, err, 'Failed to fetch production schedule'));
});

// Creates one pending production order for the shared team schedule.
app.post('/production-schedule', requireAuthenticatedUser, (request, response) => {
  let parsedProductId;
  let parsedQuantity;
  let parsedDueDate;
  let parsedComments;
  const today = new Date();
  const todayIso = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`;
  try {
    parsedProductId = parsePositiveInteger(request.body?.product_id, 'product_id');
    parsedQuantity = parsePositiveNumber(request.body?.quantity, 'quantity');
    parsedDueDate = parseIsoDate(request.body?.due_date, {
      fieldName: 'due_date',
      minDate: todayIso
    });
    parsedComments = normalizeTextInput(request.body?.comments || '', {
      fieldName: 'comments',
      maxLength: 500,
      allowEmpty: true
    });
  } catch (error) {
    return handleValidationError(response, error);
  }

  db.insertProductionScheduleOrder(parsedProductId, parsedQuantity, parsedDueDate, parsedComments, request.authUser)
    .then((scheduleOrder) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'production',
        message: 'Created planned production order',
        request,
        details: {
          production_schedule_id: scheduleOrder?.production_schedule_id,
          product_id: parsedProductId,
          quantity: parsedQuantity,
          due_date: parsedDueDate,
          has_comments: Boolean(parsedComments)
        }
      });

      return response.json({
        success: true,
        data: scheduleOrder
      });
    })
    .catch((err) => handleError(response, err, 'Failed to create production schedule order'));
});

// Converts one pending planned production into a real finished-product entry
// while keeping the schedule row in history as completed.
app.post('/production-schedule/:id/complete', requireAuthenticatedUser, async (request, response) => {
  let productionScheduleId;
  try {
    productionScheduleId = parsePositiveId(request.params.id, 'production schedule id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  try {
    const result = await db.completeProductionScheduleOrder(productionScheduleId, request.authUser);
    if (!result?.order_found) {
      return response.status(404).json({
        success: false,
        error: 'Production schedule order not found'
      });
    }

    if (result?.already_completed) {
      return response.status(409).json({
        success: false,
        error: 'This production is already completed.'
      });
    }

    writeAppLogEntry({
      level: APP_LOG_LEVELS.INFO,
      category: 'production',
      message: 'Completed planned production order',
      request,
      details: {
        production_schedule_id: productionScheduleId,
        product_id: result?.order?.product_id,
        quantity: result?.order?.planned_quantity,
        due_date: result?.order?.due_date,
        fin_product_id: result?.finished_product?.fin_product_id
      }
    });

    return response.json({
      success: true,
      data: {
        production_schedule_id: productionScheduleId,
        finished_product: result?.finished_product || null
      }
    });
  } catch (err) {
    return handleError(response, err, 'Failed to complete production schedule order');
  }
});

// Deletes one pending production order. Admin can remove any order, while
// standard users can remove orders they created themselves.
app.delete('/production-schedule/:id', requireAuthenticatedUser, async (request, response) => {
  let productionScheduleId;
  try {
    productionScheduleId = parsePositiveId(request.params.id, 'production schedule id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  try {
    const scheduleOrder = await db.getProductionScheduleOrderById(productionScheduleId);
    if (!scheduleOrder) {
      return response.status(404).json({
        success: false,
        error: 'Production schedule order not found'
      });
    }

    if (String(scheduleOrder.status || '').trim().toLowerCase() === 'completed') {
      return response.status(400).json({
        success: false,
        error: 'Completed productions are kept in history and cannot be removed.'
      });
    }

    if (!canManageProductionScheduleOrder(request.authUser, scheduleOrder)) {
      return respondForbidden(response);
    }

    const result = await db.deleteProductionScheduleOrder(productionScheduleId);
    if (!Number(result?.affectedRows || 0)) {
      return response.status(404).json({
        success: false,
        error: 'Production schedule order not found'
      });
    }

    writeAppLogEntry({
      level: APP_LOG_LEVELS.INFO,
      category: 'production',
      message: 'Deleted planned production order',
      request,
      details: {
        production_schedule_id: productionScheduleId,
        product_id: scheduleOrder.product_id,
        product_name: scheduleOrder.product_name,
        quantity: scheduleOrder.planned_quantity,
        due_date: scheduleOrder.due_date
      }
    });

    return response.json({
      success: true,
      data: {
        production_schedule_id: productionScheduleId
      }
    });
  } catch (err) {
    return handleError(response, err, 'Failed to delete production schedule order');
  }
});

// Deletes one product and dependent rows that reference it.
app.delete('/products/:id', requireAdmin, (request, response) => {
  let productId;
  try {
    productId = parsePositiveId(request.params.id, 'product id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.deleteProductWithRelations(productId);

  result
    .then((data) => {
      if (!data?.product_deleted) {
        return response.status(404).json({ success: false, error: 'Product not found' });
      }
      writeAppLogEntry({
        level: APP_LOG_LEVELS.WARN,
        category: 'inventory',
        message: `Deleted product ${data?.product_name || productId}`,
        request,
        details: {
          product_id: productId
        }
      });
      return response.json({ success: true, data });
    })
    .catch((err) => handleError(response, err, 'Failed to delete product'));
});

// ------------------------------------------------------------------------
// Section 5: Finished Product Routes
// ------------------------------------------------------------------------
// Searches finished-product history for the UI search box.
app.get('/finished-products/search/:query', requireAdmin, (request, response) => {
  let query;
  try {
    query = normalizeSearchQuery(request.params.query, 'query');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.searchFinishedProducts(query);

  result
    .then((data) => response.json({ data: data }))
    .catch((err) => handleError(response, err, 'Failed to search finished products'));
});

// Returns full finished-product history.
app.get('/finished-products', requireAdmin, (request, response) => {
  const result = db.getFinishedProducts();

  result
    .then((data) => response.json({ data: data }))
    .catch((err) => handleError(response, err, 'Failed to fetch finished products'));
});

// ------------------------------------------------------------------------
// Section 6: Stock Movement and Stock Snapshot Routes
// ------------------------------------------------------------------------
// Returns stock movement history, optionally filtered by query-string parameters.
app.get('/stock-movements', requireAuthenticatedUser, (request, response) => {
  let filters;
  try {
    filters = parseStockMovementFilters(request.query);
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = Promise.all([
    db.getStockMovements(filters),
    db.getStockMovementFilterOptions()
  ]);

  result
    .then(([data, filterOptions]) => response.json({
      data,
      meta: {
        filter_options: filterOptions
      }
    }))
    .catch((err) => handleError(response, err, 'Failed to fetch stock movements'));
});

// Backward-compatible search route; now delegates to the same filtered query pipeline.
app.get('/stock-movements/search/:query', requireAuthenticatedUser, (request, response) => {
  let filters;
  try {
    filters = parseStockMovementFilters({
      ...request.query,
      query: normalizeSearchQuery(request.params.query, 'query')
    });
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = Promise.all([
    db.searchStockMovements(filters.query, filters),
    db.getStockMovementFilterOptions()
  ]);

  result
    .then(([data, filterOptions]) => response.json({
      data,
      meta: {
        filter_options: filterOptions
      }
    }))
    .catch((err) => handleError(response, err, 'Failed to search stock movements'));
});

// Exports the filtered transaction view as CSV.
app.get('/stock-movements/export-csv', requireAuthenticatedUser, (request, response) => {
  let filters;
  try {
    filters = parseStockMovementFilters(request.query);
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getStockMovements(filters);

  result
    .then((data) => {
      const csvBuffer = buildTransactionsCsv(data);
      const datePart = new Date().toISOString().slice(0, 10);
      const filename = `transactions-${sanitizeFilenamePart(datePart, 'today')}.csv`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported transactions CSV',
        request,
        details: {
          filename,
          row_count: Array.isArray(data) ? data.length : 0
        }
      });

      response.setHeader('Content-Type', 'text/csv; charset=utf-8');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(csvBuffer.length));
      return response.send(csvBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export transactions CSV'));
});

// Exports the filtered transaction view as a plain-text PDF report.
app.get('/stock-movements/export-pdf', requireAuthenticatedUser, (request, response) => {
  let filters;
  try {
    filters = parseStockMovementFilters(request.query);
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.getStockMovements(filters);

  result
    .then((data) => {
      const pdfBuffer = buildTransactionsPdf(data, filters);
      const datePart = new Date().toISOString().slice(0, 10);
      const filename = `transactions-${sanitizeFilenamePart(datePart, 'today')}.pdf`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported transactions PDF',
        request,
        details: {
          filename,
          row_count: Array.isArray(data) ? data.length : 0
        }
      });

      response.setHeader('Content-Type', 'application/pdf');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(pdfBuffer.length));
      return response.send(pdfBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export transactions PDF'));
});

// Returns aggregated current stock snapshot for the table view.
app.get('/part-stock', requireAuthenticatedUser, (request, response) => {
  const result = db.getPartStock();

  result
    .then((data) => response.json({ data: data }))
    .catch((err) => handleError(response, err, 'Failed to fetch part stock'));
});

// Returns the initial KPI snapshot shown on the landing dashboard.
app.get('/dashboard/kpis', requireAuthenticatedUser, (request, response) => {
  const referenceDate = new Date();
  const year = referenceDate.getFullYear();
  const month = referenceDate.getMonth() + 1;
  const monthLabel = new Intl.DateTimeFormat('en-US', {
    month: 'long',
    year: 'numeric'
  }).format(referenceDate);

  db.getDashboardKpis({ year, month })
    .then((data) => response.json({
      data: {
        ...data,
        year,
        month,
        month_label: monthLabel
      }
    }))
    .catch((err) => handleError(response, err, 'Failed to fetch dashboard KPIs'));
});

// Returns yearly ranked part usage based on frozen production-run snapshots.
app.get('/analytics/most-used-parts', requireAuthenticatedUser, (request, response) => {
  let year;
  try {
    year = parseAnalyticsYear(request.query.year);
  } catch (error) {
    return handleValidationError(response, error);
  }

  db.getMostUsedPartsByYear(year)
    .then((rows) => {
      const totalQuantityUsed = rows.reduce((sum, row) => sum + Number(row.total_quantity_used || 0), 0);
      return response.json({
        data: rows,
        meta: {
          year,
          total_quantity_used: totalQuantityUsed,
          total_distinct_parts: rows.length
        }
      });
    })
    .catch((err) => handleError(response, err, 'Failed to fetch most-used parts'));
});

// Returns yearly ranked finished-product output based on production snapshots.
app.get('/analytics/most-produced-products', requireAuthenticatedUser, (request, response) => {
  let year;
  try {
    year = parseAnalyticsYear(request.query.year);
  } catch (error) {
    return handleValidationError(response, error);
  }

  db.getMostProducedProductsByYear(year)
    .then((rows) => {
      const totalQuantityProduced = rows.reduce((sum, row) => sum + Number(row.total_quantity_produced || 0), 0);
      return response.json({
        data: rows,
        meta: {
          year,
          total_quantity_produced: totalQuantityProduced,
          total_distinct_products: rows.length
        }
      });
    })
    .catch((err) => handleError(response, err, 'Failed to fetch most-produced products'));
});

// Generates a PDF reorder list for the current low-stock parts.
app.get('/part-stock/export-reorder-pdf', requireAuthenticatedUser, (request, response) => {
  const result = db.getPartStock();

  result
    .then((data) => {
      const lowStockRows = (Array.isArray(data) ? data : [])
        .filter((row) => Number(row?.tracked_stock) <= Number(row?.reorder_level))
        .sort((left, right) => String(left?.part_name || '').localeCompare(String(right?.part_name || '')));

      const pdfBuffer = buildLowStockReorderPdf(lowStockRows);
      const datePart = new Date().toISOString().slice(0, 10);
      const filename = `reorder-list-low-stock-${sanitizeFilenamePart(datePart, 'today')}.pdf`;

      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'export',
        message: 'Exported low-stock reorder PDF',
        request,
        details: {
          filename,
          row_count: lowStockRows.length
        }
      });

      response.setHeader('Content-Type', 'application/pdf');
      response.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      response.setHeader('Content-Length', String(pdfBuffer.length));
      return response.send(pdfBuffer);
    })
    .catch((err) => handleError(response, err, 'Failed to export reorder PDF'));
});

// Creates one stock movement entry.
app.post('/stock-movements', requireAuthenticatedUser, (request, response) => {
  const { part_id, quantity } = request.body;

  let parsedPartId;
  let parsedQuantity;
  try {
    parsedPartId = parsePositiveInteger(part_id, 'part_id');
    parsedQuantity = parseNonZeroNumber(quantity, 'quantity');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.insertStockMovement(parsedPartId, parsedQuantity, request.authUser);

  result
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'inventory',
        message: 'Created stock movement',
        request,
        details: {
          part_id: parsedPartId,
          quantity: parsedQuantity
        }
      });
      return response.json({ data: data });
    })
    .catch((err) => handleError(response, err, 'Failed to create stock movement'));
});

// Deletes one stock movement entry.
app.delete('/stock-movements/:id', requireAdmin, (request, response) => {
  let id;
  try {
    id = parsePositiveId(request.params.id, 'stock movement id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.deleteStockMovement(id);

  result
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.WARN,
        category: 'inventory',
        message: 'Deleted stock movement',
        request,
        details: {
          stock_movement_id: id
        }
      });
      return response.json({ success: true, data: data });
    })
    .catch((err) => handleError(response, err, 'Failed to delete stock movement'));
});

// Creates one finished-product entry for completed assembly.
app.post('/finished-products', requireAdmin, (request, response) => {
  const { product_id, quantity } = request.body;

  let parsedProductId;
  let parsedQuantity;
  try {
    parsedProductId = parsePositiveInteger(product_id, 'product_id');
    parsedQuantity = parsePositiveNumber(quantity, 'quantity');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.insertFinishedProduct(parsedProductId, parsedQuantity, request.authUser);

  result
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.INFO,
        category: 'production',
        message: 'Created finished product entry',
        request,
        details: {
          fin_product_id: data?.fin_product_id,
          product_id: parsedProductId,
          quantity: parsedQuantity
        }
      });
      return response.json({ data: data });
    })
    .catch((err) => handleError(response, err, 'Failed to create finished product'));
});

// Deletes one finished-product entry.
app.delete('/finished-products/:id', requireAdmin, (request, response) => {
  let id;
  try {
    id = parsePositiveId(request.params.id, 'finished product id');
  } catch (error) {
    return handleValidationError(response, error);
  }

  const result = db.deleteFinishedProduct(id);

  result
    .then((data) => {
      writeAppLogEntry({
        level: APP_LOG_LEVELS.WARN,
        category: 'production',
        message: 'Deleted finished product entry',
        request,
        details: {
          fin_product_id: id
        }
      });
      return response.json({ success: true, data: data });
    })
    .catch((err) => handleError(response, err, 'Failed to delete finished product'));
});

// ------------------------------------------------------------------------
// Section 7: HTTP Server Startup
// ------------------------------------------------------------------------
// Start the HTTP server on configured port.
// PORT is read from environment and defaults to 5555 for local/dev usage.
const PORT = process.env.PORT || 5555;
app.listen(PORT, '0.0.0.0', () => {
  writeAppLogEntry({
    level: APP_LOG_LEVELS.INFO,
    category: 'system',
    message: `app is running on port ${PORT}`
  });
});
