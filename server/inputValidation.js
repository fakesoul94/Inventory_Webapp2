// ============================================================================
// Shared Input Validation Helpers
// ============================================================================
// Purpose:
// - Keep request validation/sanitization rules in one place.
// - Provide consistent error messages across routes.
// - Normalize text/numeric inputs before route handlers use them.
// ============================================================================

class ValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = 'ValidationError';
    }
}

const CONTROL_CHAR_REGEX = /[\u0000-\u001F\u007F]/;
const ANGLE_BRACKET_REGEX = /[<>]/;
const USERNAME_REGEX = /^[a-z0-9._-]+$/i;
const USER_ROLE_SET = new Set(['admin', 'powermoon']);
const STOCK_MOVEMENT_TYPE_SET = new Set(['IN', 'OUT', 'OUT - Production']);
const APP_LOG_LEVEL_SET = new Set(['debug', 'info', 'warn', 'error', 'critical']);

function getLocalTodayIsoDate() {
    const today = new Date();
    const year = String(today.getFullYear());
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function normalizeWhitespace(value) {
    return String(value ?? '')
        .trim()
        .replace(/\s+/g, ' ');
}

function normalizeTextInput(value, options = {}) {
    const {
        fieldName = 'value',
        maxLength = 120,
        allowEmpty = false,
        collapseWhitespace = true,
        forbidAngleBrackets = true
    } = options;

    const normalized = collapseWhitespace
        ? normalizeWhitespace(value)
        : String(value ?? '').trim();

    if (!allowEmpty && normalized.length === 0) {
        throw new ValidationError(`${fieldName} is required`);
    }

    if (normalized.length > maxLength) {
        throw new ValidationError(`${fieldName} must be <= ${maxLength} characters`);
    }

    if (CONTROL_CHAR_REGEX.test(normalized)) {
        throw new ValidationError(`${fieldName} contains unsupported control characters`);
    }

    if (forbidAngleBrackets && ANGLE_BRACKET_REGEX.test(normalized)) {
        throw new ValidationError(`${fieldName} cannot include angle brackets`);
    }

    return normalized;
}

function parsePositiveId(value, fieldName = 'id') {
    const numericValue = Number(typeof value === 'string' ? value.trim() : value);
    if (!Number.isInteger(numericValue) || numericValue <= 0) {
        throw new ValidationError(`Invalid ${fieldName}`);
    }
    return numericValue;
}

function parsePositiveInteger(value, fieldName = 'value') {
    const numericValue = Number(typeof value === 'string' ? value.trim() : value);
    if (!Number.isInteger(numericValue) || numericValue <= 0) {
        throw new ValidationError(`${fieldName} must be a positive integer`);
    }
    return numericValue;
}

function parsePositiveNumber(value, fieldName = 'value') {
    const numericValue = Number(typeof value === 'string' ? value.trim() : value);
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
        throw new ValidationError(`${fieldName} must be a positive number`);
    }
    return numericValue;
}

function parseNonNegativeNumber(value, fieldName = 'value') {
    const numericValue = Number(typeof value === 'string' ? value.trim() : value);
    if (!Number.isFinite(numericValue) || numericValue < 0) {
        throw new ValidationError(`${fieldName} must be a number >= 0`);
    }
    return numericValue;
}

function parseNonZeroNumber(value, fieldName = 'value') {
    const numericValue = Number(typeof value === 'string' ? value.trim() : value);
    if (!Number.isFinite(numericValue) || numericValue === 0) {
        throw new ValidationError(`${fieldName} must be a non-zero number`);
    }
    return numericValue;
}

function normalizeUsages(usages, options = {}) {
    const { requireNonEmpty = false } = options;

    if (!Array.isArray(usages)) {
        throw new ValidationError('usages must be an array');
    }

    if (requireNonEmpty && usages.length === 0) {
        throw new ValidationError('usages must contain at least one product mapping');
    }

    try {
        return usages.map((usage) => ({
            product_id: parsePositiveInteger(usage?.product_id, 'product_id'),
            quantity_per_product: parsePositiveNumber(usage?.quantity_per_product, 'quantity_per_product')
        }));
    } catch (_error) {
        throw new ValidationError('Each usage must include product_id > 0 and quantity_per_product > 0');
    }
}

function normalizeSearchQuery(value, fieldName = 'query') {
    return normalizeTextInput(value, {
        fieldName,
        maxLength: 120,
        collapseWhitespace: true
    });
}

function normalizeUsername(value, fieldName = 'username') {
    const normalized = String(value ?? '').trim().toLowerCase();

    if (normalized.length < 3 || normalized.length > 40) {
        throw new ValidationError(`${fieldName} must be 3-40 characters`);
    }

    if (!USERNAME_REGEX.test(normalized)) {
        throw new ValidationError(`${fieldName} may only contain letters, numbers, dots, underscores, and hyphens`);
    }

    return normalized;
}

function normalizePassword(value, options = {}) {
    const {
        fieldName = 'password',
        minLength = 8,
        maxLength = 200,
        allowEmpty = false
    } = options;

    const normalized = String(value ?? '');

    if (allowEmpty && normalized.length === 0) {
        return '';
    }

    if (normalized.length < minLength || normalized.length > maxLength) {
        throw new ValidationError(`${fieldName} must be ${minLength}-${maxLength} characters`);
    }

    if (CONTROL_CHAR_REGEX.test(normalized)) {
        throw new ValidationError(`${fieldName} contains unsupported control characters`);
    }

    return normalized;
}

function parseUserRole(value, fieldName = 'role') {
    const normalized = String(value ?? '').trim().toLowerCase();

    if (!USER_ROLE_SET.has(normalized)) {
        throw new ValidationError(`${fieldName} must be admin or powermoon`);
    }

    return normalized;
}

function parseStockMovementType(value, options = {}) {
    const {
        fieldName = 'movement_type',
        allowEmpty = false
    } = options;

    const normalized = normalizeTextInput(value, {
        fieldName,
        maxLength: 40,
        allowEmpty,
        collapseWhitespace: true
    });

    if (allowEmpty && normalized.length === 0) {
        return '';
    }

    if (!STOCK_MOVEMENT_TYPE_SET.has(normalized)) {
        throw new ValidationError(`${fieldName} must be IN, OUT, or OUT - Production`);
    }

    return normalized;
}

function parseAppLogLevel(value, options = {}) {
    const {
        fieldName = 'level',
        allowEmpty = false
    } = options;

    const normalized = normalizeTextInput(value, {
        fieldName,
        maxLength: 16,
        allowEmpty,
        collapseWhitespace: true
    }).toLowerCase();

    if (allowEmpty && normalized.length === 0) {
        return '';
    }

    if (!APP_LOG_LEVEL_SET.has(normalized)) {
        throw new ValidationError(`${fieldName} must be debug, info, warn, error, or critical`);
    }

    return normalized;
}

function parseIsoDate(value, options = {}) {
    const {
        fieldName = 'date',
        allowEmpty = false,
        minDate = ''
    } = options;

    const normalized = normalizeTextInput(value, {
        fieldName,
        maxLength: 10,
        allowEmpty,
        collapseWhitespace: false
    });

    if (allowEmpty && normalized.length === 0) {
        return '';
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
        throw new ValidationError(`${fieldName} must use YYYY-MM-DD format`);
    }

    const parsedDate = new Date(`${normalized}T00:00:00Z`);
    const isValidDate = !Number.isNaN(parsedDate.getTime())
        && parsedDate.toISOString().slice(0, 10) === normalized;

    if (!isValidDate) {
        throw new ValidationError(`${fieldName} must be a valid calendar date`);
    }

    if (minDate && normalized < String(minDate)) {
        throw new ValidationError(`${fieldName} cannot be earlier than ${String(minDate)}`);
    }

    return normalized;
}

function parseReportingYear(value, options = {}) {
    const {
        fieldName = 'year',
        allowEmpty = false,
        minYear = 2000,
        maxYear = 2100,
        defaultValue = new Date().getFullYear()
    } = options;

    const normalized = String(value ?? '').trim();

    if (!normalized) {
        if (allowEmpty) {
            return Number(defaultValue);
        }

        throw new ValidationError(`${fieldName} is required`);
    }

    const numericValue = Number(normalized);
    if (!Number.isInteger(numericValue) || numericValue < minYear || numericValue > maxYear) {
        throw new ValidationError(`${fieldName} must be a whole year between ${minYear} and ${maxYear}`);
    }

    return numericValue;
}

module.exports = {
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
};
