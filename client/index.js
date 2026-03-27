// ============================================================================
// Inventory Dashboard Client Logic
// ============================================================================
// Purpose:
// - Own dashboard UI state (table datasets, sorting, filtering, selections).
// - Coordinate API calls and re-render affected widgets after mutations.
// - Keep table rendering/search behavior centralized and predictable.
//
// File map:
// 01) Shared state and request helpers
// 02) Startup wiring and global event handlers
// 03) Create/update/delete actions (parts, products, movements, finished goods)
// 04) Search and debounce handlers
// 05) Dropdown population helpers
// 06) Table renderers and delegated delete handlers
// 07) Tab switching behavior
// ============================================================================

// Section 01: Shared State and Request Helpers
// These arrays store the most recently fetched data for each table.
// Sort/filter state is tracked separately so re-renders remain deterministic.
let finishedProductsData = [];
let finishedProductsSort = { key: 'fin_product_id', direction: 1 };

let stockMovementsData = [];
let stockMovementsSort = { key: 'stock_movement_date', direction: -1 };
let stockMovementsPage = 1;
let stockMovementFilters = {
    query: '',
    movement_type: '',
    created_by: '',
    source_product_name: '',
    date_from: '',
    date_to: ''
};
let appLogsData = [];
let appLogsPage = 1;
let appLogFilters = {
    query: '',
    level: '',
    date_from: '',
    date_to: ''
};
let appLogLevelOptions = ['debug', 'info', 'warn', 'error', 'critical'];
let stockMovementFilterOptions = {
    movement_types: ['IN', 'OUT', 'OUT - Production'],
    created_by: [],
    source_products: []
};

let partStockData = [];
let partStockSort = { key: 'part_id', direction: 1 };
let partStockFilterLow = true; // Default to low-stock view on app load.
let partStockSearchQuery = '';
let partStockProductId = null;
let partStockPage = 1;
let partStockRequiredPartIds = new Set();
let partStockProductRequirements = [];
let partStockBuildabilityRequestSeq = 0;
let productionPlannerData = [];
let productionPlannerResult = null;
let productionPlannerPage = 1;
// Production Schedule is a pending-only queue. Completed items are created as
// real finished-product records and then disappear from this planning view.
let productionScheduleOrders = [];
let productionSchedulePage = 1;
let productionScheduleAllocationPage = 1;
let selectedProductionScheduleId = null;
let productionScheduleMeta = {
    pending_orders: 0,
    scheduled_units: 0,
    allocated_orders: 0,
    shortage_orders: 0,
    next_due_date: ''
};
let mostUsedPartsData = [];
let mostUsedPartsYear = new Date().getFullYear();
let mostUsedPartsPage = 1;
let mostUsedPartsMeta = {
    year: mostUsedPartsYear,
    total_quantity_used: 0,
    total_distinct_parts: 0
};
let mostProducedProductsData = [];
let mostProducedProductsYear = new Date().getFullYear();
let mostProducedProductsPage = 1;
let mostProducedProductsMeta = {
    year: mostProducedProductsYear,
    total_quantity_produced: 0,
    total_distinct_products: 0
};
let dashboardKpis = {
    year: new Date().getFullYear(),
    month: new Date().getMonth() + 1,
    month_label: new Intl.DateTimeFormat('en-US', {
        month: 'long',
        year: 'numeric'
    }).format(new Date()),
    production_this_month: 0,
    top_used_part: null,
    top_produced_product: null,
    shortage_parts: [],
    pending_productions: []
};
let adminPartUsages = [];
let adminEditPartUsages = [];
let adminUsersData = [];
let adminEditingUserId = null;
let partsCache = null;
let productsCache = null;
let finishedSearchController = null;
let stockMovementSearchController = null;
let appLogSearchController = null;
let currentUser = null;
let activePanelName = 'dashboard';
let authRequestSeq = 0;
let loginInFlight = false;
let sessionIdleTimerId = null;
let sessionLastActivityAt = 0;
let sessionLastBroadcastActivityAt = 0;
let idleLogoutInFlight = false;

const API_BASE_URL = window.location.origin;
const REQUEST_TIMEOUT_MS = 10000;
const SESSION_IDLE_TIMEOUT_MS = 1000 * 60 * 5;
const SESSION_ACTIVITY_BROADCAST_INTERVAL_MS = 15000;
const PART_STOCK_PAGE_SIZE = 10;
const PRODUCTION_SCHEDULE_PAGE_SIZE = 5;
const SESSION_ACTIVITY_STORAGE_KEY = 'powermoon:last-activity-at';
const SESSION_LOGOUT_STORAGE_KEY = 'powermoon:logout-at';
const GRAM_TRACKED_PART_IDS = new Set([143]);
const GRAM_TRACKED_PART_TRICOMA_NUMBERS = new Set(['11859']);
const GRAM_TRACKED_PART_NAME_MATCHES = [
    'hy910 white silicone thermal glue 470g in the 330ml plastic tube'
];
const USER_ROLE_LABELS = {
    admin: 'Administrator',
    powermoon: 'Standard User'
};
const ROLE_PANEL_ACCESS = {
    admin: ['dashboard', 'current-stock', 'finished-products', 'production-planner', 'production-schedule', 'transactions', 'most-used-parts', 'most-produced-products', 'inventory-setup', 'log-book', 'admin'],
    powermoon: ['dashboard', 'current-stock', 'production-planner', 'production-schedule', 'transactions', 'most-used-parts', 'most-produced-products']
};

// Centralized JSON request helper used by all client API calls.
// Adds timeouts, optional abort support, and consistent error extraction.
async function apiRequest(path, options = {}) {
    const {
        method = 'GET',
        body,
        signal,
        timeoutMs = REQUEST_TIMEOUT_MS,
        allowUnauthorized = false
    } = options;

    const timeoutController = new AbortController();
    const timeoutId = setTimeout(() => timeoutController.abort(), timeoutMs);

    // Use the caller signal when provided; timeout still applies independently.
    let aborted = false;
    if (signal) {
        signal.addEventListener('abort', () => {
            aborted = true;
            timeoutController.abort();
        }, { once: true });
    }

    try {
        const response = await fetch(`${API_BASE_URL}${path}`, {
            method,
            headers: body ? { 'Content-type': 'application/json' } : undefined,
            body: body ? JSON.stringify(body) : undefined,
            credentials: 'same-origin',
            signal: timeoutController.signal
        });

        let payload = null;
        try {
            payload = await response.json();
        } catch (_err) {
            payload = null;
        }

        if (!response.ok) {
            if (response.status === 401 && !allowUnauthorized) {
                handleUnauthorizedState();
            }
            throw new Error(payload?.error || payload?.detail || `Request failed (${response.status})`);
        }

        return payload;
    } catch (err) {
        if (aborted || err?.name === 'AbortError') {
            const abortErr = new Error('Request aborted');
            abortErr.name = 'AbortError';
            throw abortErr;
        }
        throw err;
    } finally {
        clearTimeout(timeoutId);
    }
}

async function refreshProductsDropdowns(force = false) {
    if (!force && Array.isArray(productsCache)) {
        loadProductDropdown(productsCache);
        return productsCache;
    }

    const payload = await apiRequest('/products');
    productsCache = Array.isArray(payload?.data) ? payload.data : [];
    loadProductDropdown(productsCache);
    return productsCache;
}

// Refreshes the parts dropdowns and optionally reuses cached values.
async function refreshPartsDropdowns(force = false) {
    if (!force && Array.isArray(partsCache)) {
        loadPartDropdown(partsCache);
        return partsCache;
    }

    const payload = await apiRequest('/parts');
    partsCache = Array.isArray(payload?.data) ? payload.data : [];
    loadPartDropdown(partsCache);
    return partsCache;
}

// Reloads the finished products table from the backend.
async function refreshFinishedProductsTable() {
    const payload = await apiRequest('/finished-products');
    loadFinishedProductsTable(Array.isArray(payload?.data) ? payload.data : []);
}

// Reloads the stock movement history from the backend.
function buildStockMovementQueryString(filters = stockMovementFilters) {
    const params = new URLSearchParams();

    Object.entries(filters || {}).forEach(([key, value]) => {
        const normalized = String(value ?? '').trim();
        if (normalized) {
            params.set(key, normalized);
        }
    });

    return params.toString();
}

async function refreshStockMovementsTable(options = {}) {
    const { signal } = options;
    const queryString = buildStockMovementQueryString();
    const endpoint = queryString ? `/stock-movements?${queryString}` : '/stock-movements';
    const payload = await apiRequest(endpoint, { signal });
    setStockMovementFilterOptions(payload?.meta?.filter_options);
    loadStockMovementTable(Array.isArray(payload?.data) ? payload.data : []);
}

// Reloads the current part stock snapshot from the backend.
async function refreshPartStockTable() {
    const payload = await apiRequest('/part-stock');
    loadPartStockTable(Array.isArray(payload?.data) ? payload.data : []);
}

async function refreshProductionSchedule() {
    const payload = await apiRequest('/production-schedule');
    const responseData = Array.isArray(payload?.data) ? payload.data : [];
    const previousSelectedId = Number(selectedProductionScheduleId);

    productionScheduleOrders = responseData.map((row) => ({
        production_schedule_id: Number(row?.production_schedule_id),
        product_id: Number(row?.product_id),
        product_name: String(row?.product_name || '').trim(),
        planned_quantity: Number(row?.planned_quantity ?? 0),
        due_date: String(row?.due_date || '').trim(),
        comments: String(row?.comments || '').trim(),
        schedule_status: String(row?.schedule_status || row?.status || 'pending').trim().toLowerCase(),
        created_at: row?.created_at || '',
        created_by_user_id: row?.created_by_user_id == null ? null : Number(row.created_by_user_id),
        created_by: String(row?.created_by || '').trim(),
        completed_at: row?.completed_at || '',
        completed_by_user_id: row?.completed_by_user_id == null ? null : Number(row.completed_by_user_id),
        completed_by: String(row?.completed_by || '').trim(),
        fin_product_id: row?.fin_product_id == null ? null : Number(row.fin_product_id),
        total_materials: Number(row?.total_materials ?? 0),
        missing_parts_count: Number(row?.missing_parts_count ?? 0),
        total_shortage_quantity: Number(row?.total_shortage_quantity ?? 0),
        can_allocate_fully: Boolean(row?.can_allocate_fully),
        allocation_status: String(row?.allocation_status || '').trim(),
        allocation_materials: Array.isArray(row?.allocation_materials)
            ? row.allocation_materials.map((material) => ({
                part_id: Number(material?.part_id),
                part_name: getPartDisplayName(material),
                tricoma_nr: String(material?.tricoma_nr || '').trim(),
                quantity_per_product: Number(material?.quantity_per_product ?? 0),
                required_quantity: Number(material?.required_quantity ?? 0),
                available_before_order: Number(material?.available_before_order ?? 0),
                reserved_quantity: Number(material?.reserved_quantity ?? 0),
                shortage_quantity: Number(material?.shortage_quantity ?? 0),
                remaining_after_reservation: Number(material?.remaining_after_reservation ?? 0),
                has_enough_reserved: Boolean(material?.has_enough_reserved)
            }))
            : []
    }));

    productionScheduleMeta = {
        pending_orders: Number(payload?.meta?.pending_orders || 0),
        scheduled_units: Number(payload?.meta?.scheduled_units || 0),
        allocated_orders: Number(payload?.meta?.allocated_orders || 0),
        shortage_orders: Number(payload?.meta?.shortage_orders || 0),
        next_due_date: String(payload?.meta?.next_due_date || '').trim()
    };

    // Preserve the current selection only while it still points to a pending
    // row. Completed schedule items no longer stay visible in this tab.
    const hasPreviousSelection = getPendingProductionScheduleOrders().some((row) => row.production_schedule_id === previousSelectedId);
    if (hasPreviousSelection) {
        selectedProductionScheduleId = previousSelectedId;
    } else {
        const firstPendingOrder = getPendingProductionScheduleOrders()[0];
        selectedProductionScheduleId = firstPendingOrder?.production_schedule_id || null;
        productionScheduleAllocationPage = 1;
    }

    renderProductionScheduleSummary();
    renderProductionScheduleTable();
    renderProductionScheduleInspector();
    renderProductionScheduleAllocationTable();
}

async function refreshDashboardKpis() {
    const payload = await apiRequest('/dashboard/kpis');
    const responseData = payload?.data || {};

    dashboardKpis = {
        year: Number(responseData.year || new Date().getFullYear()),
        month: Number(responseData.month || (new Date().getMonth() + 1)),
        month_label: String(responseData.month_label || dashboardKpis.month_label || ''),
        production_this_month: Number(responseData.production_this_month || 0),
        top_used_part: responseData.top_used_part && typeof responseData.top_used_part === 'object'
            ? {
                part_id: Number(responseData.top_used_part.part_id || 0),
                part_name: String(responseData.top_used_part.part_name || ''),
                tricoma_nr: String(responseData.top_used_part.tricoma_nr || ''),
                total_quantity_used: Number(responseData.top_used_part.total_quantity_used || 0)
            }
            : null,
        top_produced_product: responseData.top_produced_product && typeof responseData.top_produced_product === 'object'
            ? {
                product_id: Number(responseData.top_produced_product.product_id || 0),
                product_name: String(responseData.top_produced_product.product_name || ''),
                total_quantity_produced: Number(responseData.top_produced_product.total_quantity_produced || 0)
            }
            : null,
        shortage_parts: Array.isArray(responseData.shortage_parts)
            ? responseData.shortage_parts.map((row) => ({
                part_id: Number(row?.part_id || 0),
                part_name: String(row?.part_name || ''),
                tricoma_nr: row?.tricoma_nr == null ? '' : String(row.tricoma_nr),
                tracked_stock: Number(row?.tracked_stock || 0),
                reorder_level: Number(row?.reorder_level || 0),
                missing_quantity: Number(row?.missing_quantity || 0)
            }))
            : [],
        pending_productions: Array.isArray(responseData.pending_productions)
            ? responseData.pending_productions.map((row) => ({
                production_schedule_id: Number(row?.production_schedule_id || 0),
                product_id: Number(row?.product_id || 0),
                product_name: String(row?.product_name || ''),
                planned_quantity: Number(row?.planned_quantity || 0),
                due_date: String(row?.due_date || '').trim(),
                comments: String(row?.comments || '').trim(),
                created_by: String(row?.created_by || '').trim()
            }))
            : []
    };

    renderDashboardKpis();
}

function buildAppLogQueryString(filters = appLogFilters) {
    const params = new URLSearchParams();

    Object.entries(filters || {}).forEach(([key, value]) => {
        const normalized = String(value ?? '').trim();
        if (normalized) {
            params.set(key, normalized);
        }
    });

    return params.toString();
}

function populateAppLogLevelSelect(levels = [], selectedValue = '') {
    if (!appLogLevelFilterSelect) {
        return;
    }

    appLogLevelFilterSelect.innerHTML = '';

    const placeholderOption = document.createElement('option');
    placeholderOption.value = '';
    placeholderOption.textContent = 'All levels';
    appLogLevelFilterSelect.appendChild(placeholderOption);

    const uniqueLevels = [...new Set((Array.isArray(levels) ? levels : [])
        .map((level) => String(level ?? '').trim().toLowerCase())
        .filter(Boolean))];

    uniqueLevels.forEach((level) => {
        const option = document.createElement('option');
        option.value = level;
        option.textContent = level.charAt(0).toUpperCase() + level.slice(1);
        appLogLevelFilterSelect.appendChild(option);
    });

    appLogLevelFilterSelect.value = uniqueLevels.includes(String(selectedValue || '').toLowerCase())
        ? String(selectedValue).toLowerCase()
        : '';
}

async function refreshAppLogs(options = {}) {
    const { signal } = options;
    const queryString = buildAppLogQueryString();
    const endpoint = queryString ? `/admin/logs?${queryString}` : '/admin/logs';
    const payload = await apiRequest(endpoint, { signal });
    appLogLevelOptions = Array.isArray(payload?.meta?.levels) && payload.meta.levels.length > 0
        ? payload.meta.levels
        : ['debug', 'info', 'warn', 'error', 'critical'];
    populateAppLogLevelSelect(appLogLevelOptions, appLogFilters.level);
    loadAppLogTable(Array.isArray(payload?.data) ? payload.data : []);
}

async function refreshMostUsedPartsReport(year = mostUsedPartsYear) {
    const numericYear = Number(year);
    const queryYear = Number.isInteger(numericYear) ? numericYear : mostUsedPartsYear;
    const payload = await apiRequest(`/analytics/most-used-parts?year=${encodeURIComponent(String(queryYear))}`);
    mostUsedPartsData = Array.isArray(payload?.data) ? payload.data : [];
    mostUsedPartsMeta = {
        year: Number(payload?.meta?.year || queryYear),
        total_quantity_used: Number(payload?.meta?.total_quantity_used || 0),
        total_distinct_parts: Number(payload?.meta?.total_distinct_parts || 0)
    };
    mostUsedPartsYear = mostUsedPartsMeta.year;
    mostUsedPartsPage = 1;
    renderMostUsedPartsSummary();
    renderMostUsedPartsTable();
}

async function refreshMostProducedProductsReport(year = mostProducedProductsYear) {
    const numericYear = Number(year);
    const queryYear = Number.isInteger(numericYear) ? numericYear : mostProducedProductsYear;
    const payload = await apiRequest(`/analytics/most-produced-products?year=${encodeURIComponent(String(queryYear))}`);
    mostProducedProductsData = Array.isArray(payload?.data) ? payload.data : [];
    mostProducedProductsMeta = {
        year: Number(payload?.meta?.year || queryYear),
        total_quantity_produced: Number(payload?.meta?.total_quantity_produced || 0),
        total_distinct_products: Number(payload?.meta?.total_distinct_products || 0)
    };
    mostProducedProductsYear = mostProducedProductsMeta.year;
    mostProducedProductsPage = 1;
    renderMostProducedProductsSummary();
    renderMostProducedProductsTable();
}

function getAllowedPanelsForCurrentUser() {
    if (isPasswordChangeRequired()) {
        return [];
    }

    const role = String(currentUser?.role || '').toLowerCase();
    return ROLE_PANEL_ACCESS[role] || [];
}

function isPasswordChangeRequired() {
    return Boolean(currentUser?.must_change_password);
}

function isAdminUser() {
    return String(currentUser?.role || '').toLowerCase() === 'admin';
}

function canAccessPanel(panelName) {
    return getAllowedPanelsForCurrentUser().includes(panelName);
}

// Formats backend date strings into dd/mm/yy for table display and search.
function formatDateDDMMYY(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';

    const dd = String(date.getDate()).padStart(2, '0');
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const yy = String(date.getFullYear()).slice(-2);

    return `${dd}/${mm}/${yy}`;
}

function formatDateTimeDDMMYYHHMM(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';

    const dd = String(date.getDate()).padStart(2, '0');
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const yy = String(date.getFullYear()).slice(-2);
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');

    return `${dd}/${mm}/${yy} ${hours}:${minutes}`;
}

function getLocalTodayIsoDate() {
    const today = new Date();
    const year = String(today.getFullYear());
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

// Formats numeric values for table display: integers stay clean, decimals stay visible.
function formatNumberForDisplay(value) {
    if (value == null || value === '') return '';

    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
        return String(value);
    }

    return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 10
    }).format(numericValue);
}

function isGramTrackedPart(partLike = {}) {
    const partId = Number(partLike?.part_id);
    if (GRAM_TRACKED_PART_IDS.has(partId)) {
        return true;
    }

    const tricomaNr = String(partLike?.tricoma_nr ?? '').trim();
    if (tricomaNr && GRAM_TRACKED_PART_TRICOMA_NUMBERS.has(tricomaNr)) {
        return true;
    }

    const partName = String(partLike?.part_name ?? partLike?.name ?? '').trim().toLowerCase();
    if (!partName) {
        return false;
    }

    return GRAM_TRACKED_PART_NAME_MATCHES.some((match) => partName.includes(match));
}

function formatPartQuantityForDisplay(value, partLike = {}) {
    const formattedValue = formatNumberForDisplay(value);
    if (!formattedValue) {
        return formattedValue;
    }

    return isGramTrackedPart(partLike) ? `${formattedValue} g` : formattedValue;
}

// Escapes untrusted text before it is inserted into HTML strings.
function escapeHtml(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

const INPUT_CONTROL_CHAR_REGEX = /[\u0000-\u001F\u007F]/;
const INPUT_ANGLE_BRACKET_REGEX = /[<>]/;

// Shared frontend input normalization so forms behave consistently before data
// is sent to the backend validation layer.
function normalizeInputTextValue(value, options = {}) {
    const { collapseWhitespace = true } = options;
    let normalized = String(value ?? '').trim();

    if (collapseWhitespace) {
        normalized = normalized.replace(/\s+/g, ' ');
    }

    return normalized;
}

function hasUnsafeInputCharacters(value) {
    return INPUT_CONTROL_CHAR_REGEX.test(value) || INPUT_ANGLE_BRACKET_REGEX.test(value);
}

function readValidatedTextInput(input, options = {}) {
    const {
        fieldLabel = 'value',
        requiredMessage = `Please enter ${fieldLabel}.`,
        maxLength = 120,
        collapseWhitespace = true
    } = options;

    const normalized = normalizeInputTextValue(input?.value, { collapseWhitespace });

    if (!normalized) {
        alert(requiredMessage);
        return null;
    }

    if (normalized.length > maxLength) {
        alert(`${fieldLabel} must be ${maxLength} characters or less.`);
        return null;
    }

    if (hasUnsafeInputCharacters(normalized)) {
        alert(`${fieldLabel} contains unsupported characters.`);
        return null;
    }

    if (input) {
        input.value = normalized;
    }

    return normalized;
}

function readOptionalValidatedTextInput(input, options = {}) {
    const {
        fieldLabel = 'value',
        maxLength = 120,
        collapseWhitespace = true
    } = options;

    const normalized = normalizeInputTextValue(input?.value, { collapseWhitespace });
    if (!normalized) {
        if (input) {
            input.value = '';
        }
        return '';
    }

    if (normalized.length > maxLength) {
        alert(`${fieldLabel} must be ${maxLength} characters or less.`);
        return null;
    }

    if (hasUnsafeInputCharacters(normalized)) {
        alert(`${fieldLabel} contains unsupported characters.`);
        return null;
    }

    if (input) {
        input.value = normalized;
    }

    return normalized;
}

function readValidatedUsernameInput(input, options = {}) {
    const {
        fieldLabel = 'username',
        requiredMessage = 'Please enter a username.',
        allowEmpty = false
    } = options;

    const normalized = String(input?.value ?? '').trim().toLowerCase();

    if (!normalized) {
        if (allowEmpty) {
            return '';
        }

        alert(requiredMessage);
        return null;
    }

    if (normalized.length < 3 || normalized.length > 40) {
        alert(`${fieldLabel} must be between 3 and 40 characters.`);
        return null;
    }

    if (!/^[a-z0-9._-]+$/i.test(normalized)) {
        alert(`${fieldLabel} may only contain letters, numbers, dots, underscores, and hyphens.`);
        return null;
    }

    if (input) {
        input.value = normalized;
    }

    return normalized;
}

function readValidatedPasswordInput(input, options = {}) {
    const {
        fieldLabel = 'password',
        requiredMessage = `Please enter ${fieldLabel}.`,
        allowEmpty = false,
        minLength = 8,
        maxLength = 200
    } = options;

    const value = String(input?.value ?? '');

    if (!value) {
        if (allowEmpty) {
            return '';
        }

        alert(requiredMessage);
        return null;
    }

    if (value.length < minLength || value.length > maxLength) {
        alert(`${fieldLabel} must be between ${minLength} and ${maxLength} characters.`);
        return null;
    }

    if (INPUT_CONTROL_CHAR_REGEX.test(value)) {
        alert(`${fieldLabel} contains unsupported characters.`);
        return null;
    }

    return value;
}

function readValidatedNumberInput(input, options = {}) {
    const {
        fieldLabel = 'value',
        requiredMessage = `Please enter ${fieldLabel}.`,
        invalidMessage = `Please enter a valid ${fieldLabel}.`,
        integer = false,
        positive = false,
        nonNegative = false,
        nonZero = false
    } = options;

    const normalized = normalizeInputTextValue(input?.value, { collapseWhitespace: false });

    if (!normalized) {
        alert(requiredMessage);
        return null;
    }

    const numericValue = Number(normalized);
    const isValid = Number.isFinite(numericValue)
        && (!integer || Number.isInteger(numericValue))
        && (!positive || numericValue > 0)
        && (!nonNegative || numericValue >= 0)
        && (!nonZero || numericValue !== 0);

    if (!isValid) {
        alert(invalidMessage);
        return null;
    }

    if (input) {
        input.value = normalized;
    }

    return numericValue;
}

function readValidatedYearInput(input, fieldLabel = 'year') {
    const year = readValidatedNumberInput(input, {
        fieldLabel,
        requiredMessage: 'Please enter a year.',
        invalidMessage: 'Please enter a valid year.',
        integer: true,
        positive: true
    });

    if (year == null) {
        return null;
    }

    if (year < 2000 || year > 2100) {
        alert(`${fieldLabel} must be between 2000 and 2100.`);
        return null;
    }

    return year;
}

function readValidatedDateInput(input, options = {}) {
    const {
        fieldLabel = 'date',
        requiredMessage = `Please select ${fieldLabel}.`,
        minDate = ''
    } = options;

    const normalized = normalizeInputTextValue(input?.value, { collapseWhitespace: false });
    if (!normalized) {
        alert(requiredMessage);
        return null;
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
        alert(`${fieldLabel} must use YYYY-MM-DD format.`);
        return null;
    }

    const parsedDate = new Date(`${normalized}T00:00:00Z`);
    const isValidDate = !Number.isNaN(parsedDate.getTime())
        && parsedDate.toISOString().slice(0, 10) === normalized;

    if (!isValidDate) {
        alert(`${fieldLabel} must be a valid calendar date.`);
        return null;
    }

    if (minDate && normalized < String(minDate)) {
        alert(`${fieldLabel} cannot be earlier than ${String(minDate)}.`);
        return null;
    }

    if (input) {
        input.value = normalized;
    }

    return normalized;
}

function readSelectedPositiveInteger(select, requiredMessage) {
    const numericValue = Number(select?.value);

    if (!Number.isInteger(numericValue) || numericValue <= 0) {
        alert(requiredMessage);
        return null;
    }

    return numericValue;
}

function readSelectedRole(select, requiredMessage = 'Please select a role.') {
    const role = String(select?.value || '').trim().toLowerCase();

    if (role !== 'admin' && role !== 'powermoon') {
        alert(requiredMessage);
        return null;
    }

    return role;
}

// Search boxes are sanitized more gently: unsupported characters are stripped
// and the query is capped so accidental long pastes do not hit the API.
function sanitizeSearchQuery(value, maxLength = 120) {
    return normalizeInputTextValue(value, { collapseWhitespace: true })
        .replace(/[\u0000-\u001F\u007F]/g, '')
        .replace(/[<>]/g, '')
        .slice(0, maxLength);
}

// Sorts the provided data by the given key and direction.
// Works for both numeric and string values (case-insensitive string sorting).
function sortData(data, key, direction) {
    return [...data].sort((a, b) => {
        const va = a[key];
        const vb = b[key];

        if (va == null && vb == null) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;

        const na = typeof va === 'number' ? va : Number(va);
        const nb = typeof vb === 'number' ? vb : Number(vb);
        const bothNumeric = Number.isFinite(na) && Number.isFinite(nb) && String(va).trim() !== '' && String(vb).trim() !== '';

        if (bothNumeric) {
            return direction * (na - nb);
        }

        const sa = String(va).toLowerCase();
        const sb = String(vb).toLowerCase();

        if (sa < sb) return -direction;
        if (sa > sb) return direction;
        return 0;
    });
}

// Enables click-to-sort behavior for a table.
// - tableId: id of the <table>
// - sortState: object holding current sort key/direction (mutated in place)
// - sortKeys: array of property names matching each column
// - renderFn: function to call after sort order changes
function setupSortableTable(tableId, sortState, sortKeys, renderFn) {
    const table = document.getElementById(tableId);
    if (!table) return;

    // Grab all header cells so the sort indicator arrow can be updated.
    const ths = Array.from(table.querySelectorAll('thead th'));

    const getArrow = () => (sortState.direction === 1 ? ' ▲' : ' ▼');

    const updateHeaders = () => {
        ths.forEach((th, idx) => {
            const key = sortKeys[idx];
            if (!key) return;

            if (!th.dataset.origText) {
                th.dataset.origText = th.textContent.trim();
            }

            const base = th.dataset.origText;
            if (sortState.key === key) {
                th.textContent = base + getArrow();
            } else {
                th.textContent = base;
            }
        });
    };

    ths.forEach((th, idx) => {
        const key = sortKeys[idx];
        if (!key) return;

        th.style.cursor = 'pointer';
        th.addEventListener('click', () => {
            if (sortState.key === key) {
                sortState.direction *= -1;
            } else {
                sortState.key = key;
                sortState.direction = 1;
            }
            renderFn();
            updateHeaders();
        });
    });

    updateHeaders();
}

document.addEventListener('DOMContentLoaded', function () {
    // Section 02: Startup Wiring and Shared Handlers.
    setupSortableTable('finished-products-table', finishedProductsSort, ['fin_product_id', 'product_name', 'fin_product_qty', 'fin_product_date', 'production_schedule_id', 'due_date', 'comments', 'created_by', null], () => loadFinishedProductsTable());
    setupSortableTable('stock-movement-table', stockMovementsSort, [null, 'part_name', 'stock_movement_qty', 'movement_type', 'created_by', 'source_product_name', 'stock_movement_date', null], () => {
        resetStockMovementPagination();
        loadStockMovementTable();
    });
    setupSortableTable('part-stock-table', partStockSort, ['part_id', 'part_name', 'tracked_stock', 'reorder_level', 'reorder_quantity', 'tricoma_nr', 'total_movements', 'total_used_in_assembly'], () => {
        resetPartStockPagination();
        loadPartStockTable();
    });

    // Wire up delete handling for finished products.
    setupFinishedProductsDeleteHandler();

    // Wire up selection/removal for pending production schedule rows.
    setupProductionScheduleTableHandler();

    // Wire up delete handling for stock movements.
    setupStockMovementDeleteHandler();

    if (productionScheduleDueDateInput) {
        productionScheduleDueDateInput.min = getLocalTodayIsoDate();
    }

    // Wire up the low-stock filter toggle for the Current Stock table.
    const partStockFilterBtn = document.querySelector('#part-stock-filter-btn');
    if (partStockFilterBtn) {
        const updateFilterButton = () => {
            partStockFilterBtn.textContent = partStockFilterLow ? 'Show all stock' : 'Show low stock';
        };

        partStockFilterBtn.addEventListener('click', () => {
            partStockFilterLow = !partStockFilterLow;
            updateFilterButton();
            resetPartStockPagination();
            loadPartStockTable();
        });

        updateFilterButton();
    }

    bootstrapAuthenticatedSession();
});

const appShell = document.querySelector('#app-shell');
const dashboardShell = document.querySelector('.dashboard-shell');
const sidebar = document.querySelector('.sidebar');
const authOverlay = document.querySelector('#auth-overlay');
const loginForm = document.querySelector('#login-form');
const loginUsernameInput = document.querySelector('#login-username-input');
const loginPasswordInput = document.querySelector('#login-password-input');
const loginBtn = document.querySelector('#login-btn');
const loginMessage = document.querySelector('#login-message');
const sessionDisplayName = document.querySelector('#session-display-name');
const sessionRoleLabel = document.querySelector('#session-role-label');
const toggleChangePasswordBtn = document.querySelector('#toggle-change-password-btn');
const logoutBtn = document.querySelector('#logout-btn');
const changePasswordPanel = document.querySelector('#change-password-panel');
const changePasswordStatus = document.querySelector('#change-password-status');
const currentPasswordInput = document.querySelector('#current-password-input');
const newPasswordInput = document.querySelector('#new-password-input');
const confirmPasswordInput = document.querySelector('#confirm-password-input');
const savePasswordBtn = document.querySelector('#save-password-btn');
const cancelPasswordBtn = document.querySelector('#cancel-password-btn');
const addMovementBtn = document.querySelector('#add-movement-btn');
const adminAddProductBtn = document.querySelector('#admin-add-product-btn');
const adminAddPartBtn = document.querySelector('#admin-add-part-btn');
const adminAddUsageBtn = document.querySelector('#admin-add-usage-btn');
const adminCsvToolsStatus = document.querySelector('#admin-csv-tools-status');
const adminProductsCsvInput = document.querySelector('#admin-products-csv-input');
const adminPartsCsvInput = document.querySelector('#admin-parts-csv-input');
const adminBomCsvInput = document.querySelector('#admin-bom-csv-input');
const adminExportProductsCsvBtn = document.querySelector('#admin-export-products-csv-btn');
const adminImportProductsCsvBtn = document.querySelector('#admin-import-products-csv-btn');
const adminExportPartsCsvBtn = document.querySelector('#admin-export-parts-csv-btn');
const adminImportPartsCsvBtn = document.querySelector('#admin-import-parts-csv-btn');
const adminExportBomCsvBtn = document.querySelector('#admin-export-bom-csv-btn');
const adminImportBomCsvBtn = document.querySelector('#admin-import-bom-csv-btn');
const adminDeletePartBtn = document.querySelector('#admin-delete-part-btn');
const adminDeleteProductBtn = document.querySelector('#admin-delete-product-btn');
const adminEditPartSelect = document.querySelector('#admin-edit-part-select');
const adminEditPartDetailsSelect = document.querySelector('#admin-edit-part-details-select');
const adminEditAddUsageBtn = document.querySelector('#admin-edit-add-usage-btn');
const adminSavePartBomBtn = document.querySelector('#admin-save-part-bom-btn');
const adminSavePartDetailsBtn = document.querySelector('#admin-save-part-details-btn');
const adminUserUsernameInput = document.querySelector('#admin-user-username-input');
const adminUserDisplayNameInput = document.querySelector('#admin-user-display-name-input');
const adminUserRoleSelect = document.querySelector('#admin-user-role-select');
const adminUserPasswordInput = document.querySelector('#admin-user-password-input');
const adminSaveUserBtn = document.querySelector('#admin-save-user-btn');
const adminCancelUserEditBtn = document.querySelector('#admin-cancel-user-edit-btn');
const currentStockProductFilterSelect = document.querySelector('#current-stock-product-filter-select');
const currentStockResetFilterBtn = document.querySelector('#current-stock-reset-filter-btn');
const productionPlannerProductSelect = document.querySelector('#production-planner-product-select');
const productionPlannerQtyInput = document.querySelector('#production-planner-qty-input');
const productionScheduleProductSelect = document.querySelector('#production-schedule-product-select');
const productionScheduleQtyInput = document.querySelector('#production-schedule-qty-input');
const productionScheduleDueDateInput = document.querySelector('#production-schedule-due-date-input');
const productionScheduleCommentsInput = document.querySelector('#production-schedule-comments-input');
const productionPlannerCalculateBtn = document.querySelector('#production-planner-calculate-btn');
const productionScheduleAddBtn = document.querySelector('#production-schedule-add-btn');
const productionPlannerExportPdfBtn = document.querySelector('#production-planner-export-pdf-btn');
const productionPlannerPageSummary = document.querySelector('#production-planner-page-summary');
const productionPlannerPaginationControls = document.querySelector('#production-planner-pagination-controls');
const productionSchedulePendingCount = document.querySelector('#production-schedule-pending-count');
const productionScheduleUnitsCount = document.querySelector('#production-schedule-units-count');
const productionScheduleReadyCount = document.querySelector('#production-schedule-ready-count');
const productionScheduleShortageCount = document.querySelector('#production-schedule-shortage-count');
const productionScheduleNextDueCaption = document.querySelector('#production-schedule-next-due-caption');
const productionSchedulePageSummary = document.querySelector('#production-schedule-page-summary');
const productionSchedulePaginationControls = document.querySelector('#production-schedule-pagination-controls');
const productionScheduleInspectorCaption = document.querySelector('#production-schedule-inspector-caption');
const productionScheduleInspectorEmpty = document.querySelector('#production-schedule-inspector-empty');
const productionScheduleInspectorContent = document.querySelector('#production-schedule-inspector-content');
const productionScheduleInspectorProduct = document.querySelector('#production-schedule-inspector-product');
const productionScheduleInspectorSubtitle = document.querySelector('#production-schedule-inspector-subtitle');
const productionScheduleInspectorStatus = document.querySelector('#production-schedule-inspector-status');
const productionScheduleInspectorId = document.querySelector('#production-schedule-inspector-id');
const productionScheduleInspectorReady = document.querySelector('#production-schedule-inspector-ready');
const productionScheduleInspectorQty = document.querySelector('#production-schedule-inspector-qty');
const productionScheduleInspectorDue = document.querySelector('#production-schedule-inspector-due');
const productionScheduleInspectorMissing = document.querySelector('#production-schedule-inspector-missing');
const productionScheduleInspectorCreatedBy = document.querySelector('#production-schedule-inspector-created-by');
const productionScheduleInspectorCreatedAt = document.querySelector('#production-schedule-inspector-created-at');
const productionScheduleInspectorComments = document.querySelector('#production-schedule-inspector-comments');
const productionScheduleAllocationCaption = document.querySelector('#production-schedule-allocation-caption');
const productionScheduleAllocationPageSummary = document.querySelector('#production-schedule-allocation-page-summary');
const productionScheduleAllocationPaginationControls = document.querySelector('#production-schedule-allocation-pagination-controls');
const partStockSearchInput = document.querySelector('#part-stock-search-input');
const partStockExportPdfBtn = document.querySelector('#part-stock-export-pdf-btn');
const stockMovementSearchInput = document.querySelector('#stock-movement-search-input');
const stockMovementTypeFilterSelect = document.querySelector('#stock-movement-type-filter-select');
const stockMovementUserFilterSelect = document.querySelector('#stock-movement-user-filter-select');
const stockMovementProductFilterSelect = document.querySelector('#stock-movement-product-filter-select');
const stockMovementDateFromInput = document.querySelector('#stock-movement-date-from-input');
const stockMovementDateToInput = document.querySelector('#stock-movement-date-to-input');
const stockMovementResetFiltersBtn = document.querySelector('#stock-movement-reset-filters-btn');
const stockMovementExportCsvBtn = document.querySelector('#stock-movement-export-csv-btn');
const stockMovementExportPdfBtn = document.querySelector('#stock-movement-export-pdf-btn');
const stockMovementPageSummary = document.querySelector('#stock-movement-page-summary');
const stockMovementPaginationControls = document.querySelector('#stock-movement-pagination-controls');
const appLogSearchInput = document.querySelector('#app-log-search-input');
const appLogLevelFilterSelect = document.querySelector('#app-log-level-filter-select');
const appLogDateFromInput = document.querySelector('#app-log-date-from-input');
const appLogDateToInput = document.querySelector('#app-log-date-to-input');
const appLogResetFiltersBtn = document.querySelector('#app-log-reset-filters-btn');
const appLogRefreshBtn = document.querySelector('#app-log-refresh-btn');
const appLogPageSummary = document.querySelector('#app-log-page-summary');
const appLogPaginationControls = document.querySelector('#app-log-pagination-controls');
const dashboardPeriodCaption = document.querySelector('#dashboard-period-caption');
const dashboardProductionThisMonth = document.querySelector('#dashboard-production-this-month');
const dashboardProductionMonthCaption = document.querySelector('#dashboard-production-month-caption');
const dashboardTopUsedPartName = document.querySelector('#dashboard-top-used-part-name');
const dashboardTopUsedPartQty = document.querySelector('#dashboard-top-used-part-qty');
const dashboardTopUsedPartCaption = document.querySelector('#dashboard-top-used-part-caption');
const dashboardTopProducedProductName = document.querySelector('#dashboard-top-produced-product-name');
const dashboardTopProducedProductQty = document.querySelector('#dashboard-top-produced-product-qty');
const dashboardTopProducedProductCaption = document.querySelector('#dashboard-top-produced-product-caption');
const dashboardShortageList = document.querySelector('#dashboard-shortage-list');
const dashboardShortageCaption = document.querySelector('#dashboard-shortage-caption');
const dashboardPendingProductionsList = document.querySelector('#dashboard-pending-productions-list');
const dashboardPendingProductionsCaption = document.querySelector('#dashboard-pending-productions-caption');
const mostUsedPartsYearInput = document.querySelector('#most-used-parts-year-input');
const mostUsedPartsApplyBtn = document.querySelector('#most-used-parts-apply-btn');
const mostUsedPartsTopCount = document.querySelector('#most-used-parts-top-count');
const mostUsedPartsTopCaption = document.querySelector('#most-used-parts-top-caption');
const mostUsedPartsTopList = document.querySelector('#most-used-parts-top-list');
const mostUsedPartsTotalTypes = document.querySelector('#most-used-parts-total-types');
const mostUsedPartsPageSummary = document.querySelector('#most-used-parts-page-summary');
const mostUsedPartsPaginationControls = document.querySelector('#most-used-parts-pagination-controls');
const mostProducedProductsYearInput = document.querySelector('#most-produced-products-year-input');
const mostProducedProductsApplyBtn = document.querySelector('#most-produced-products-apply-btn');
const mostProducedProductsTotalProduced = document.querySelector('#most-produced-products-total-produced');
const mostProducedProductsTotalTypes = document.querySelector('#most-produced-products-total-types');
const mostProducedProductsTotalCaption = document.querySelector('#most-produced-products-total-caption');
const mostProducedProductsPageSummary = document.querySelector('#most-produced-products-page-summary');
const mostProducedProductsPaginationControls = document.querySelector('#most-produced-products-pagination-controls');
const partStockPageSummary = document.querySelector('#part-stock-page-summary');
const partStockPaginationControls = document.querySelector('#part-stock-pagination-controls');
const currentStockMaterialsCount = document.querySelector('#current-stock-materials-count');
const currentStockMaterialsCaption = document.querySelector('#current-stock-materials-caption');
const currentStockMaterialsList = document.querySelector('#current-stock-materials-list');
const productionPlannerBuildabilitySummary = document.querySelector('#production-planner-buildability-summary');
const productionPlannerMaxBuildable = document.querySelector('#production-planner-max-buildable');
const productionPlannerLimitingPartsCount = document.querySelector('#production-planner-limiting-parts-count');
const productionPlannerLimitingCaption = document.querySelector('#production-planner-limiting-caption');

if (mostUsedPartsYearInput) {
    mostUsedPartsYearInput.value = String(mostUsedPartsYear);
}

if (mostProducedProductsYearInput) {
    mostProducedProductsYearInput.value = String(mostProducedProductsYear);
}

populateAppLogLevelSelect(appLogLevelOptions, appLogFilters.level);
renderDashboardKpis();

function setLoginMessage(message = '') {
    if (loginMessage) {
        loginMessage.textContent = message;
    }
}

function setSidebarExpanded(expanded) {
    if (!dashboardShell) {
        return;
    }

    dashboardShell.classList.toggle('sidebar-expanded', Boolean(expanded));
}

if (sidebar) {
    sidebar.addEventListener('mouseenter', () => {
        setSidebarExpanded(true);
    });

    sidebar.addEventListener('mouseleave', () => {
        setSidebarExpanded(false);
    });

    sidebar.addEventListener('focusin', () => {
        setSidebarExpanded(true);
    });

    sidebar.addEventListener('focusout', (event) => {
        if (!sidebar.contains(event.relatedTarget)) {
            setSidebarExpanded(false);
        }
    });
}

function clearPasswordChangeInputs() {
    if (currentPasswordInput) currentPasswordInput.value = '';
    if (newPasswordInput) newPasswordInput.value = '';
    if (confirmPasswordInput) confirmPasswordInput.value = '';
}

function clearSessionIdleTimer() {
    if (sessionIdleTimerId) {
        window.clearTimeout(sessionIdleTimerId);
        sessionIdleTimerId = null;
    }
}

function scheduleSessionIdleLogout() {
    clearSessionIdleTimer();

    if (!currentUser || idleLogoutInFlight || !Number.isFinite(sessionLastActivityAt) || sessionLastActivityAt <= 0) {
        return;
    }

    const remainingMs = Math.max(0, (sessionLastActivityAt + SESSION_IDLE_TIMEOUT_MS) - Date.now());
    sessionIdleTimerId = window.setTimeout(() => {
        triggerIdleLogout('Signed out after 5 minutes of inactivity.');
    }, remainingMs);
}

function recordSessionActivity(options = {}) {
    const { forceBroadcast = false } = options;

    if (!currentUser || idleLogoutInFlight) {
        return;
    }

    const now = Date.now();
    sessionLastActivityAt = now;
    scheduleSessionIdleLogout();

    if (forceBroadcast || (now - sessionLastBroadcastActivityAt) >= SESSION_ACTIVITY_BROADCAST_INTERVAL_MS) {
        sessionLastBroadcastActivityAt = now;
        try {
            window.localStorage.setItem(SESSION_ACTIVITY_STORAGE_KEY, String(now));
        } catch (_err) {
            // Ignore storage write failures; local tab timeout still works.
        }
    }
}

function stopSessionIdleTracking() {
    clearSessionIdleTimer();
    idleLogoutInFlight = false;
    sessionLastActivityAt = 0;
    sessionLastBroadcastActivityAt = 0;
}

function startSessionIdleTracking() {
    idleLogoutInFlight = false;
    recordSessionActivity({ forceBroadcast: true });
}

function broadcastLogoutToOtherTabs() {
    try {
        window.localStorage.setItem(SESSION_LOGOUT_STORAGE_KEY, String(Date.now()));
    } catch (_err) {
        // Ignore storage write failures; the current tab still logs out.
    }
}

function logoutCurrentSession(options = {}) {
    const {
        message = '',
        syncAcrossTabs = true
    } = options;

    authRequestSeq += 1;
    idleLogoutInFlight = true;
    clearSessionIdleTimer();

    if (syncAcrossTabs) {
        broadcastLogoutToOtherTabs();
    }

    return apiRequest('/auth/logout', {
        method: 'POST',
        allowUnauthorized: true
    })
        .catch((err) => {
            console.error(err);
        })
        .finally(() => {
            handleUnauthorizedState(message);
        });
}

function triggerIdleLogout(message) {
    if (!currentUser || idleLogoutInFlight) {
        return;
    }

    logoutCurrentSession({
        message,
        syncAcrossTabs: true
    });
}

function handleSharedSessionStorageEvent(event) {
    if (event.key === SESSION_ACTIVITY_STORAGE_KEY) {
        const sharedActivityAt = Number(event.newValue);
        if (!currentUser || !Number.isFinite(sharedActivityAt) || sharedActivityAt <= 0) {
            return;
        }

        sessionLastActivityAt = Math.max(sessionLastActivityAt, sharedActivityAt);
        sessionLastBroadcastActivityAt = Math.max(sessionLastBroadcastActivityAt, sharedActivityAt);
        scheduleSessionIdleLogout();
        return;
    }

    if (event.key === SESSION_LOGOUT_STORAGE_KEY && currentUser && !idleLogoutInFlight) {
        handleUnauthorizedState('Signed out from another tab.');
    }
}

function handlePotentialSessionActivity() {
    recordSessionActivity();
}

[
    'pointerdown',
    'keydown',
    'scroll',
    'touchstart',
    'mousemove',
    'focus'
].forEach((eventName) => {
    window.addEventListener(eventName, handlePotentialSessionActivity, { passive: true });
});

document.addEventListener('visibilitychange', () => {
    if (!currentUser || document.visibilityState !== 'visible') {
        return;
    }

    if (sessionLastActivityAt > 0 && (Date.now() - sessionLastActivityAt) >= SESSION_IDLE_TIMEOUT_MS) {
        triggerIdleLogout('Signed out after 5 minutes of inactivity.');
        return;
    }

    scheduleSessionIdleLogout();
});

window.addEventListener('storage', handleSharedSessionStorageEvent);

function setChangePasswordPanelVisible(visible) {
    if (!changePasswordPanel) return;

    const shouldShow = Boolean(visible) || isPasswordChangeRequired();
    changePasswordPanel.hidden = !shouldShow;
    if (!shouldShow) {
        clearPasswordChangeInputs();
    }
}

function setPasswordChangeStatus(message = '') {
    if (!changePasswordStatus) {
        return;
    }

    changePasswordStatus.textContent = message;
    changePasswordStatus.hidden = !message;
}

function resetCachedDashboardState() {
    dashboardKpis = {
        year: new Date().getFullYear(),
        month: new Date().getMonth() + 1,
        month_label: new Intl.DateTimeFormat('en-US', {
            month: 'long',
            year: 'numeric'
        }).format(new Date()),
        production_this_month: 0,
        top_used_part: null,
        top_produced_product: null,
        shortage_parts: [],
        pending_productions: []
    };
    finishedProductsData = [];
    stockMovementsData = [];
    stockMovementsPage = 1;
    partStockData = [];
    productionPlannerData = [];
    productionPlannerResult = null;
    productionPlannerPage = 1;
    productionScheduleOrders = [];
    productionSchedulePage = 1;
    productionScheduleAllocationPage = 1;
    selectedProductionScheduleId = null;
    productionScheduleMeta = {
        pending_orders: 0,
        scheduled_units: 0,
        allocated_orders: 0,
        shortage_orders: 0,
        next_due_date: ''
    };
    mostUsedPartsData = [];
    mostUsedPartsYear = new Date().getFullYear();
    mostUsedPartsPage = 1;
    mostUsedPartsMeta = {
        year: mostUsedPartsYear,
        total_quantity_used: 0,
        total_distinct_parts: 0
    };
    mostProducedProductsData = [];
    mostProducedProductsYear = new Date().getFullYear();
    mostProducedProductsPage = 1;
    mostProducedProductsMeta = {
        year: mostProducedProductsYear,
        total_quantity_produced: 0,
        total_distinct_products: 0
    };
    adminUsersData = [];
    adminEditingUserId = null;
    partsCache = null;
    productsCache = null;
    partStockSearchQuery = '';
    partStockProductId = null;
    partStockRequiredPartIds = new Set();
    partStockProductRequirements = [];
    stockMovementFilters = {
        query: '',
        movement_type: '',
        created_by: '',
        source_product_name: '',
        date_from: '',
        date_to: ''
    };
    appLogsData = [];
    appLogsPage = 1;
    appLogFilters = {
        query: '',
        level: '',
        date_from: '',
        date_to: ''
    };
    appLogLevelOptions = ['debug', 'info', 'warn', 'error', 'critical'];
    stockMovementFilterOptions = {
        movement_types: ['IN', 'OUT', 'OUT - Production'],
        created_by: [],
        source_products: []
    };

    if (finishedSearchController) {
        finishedSearchController.abort();
        finishedSearchController = null;
    }

    if (stockMovementSearchController) {
        stockMovementSearchController.abort();
        stockMovementSearchController = null;
    }

    if (appLogSearchController) {
        appLogSearchController.abort();
        appLogSearchController = null;
    }

    renderCurrentStockMaterialsPanel();
    renderProductionPlannerBuildabilitySummary();
    renderProductionPlannerTable([]);
    renderDashboardKpis();
    updateProductionPlannerExportButton();
    updatePartStockExportButton();
    loadFinishedProductsTable([]);
    loadStockMovementTable([]);
    loadPartStockTable([]);
    renderMostUsedPartsSummary();
    renderMostUsedPartsTable();
    renderMostProducedProductsSummary();
    renderMostProducedProductsTable();
    loadAppLogTable([]);
    loadAdminUsersTable([]);

    if (partStockSearchInput) partStockSearchInput.value = '';
    if (finishedProductSearchInput) finishedProductSearchInput.value = '';
    if (stockMovementSearchInput) stockMovementSearchInput.value = '';
    if (stockMovementTypeFilterSelect) stockMovementTypeFilterSelect.value = '';
    if (stockMovementUserFilterSelect) stockMovementUserFilterSelect.innerHTML = '<option value="">All users</option>';
    if (stockMovementProductFilterSelect) stockMovementProductFilterSelect.innerHTML = '<option value="">All produced products</option>';
    if (stockMovementDateFromInput) stockMovementDateFromInput.value = '';
    if (stockMovementDateToInput) stockMovementDateToInput.value = '';
    if (appLogSearchInput) appLogSearchInput.value = '';
    if (appLogDateFromInput) appLogDateFromInput.value = '';
    if (appLogDateToInput) appLogDateToInput.value = '';
    populateAppLogLevelSelect(appLogLevelOptions, '');
    if (mostUsedPartsYearInput) mostUsedPartsYearInput.value = String(mostUsedPartsYear);
    if (mostProducedProductsYearInput) mostProducedProductsYearInput.value = String(mostProducedProductsYear);
    if (currentStockProductFilterSelect) currentStockProductFilterSelect.selectedIndex = 0;
    if (productionPlannerProductSelect) productionPlannerProductSelect.selectedIndex = 0;
    if (productionPlannerQtyInput) productionPlannerQtyInput.value = '';
    if (productionScheduleProductSelect) productionScheduleProductSelect.selectedIndex = 0;
    if (productionScheduleQtyInput) productionScheduleQtyInput.value = '';
    if (productionScheduleDueDateInput) productionScheduleDueDateInput.value = '';
    if (productionScheduleCommentsInput) productionScheduleCommentsInput.value = '';

    setPasswordChangeStatus('');
    resetAdminUserForm();
}

function applyRoleVisibility() {
    const allowedPanels = new Set(getAllowedPanelsForCurrentUser());

    tabButtons.forEach((button) => {
        const panelName = button.dataset.panelTarget || button.getAttribute('aria-controls');
        const visible = allowedPanels.has(panelName);
        button.hidden = !visible;
        button.disabled = !visible;

        if (!visible) {
            button.classList.remove('active');
            button.setAttribute('aria-selected', 'false');
        }
    });

    const nextPanel = allowedPanels.has(activePanelName)
        ? activePanelName
        : (getAllowedPanelsForCurrentUser()[0] || 'dashboard');

    activateTabPanel(nextPanel);
}

function applyPasswordChangeRequiredState() {
    tabButtons.forEach((button) => {
        button.hidden = true;
        button.disabled = true;
        button.classList.remove('active');
        button.setAttribute('aria-selected', 'false');
    });

    tabPanels.forEach((panel) => {
        panel.classList.remove('active');
        panel.hidden = true;
    });

    if (toggleChangePasswordBtn) {
        toggleChangePasswordBtn.disabled = true;
    }

    if (cancelPasswordBtn) {
        cancelPasswordBtn.hidden = true;
    }

    setPasswordChangeStatus('You must change your password before continuing.');
    setChangePasswordPanelVisible(true);

    if (currentPasswordInput) {
        currentPasswordInput.focus();
    }
}

function setAuthenticatedState(user) {
    currentUser = user;

    if (sessionDisplayName) {
        sessionDisplayName.textContent = user?.display_name || user?.username || '-';
    }

    if (sessionRoleLabel) {
        const roleKey = String(user?.role || '').toLowerCase();
        sessionRoleLabel.textContent = USER_ROLE_LABELS[roleKey] || roleKey || '-';
    }

    if (authOverlay) authOverlay.hidden = true;
    if (appShell) appShell.hidden = false;
    if (loginForm) loginForm.reset();
    setLoginMessage('');
    startSessionIdleTracking();

    if (isPasswordChangeRequired()) {
        applyPasswordChangeRequiredState();
        return;
    }

    if (toggleChangePasswordBtn) {
        toggleChangePasswordBtn.disabled = false;
    }

    if (cancelPasswordBtn) {
        cancelPasswordBtn.hidden = false;
    }

    setPasswordChangeStatus('');
    setChangePasswordPanelVisible(false);
    applyRoleVisibility();
}

function handleUnauthorizedState(message = '') {
    currentUser = null;
    activePanelName = 'dashboard';
    setSidebarExpanded(false);
    stopSessionIdleTracking();
    resetCachedDashboardState();

    if (sessionDisplayName) {
        sessionDisplayName.textContent = '-';
    }

    if (sessionRoleLabel) {
        sessionRoleLabel.textContent = '-';
    }

    if (toggleChangePasswordBtn) {
        toggleChangePasswordBtn.disabled = false;
    }

    if (cancelPasswordBtn) {
        cancelPasswordBtn.hidden = false;
    }

    if (appShell) appShell.hidden = true;
    if (authOverlay) authOverlay.hidden = false;
    setChangePasswordPanelVisible(false);
    setPasswordChangeStatus('');
    setLoginMessage(message);

    if (loginUsernameInput && !loginUsernameInput.value) {
        loginUsernameInput.focus();
    }
}

async function loadInitialDashboardData() {
    const jobs = [
        refreshDashboardKpis(),
        refreshPartsDropdowns(true),
        refreshProductsDropdowns(true),
        refreshStockMovementsTable(),
        refreshPartStockTable(),
        refreshProductionSchedule(),
        refreshMostUsedPartsReport(),
        refreshMostProducedProductsReport()
    ];

    if (isAdminUser()) {
        jobs.push(refreshFinishedProductsTable());
        jobs.push(refreshAppLogs());
        jobs.push(refreshAdminUsersTable());
    } else {
        loadFinishedProductsTable([]);
        loadAppLogTable([]);
        loadAdminUsersTable([]);
    }

    await Promise.all(jobs);
}

async function bootstrapAuthenticatedSession() {
    if (loginInFlight) {
        return;
    }

    const requestSeq = ++authRequestSeq;

    try {
        const payload = await apiRequest('/auth/me', { allowUnauthorized: true });
        if (requestSeq !== authRequestSeq) {
            return;
        }

        const user = payload?.data?.user || null;

        if (!user) {
            handleUnauthorizedState();
            return;
        }

        setAuthenticatedState(user);
        if (Boolean(user?.must_change_password)) {
            return;
        }
        await loadInitialDashboardData();
    } catch (err) {
        if (requestSeq !== authRequestSeq) {
            return;
        }

        const unauthorizedMessage = /Authentication required|Request failed \(401\)/i.test(String(err?.message || ''));
        if (unauthorizedMessage) {
            handleUnauthorizedState();
            return;
        }

        console.error('Failed to restore authenticated session data:', err);
    }
}

if (loginForm) {
    loginForm.addEventListener('submit', (event) => {
        event.preventDefault();
        const requestSeq = ++authRequestSeq;
        loginInFlight = true;

        const username = readValidatedUsernameInput(loginUsernameInput, {
            fieldLabel: 'username',
            requiredMessage: 'Please enter your username.'
        });
        if (username == null) {
            loginInFlight = false;
            return;
        }

        const password = readValidatedPasswordInput(loginPasswordInput, {
            fieldLabel: 'password',
            requiredMessage: 'Please enter your password.'
        });
        if (password == null) {
            loginInFlight = false;
            return;
        }

        if (loginBtn) {
            loginBtn.disabled = true;
        }
        setLoginMessage('');

        apiRequest('/auth/login', {
            method: 'POST',
            body: { username, password },
            allowUnauthorized: true
        })
            .then((payload) => {
                if (requestSeq !== authRequestSeq) {
                    return;
                }

                const user = payload?.data?.user || null;
                if (!user) {
                    throw new Error('Unable to load authenticated user.');
                }

                setAuthenticatedState(user);
                if (Boolean(user?.must_change_password)) {
                    return;
                }
                return loadInitialDashboardData();
            })
            .catch((err) => {
                if (requestSeq !== authRequestSeq) {
                    return;
                }

                console.error(err);
                if (currentUser) {
                    alert(err.message || 'Unable to load dashboard data.');
                } else {
                    setLoginMessage(err.message || 'Unable to sign in.');
                }
            })
            .finally(() => {
                loginInFlight = false;

                if (requestSeq !== authRequestSeq) {
                    return;
                }

                if (loginBtn) {
                    loginBtn.disabled = false;
                }
            });
    });
}

if (toggleChangePasswordBtn) {
    toggleChangePasswordBtn.addEventListener('click', () => {
        setChangePasswordPanelVisible(Boolean(changePasswordPanel?.hidden));
    });
}

if (cancelPasswordBtn) {
    cancelPasswordBtn.addEventListener('click', () => {
        setChangePasswordPanelVisible(false);
    });
}

if (savePasswordBtn) {
    savePasswordBtn.addEventListener('click', () => {
        const currentPassword = readValidatedPasswordInput(currentPasswordInput, {
            fieldLabel: 'current password',
            requiredMessage: 'Please enter your current password.'
        });
        if (currentPassword == null) {
            return;
        }

        const newPassword = readValidatedPasswordInput(newPasswordInput, {
            fieldLabel: 'new password',
            requiredMessage: 'Please enter a new password.'
        });
        if (newPassword == null) {
            return;
        }

        const confirmPassword = readValidatedPasswordInput(confirmPasswordInput, {
            fieldLabel: 'confirmation password',
            requiredMessage: 'Please confirm the new password.'
        });
        if (confirmPassword == null) {
            return;
        }

        if (newPassword !== confirmPassword) {
            alert('New password and confirmation do not match.');
            return;
        }

        savePasswordBtn.disabled = true;

        apiRequest('/auth/change-password', {
            method: 'PUT',
            body: {
                current_password: currentPassword,
                new_password: newPassword
            }
        })
            .then(async (payload) => {
                const user = payload?.data?.user || currentUser;
                if (user) {
                    setAuthenticatedState(user);
                }

                if (!Boolean(user?.must_change_password)) {
                    await loadInitialDashboardData();
                    setChangePasswordPanelVisible(false);
                    setPasswordChangeStatus('');
                }

                alert('Password updated successfully.');
            })
            .catch((err) => {
                console.error(err);
                alert(err.message || 'Unable to change password.');
            })
            .finally(() => {
                savePasswordBtn.disabled = false;
            });
    });
}

if (logoutBtn) {
    logoutBtn.addEventListener('click', () => {
        logoutCurrentSession();
    });
}

function getPartDisplayName(row, fallbackPrefix = 'Part') {
    const partId = Number(row?.part_id);
    const partName = String(row?.part_name || '').trim();
    if (partName) {
        return partName;
    }

    if (Number.isInteger(partId) && partId > 0) {
        return `${fallbackPrefix} ${partId}`;
    }

    return fallbackPrefix;
}

function normalizeBuildabilityParts(rows = []) {
    if (!Array.isArray(rows)) {
        return [];
    }

    return rows.map((row) => ({
        part_id: Number(row?.part_id),
        part_name: getPartDisplayName(row),
        tricoma_nr: String(row?.tricoma_nr || '').trim(),
        quantity_per_product: Number(row?.quantity_per_product ?? 0),
        reorder_quantity: Number(row?.reorder_quantity ?? 0),
        tracked_stock: Number(row?.tracked_stock ?? 0),
        buildable_units: Number(row?.buildable_units ?? 0)
    }));
}

function renderBuildableItemCards(container, items = [], options = {}) {
    if (!container) return;

    const {
        emptyMessage = 'No items found.',
        getName = (item) => getPartDisplayName(item),
        getMetaLines = () => []
    } = options;

    container.innerHTML = '';

    if (!Array.isArray(items) || items.length === 0) {
        const message = document.createElement('p');
        message.className = 'buildable-limiting-empty';
        message.textContent = emptyMessage;
        container.appendChild(message);
        return;
    }

    const fragment = document.createDocumentFragment();

    items.forEach((item, index) => {
        const card = document.createElement('div');
        card.className = 'buildable-limiting-item';
        card.title = getName(item);

        const badge = document.createElement('span');
        badge.className = 'buildable-limiting-index';
        badge.textContent = String(index + 1).padStart(2, '0');
        card.appendChild(badge);

        const body = document.createElement('div');
        body.className = 'buildable-limiting-body';

        const name = document.createElement('span');
        name.className = 'buildable-limiting-name';
        name.textContent = getName(item);
        body.appendChild(name);

        const metaLines = getMetaLines(item)
            .map((line) => String(line || '').trim())
            .filter(Boolean);

        metaLines.forEach((line) => {
            const meta = document.createElement('span');
            meta.className = 'buildable-limiting-meta';
            meta.textContent = line;
            body.appendChild(meta);
        });

        card.appendChild(body);
        fragment.appendChild(card);
    });

    container.appendChild(fragment);
}

function renderDashboardKpis() {
    if (dashboardPeriodCaption) {
        dashboardPeriodCaption.textContent = `Live overview for ${dashboardKpis.month_label || 'this month'} and ${dashboardKpis.year || new Date().getFullYear()} year-to-date.`;
    }

    if (dashboardProductionThisMonth) {
        dashboardProductionThisMonth.textContent = formatNumberForDisplay(dashboardKpis.production_this_month);
    }

    if (dashboardProductionMonthCaption) {
        dashboardProductionMonthCaption.textContent = `Total finished-product units produced in ${dashboardKpis.month_label || 'the current month'}.`;
    }

    const topUsedPart = dashboardKpis.top_used_part;
    if (dashboardTopUsedPartName) {
        dashboardTopUsedPartName.textContent = topUsedPart?.part_name || 'No production yet';
        dashboardTopUsedPartName.title = topUsedPart?.part_name || '';
    }

    if (dashboardTopUsedPartQty) {
        dashboardTopUsedPartQty.textContent = topUsedPart
            ? `Total used: ${formatPartQuantityForDisplay(topUsedPart.total_quantity_used, topUsedPart)}`
            : 'No part usage recorded yet';
    }

    if (dashboardTopUsedPartCaption) {
        dashboardTopUsedPartCaption.textContent = topUsedPart
            ? `Highest-consumption individual part in ${dashboardKpis.year}.`
            : `No production-run material usage recorded in ${dashboardKpis.year}.`;
    }

    const topProducedProduct = dashboardKpis.top_produced_product;
    if (dashboardTopProducedProductName) {
        dashboardTopProducedProductName.textContent = topProducedProduct?.product_name || 'No production yet';
        dashboardTopProducedProductName.title = topProducedProduct?.product_name || '';
    }

    if (dashboardTopProducedProductQty) {
        dashboardTopProducedProductQty.textContent = topProducedProduct
            ? `Total produced: ${formatNumberForDisplay(topProducedProduct.total_quantity_produced)}`
            : 'No finished products recorded yet';
    }

    if (dashboardTopProducedProductCaption) {
        dashboardTopProducedProductCaption.textContent = topProducedProduct
            ? `Highest-output product in ${dashboardKpis.year}.`
            : `No finished-product output recorded in ${dashboardKpis.year}.`;
    }

    if (dashboardShortageList) {
        dashboardShortageList.innerHTML = '';

        if (!Array.isArray(dashboardKpis.shortage_parts) || dashboardKpis.shortage_parts.length === 0) {
            const emptyMessage = document.createElement('p');
            emptyMessage.className = 'dashboard-kpi-empty';
            emptyMessage.textContent = 'No parts are currently below reorder level.';
            dashboardShortageList.appendChild(emptyMessage);
        } else {
            const fragment = document.createDocumentFragment();

            dashboardKpis.shortage_parts.forEach((part) => {
                const row = document.createElement('div');
                row.className = 'dashboard-kpi-list-item';

                const copy = document.createElement('div');
                copy.className = 'dashboard-kpi-list-copy';

                const title = document.createElement('span');
                title.className = 'dashboard-kpi-list-title';
                title.textContent = part.part_name || `Part ${part.part_id}`;
                title.title = part.part_name || '';
                copy.appendChild(title);

                const meta = document.createElement('span');
                meta.className = 'dashboard-kpi-list-meta';
                meta.textContent = [
                    part.tricoma_nr ? `Tricoma ${part.tricoma_nr}` : '',
                    `Stock ${formatPartQuantityForDisplay(part.tracked_stock, part)}`,
                    `Reorder ${formatPartQuantityForDisplay(part.reorder_level, part)}`
                ].filter(Boolean).join(' • ');
                copy.appendChild(meta);

                const value = document.createElement('span');
                value.className = 'dashboard-kpi-list-value';
                value.textContent = `Missing ${formatPartQuantityForDisplay(part.missing_quantity, part)}`;

                row.appendChild(copy);
                row.appendChild(value);
                fragment.appendChild(row);
            });

            dashboardShortageList.appendChild(fragment);
        }
    }

    if (dashboardShortageCaption) {
        dashboardShortageCaption.textContent = Array.isArray(dashboardKpis.shortage_parts) && dashboardKpis.shortage_parts.length > 0
            ? 'Current top 10 parts with the highest missing quantity to reorder level.'
            : 'No current shortages against reorder level.';
    }

    if (dashboardPendingProductionsList) {
        dashboardPendingProductionsList.innerHTML = '';

        if (!Array.isArray(dashboardKpis.pending_productions) || dashboardKpis.pending_productions.length === 0) {
            const emptyMessage = document.createElement('p');
            emptyMessage.className = 'dashboard-kpi-empty';
            emptyMessage.textContent = 'No pending productions scheduled.';
            dashboardPendingProductionsList.appendChild(emptyMessage);
        } else {
            const fragment = document.createDocumentFragment();

            dashboardKpis.pending_productions.forEach((order) => {
                const row = document.createElement('div');
                row.className = 'dashboard-kpi-list-item';

                const copy = document.createElement('div');
                copy.className = 'dashboard-kpi-list-copy';

                const title = document.createElement('span');
                title.className = 'dashboard-kpi-list-title';
                title.textContent = order.product_name || `Product ${order.product_id}`;
                title.title = order.product_name || '';
                copy.appendChild(title);

                const meta = document.createElement('span');
                meta.className = 'dashboard-kpi-list-meta';
                meta.textContent = [
                    `Qty ${formatNumberForDisplay(order.planned_quantity)}`,
                    order.created_by ? `Created by ${order.created_by}` : '',
                    order.due_date ? `Due ${formatDateDDMMYY(order.due_date)}` : ''
                ].filter(Boolean).join(' • ');
                copy.appendChild(meta);

                if (order.comments) {
                    const note = document.createElement('span');
                    note.className = 'dashboard-kpi-list-note';
                    note.textContent = `Comment: ${order.comments}`;
                    copy.appendChild(note);
                }

                row.appendChild(copy);
                fragment.appendChild(row);
            });

            dashboardPendingProductionsList.appendChild(fragment);
        }
    }

    if (dashboardPendingProductionsCaption) {
        dashboardPendingProductionsCaption.textContent = Array.isArray(dashboardKpis.pending_productions) && dashboardKpis.pending_productions.length > 0
            ? 'Nearest pending productions ordered by due date.'
            : 'No pending productions are waiting to be scheduled.';
    }
}

function renderCurrentStockMaterialsPanel() {
    if (!currentStockMaterialsList) return;

    if (!partStockProductId) {
        if (currentStockMaterialsCount) {
            currentStockMaterialsCount.textContent = '-';
        }

        if (currentStockMaterialsCaption) {
            currentStockMaterialsCaption.textContent = 'Select a product to see the parts required for assembly.';
        }

        renderBuildableItemCards(currentStockMaterialsList, [], {
            emptyMessage: 'Choose a product to see the materials used in its BOM.'
        });
        return;
    }

    const materials = Array.isArray(partStockProductRequirements) ? partStockProductRequirements : [];
    if (currentStockMaterialsCount) {
        currentStockMaterialsCount.textContent = String(materials.length);
    }

    if (currentStockMaterialsCaption) {
        if (materials.length === 0) {
            currentStockMaterialsCaption.textContent = 'No BOM relations were found for the selected product.';
        } else {
            const noun = materials.length === 1 ? 'part is' : 'parts are';
            currentStockMaterialsCaption.textContent = `${materials.length} ${noun} used to assemble the selected product.`;
        }
    }

    renderBuildableItemCards(currentStockMaterialsList, materials, {
        emptyMessage: 'No BOM relations found for the selected product.'
    });
}

function renderProductionPlannerBuildabilitySummary() {
    if (!productionPlannerBuildabilitySummary) return;

    const result = productionPlannerResult;
    const limitingParts = normalizeBuildabilityParts(result?.limiting_parts);

    if (!result || !Number.isInteger(Number(result.product_id)) || Number(result.product_id) <= 0) {
        productionPlannerBuildabilitySummary.hidden = true;

        if (productionPlannerMaxBuildable) {
            productionPlannerMaxBuildable.textContent = '-';
        }

        if (productionPlannerLimitingPartsCount) {
            productionPlannerLimitingPartsCount.textContent = '-';
        }

        if (productionPlannerLimitingCaption) {
            productionPlannerLimitingCaption.textContent = 'Run a calculation to inspect the current bottlenecks.';
        }
        return;
    }

    productionPlannerBuildabilitySummary.hidden = false;

    if (productionPlannerMaxBuildable) {
        productionPlannerMaxBuildable.textContent = formatNumberForDisplay(result.max_buildable ?? 0);
    }

    if (productionPlannerLimitingPartsCount) {
        productionPlannerLimitingPartsCount.textContent = String(limitingParts.length);
    }

    if (productionPlannerLimitingCaption) {
        if (limitingParts.length === 0) {
            productionPlannerLimitingCaption.textContent = 'No limiting parts were identified for this product.';
        } else {
            const maxBuildable = formatNumberForDisplay(result.max_buildable ?? 0);
            const noun = limitingParts.length === 1 ? 'part is' : 'parts are';
            const unitSuffix = Number(result.max_buildable ?? 0) === 1 ? '' : 's';
            productionPlannerLimitingCaption.textContent = `${limitingParts.length} ${noun} currently limiting this product to ${maxBuildable} buildable unit${unitSuffix}.`;
        }
    }
}

function resetProductionSchedulePagination() {
    productionSchedulePage = 1;
}

function resetProductionScheduleAllocationPagination() {
    productionScheduleAllocationPage = 1;
}

function getPendingProductionScheduleOrders() {
    return productionScheduleOrders.filter((row) => String(row?.schedule_status || '').trim().toLowerCase() !== 'completed');
}

function getSelectedProductionScheduleOrder() {
    const selectedId = Number(selectedProductionScheduleId);
    return getPendingProductionScheduleOrders().find((row) => Number(row.production_schedule_id) === selectedId) || null;
}

function selectProductionScheduleOrder(productionScheduleId) {
    const numericId = Number(productionScheduleId);
    if (!Number.isInteger(numericId) || numericId <= 0) {
        return;
    }

    if (Number(selectedProductionScheduleId) === numericId) {
        return;
    }

    selectedProductionScheduleId = numericId;
    resetProductionScheduleAllocationPagination();
    renderProductionScheduleTable();
    renderProductionScheduleInspector();
    renderProductionScheduleAllocationTable();
}

function getProductionScheduleStatusLabel(order) {
    if (String(order?.schedule_status || '').trim().toLowerCase() === 'completed') {
        return 'Completed';
    }

    const normalized = String(order?.allocation_status || '').trim();
    return normalized || 'Pending';
}

function getProductionScheduleStatusClassSuffix(statusLabel) {
    return String(statusLabel || 'pending').trim().toLowerCase().replace(/\s+/g, '-');
}

function getProductionScheduleReadinessPercent(order) {
    if (String(order?.schedule_status || '').trim().toLowerCase() === 'completed') {
        return 100;
    }

    const totalMaterials = Number(order?.total_materials ?? 0);
    if (totalMaterials <= 0) {
        return 0;
    }

    const missingParts = Math.max(0, Number(order?.missing_parts_count ?? 0));
    const readyParts = Math.max(0, totalMaterials - missingParts);
    return Math.max(0, Math.min(100, Math.round((readyParts / totalMaterials) * 100)));
}

function getProductionScheduleCommentPreview(value, maxLength = 120) {
    const normalized = String(value || '').trim().replace(/\s+/g, ' ');
    if (!normalized) {
        return 'No notes on this production order.';
    }

    if (normalized.length <= maxLength) {
        return normalized;
    }

    return `${normalized.slice(0, maxLength - 1).trimEnd()}...`;
}

function renderProductionScheduleSummary() {
    if (productionSchedulePendingCount) {
        productionSchedulePendingCount.textContent = formatNumberForDisplay(productionScheduleMeta.pending_orders);
    }

    if (productionScheduleUnitsCount) {
        productionScheduleUnitsCount.textContent = formatNumberForDisplay(productionScheduleMeta.scheduled_units);
    }

    if (productionScheduleReadyCount) {
        productionScheduleReadyCount.textContent = formatNumberForDisplay(productionScheduleMeta.allocated_orders);
    }

    if (productionScheduleShortageCount) {
        productionScheduleShortageCount.textContent = formatNumberForDisplay(productionScheduleMeta.shortage_orders);
    }

    if (productionScheduleNextDueCaption) {
        if (!productionScheduleMeta.pending_orders) {
            productionScheduleNextDueCaption.textContent = 'No pending productions scheduled.';
        } else if (productionScheduleMeta.next_due_date) {
            productionScheduleNextDueCaption.textContent = `Next due date: ${formatDateDDMMYY(productionScheduleMeta.next_due_date)}.`;
        } else {
            productionScheduleNextDueCaption.textContent = 'Pending productions are sorted by due date automatically.';
        }
    }
}

function renderProductionSchedulePagination(totalItems, totalPages, startIndex, visibleCount) {
    if (productionSchedulePageSummary) {
        if (totalItems <= 0) {
            productionSchedulePageSummary.textContent = 'No pending productions scheduled';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            productionSchedulePageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} pending productions`;
        }
    }

    if (!productionSchedulePaginationControls) {
        return;
    }

    productionSchedulePaginationControls.innerHTML = '';
    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.disabled = disabled;
        button.className = `part-stock-page-button${active ? ' is-active' : ''}${nav ? ' is-nav' : ''}`;
        button.addEventListener('click', () => {
            if (pageNumber === productionSchedulePage) return;
            productionSchedulePage = pageNumber;
            renderProductionScheduleTable();
        });
        fragment.appendChild(button);
    };

    appendButton('Previous', Math.max(1, productionSchedulePage - 1), {
        disabled: productionSchedulePage <= 1,
        nav: true
    });

    for (let page = 1; page <= totalPages; page += 1) {
        appendButton(String(page), page, {
            active: page === productionSchedulePage
        });
    }

    appendButton('Next', Math.min(totalPages, productionSchedulePage + 1), {
        disabled: productionSchedulePage >= totalPages,
        nav: true
    });

    productionSchedulePaginationControls.appendChild(fragment);
}

function renderProductionScheduleInspector() {
    const selectedOrder = getSelectedProductionScheduleOrder();
    if (!selectedOrder) {
        if (productionScheduleInspectorCaption) {
            productionScheduleInspectorCaption.textContent = 'Select a pending production to inspect notes, dates, readiness, and execution context.';
        }
        if (productionScheduleInspectorEmpty) {
            productionScheduleInspectorEmpty.hidden = false;
        }
        if (productionScheduleInspectorContent) {
            productionScheduleInspectorContent.hidden = true;
        }
        return;
    }

    if (productionScheduleInspectorEmpty) {
        productionScheduleInspectorEmpty.hidden = true;
    }
    if (productionScheduleInspectorContent) {
        productionScheduleInspectorContent.hidden = false;
    }

    const statusLabel = getProductionScheduleStatusLabel(selectedOrder);
    const readinessPercent = getProductionScheduleReadinessPercent(selectedOrder);

    if (productionScheduleInspectorCaption) {
        productionScheduleInspectorCaption.textContent = 'Pending productions reserve material automatically by due date and show readiness before completion.';
    }

    if (productionScheduleInspectorProduct) {
        productionScheduleInspectorProduct.textContent = selectedOrder.product_name || `Product ${selectedOrder.product_id}`;
    }

    if (productionScheduleInspectorSubtitle) {
        const subtitleParts = [
            `ID ${selectedOrder.production_schedule_id}`,
            `Qty ${formatNumberForDisplay(selectedOrder.planned_quantity)}`,
            selectedOrder.due_date ? `Due ${formatDateDDMMYY(selectedOrder.due_date)}` : '',
            selectedOrder.created_by ? `Created by ${selectedOrder.created_by}` : ''
        ].filter(Boolean);
        productionScheduleInspectorSubtitle.textContent = subtitleParts.join(' • ') || 'No extra schedule metadata available.';
    }

    if (productionScheduleInspectorStatus) {
        productionScheduleInspectorStatus.textContent = statusLabel;
        productionScheduleInspectorStatus.className = `production-schedule-status-chip is-${getProductionScheduleStatusClassSuffix(statusLabel)}`;
    }

    if (productionScheduleInspectorId) {
        productionScheduleInspectorId.textContent = String(selectedOrder.production_schedule_id || '-');
    }

    if (productionScheduleInspectorReady) {
        productionScheduleInspectorReady.textContent = `${readinessPercent}%`;
    }

    if (productionScheduleInspectorQty) {
        productionScheduleInspectorQty.textContent = formatNumberForDisplay(selectedOrder.planned_quantity);
    }

    if (productionScheduleInspectorDue) {
        productionScheduleInspectorDue.textContent = selectedOrder.due_date ? formatDateDDMMYY(selectedOrder.due_date) : '-';
    }

    if (productionScheduleInspectorMissing) {
        productionScheduleInspectorMissing.textContent = formatNumberForDisplay(selectedOrder.missing_parts_count);
    }

    if (productionScheduleInspectorCreatedBy) {
        productionScheduleInspectorCreatedBy.textContent = selectedOrder.created_by || '-';
    }

    if (productionScheduleInspectorCreatedAt) {
        productionScheduleInspectorCreatedAt.textContent = selectedOrder.created_at
            ? formatDateTimeDDMMYYHHMM(selectedOrder.created_at)
            : '-';
    }

    if (productionScheduleInspectorComments) {
        productionScheduleInspectorComments.textContent = selectedOrder.comments || 'No comments recorded.';
    }
}

function renderProductionScheduleAllocationPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (productionScheduleAllocationPageSummary) {
        if (totalItems <= 0) {
            productionScheduleAllocationPageSummary.textContent = 'Showing 0 materials';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            productionScheduleAllocationPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} materials`;
        }
    }

    if (!productionScheduleAllocationPaginationControls) {
        return;
    }

    productionScheduleAllocationPaginationControls.innerHTML = '';
    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.disabled = disabled;
        button.className = `part-stock-page-button${active ? ' is-active' : ''}${nav ? ' is-nav' : ''}`;
        button.addEventListener('click', () => {
            if (pageNumber === productionScheduleAllocationPage) return;
            productionScheduleAllocationPage = pageNumber;
            renderProductionScheduleAllocationTable();
        });
        fragment.appendChild(button);
    };

    appendButton('Previous', Math.max(1, productionScheduleAllocationPage - 1), {
        disabled: productionScheduleAllocationPage <= 1,
        nav: true
    });

    for (let page = 1; page <= totalPages; page += 1) {
        appendButton(String(page), page, {
            active: page === productionScheduleAllocationPage
        });
    }

    appendButton('Next', Math.min(totalPages, productionScheduleAllocationPage + 1), {
        disabled: productionScheduleAllocationPage >= totalPages,
        nav: true
    });

    productionScheduleAllocationPaginationControls.appendChild(fragment);
}

function renderProductionScheduleTable(data = getPendingProductionScheduleOrders()) {
    const tbody = document.querySelector('#production-schedule-table tbody');
    if (!tbody) return;

    if (!Array.isArray(data) || data.length === 0) {
        tbody.innerHTML = "<tr><td class='no-data' colspan='9'>No pending productions scheduled.</td></tr>";
        renderProductionSchedulePagination(0, 0, 0, 0);
        return;
    }

    const totalItems = data.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / PRODUCTION_SCHEDULE_PAGE_SIZE));
    productionSchedulePage = Math.min(Math.max(productionSchedulePage, 1), totalPages);
    const startIndex = (productionSchedulePage - 1) * PRODUCTION_SCHEDULE_PAGE_SIZE;
    const paginatedData = data.slice(startIndex, startIndex + PRODUCTION_SCHEDULE_PAGE_SIZE);
    const todayIso = getLocalTodayIsoDate();

    tbody.innerHTML = '';
    const fragment = document.createDocumentFragment();

    paginatedData.forEach((row, index) => {
        const tr = document.createElement('tr');
        tr.dataset.id = String(row.production_schedule_id);
        tr.classList.add('production-schedule-row');

        if (Number(row.production_schedule_id) === Number(selectedProductionScheduleId)) {
            tr.classList.add('is-selected');
        }

        if (row.due_date && row.due_date < todayIso) {
            tr.classList.add('is-overdue');
        }

        const statusLabel = getProductionScheduleStatusLabel(row);
        const statusClassSuffix = getProductionScheduleStatusClassSuffix(statusLabel);
        const readinessPercent = getProductionScheduleReadinessPercent(row);
        const canDelete = isAdminUser() || Number(currentUser?.user_id) === Number(row.created_by_user_id);

        const markerCell = document.createElement('td');
        const marker = document.createElement('span');
        marker.className = `production-schedule-row-marker is-${statusClassSuffix}`;
        markerCell.appendChild(marker);
        tr.appendChild(markerCell);

        const productCell = document.createElement('td');
        const taskCell = document.createElement('div');
        taskCell.className = 'production-schedule-task-cell';

        const taskTitle = document.createElement('span');
        taskTitle.className = 'production-schedule-task-title';
        taskTitle.textContent = row.product_name || `Product ${row.product_id}`;
        taskCell.appendChild(taskTitle);

        const taskMeta = document.createElement('span');
        taskMeta.className = 'production-schedule-task-meta';
        const commentPreview = getProductionScheduleCommentPreview(row.comments);
        taskMeta.textContent = row.created_by
            ? `${row.created_by} • ${commentPreview}`
            : commentPreview;
        taskCell.appendChild(taskMeta);

        productCell.appendChild(taskCell);
        tr.appendChild(productCell);

        const idCell = document.createElement('td');
        idCell.textContent = String(row.production_schedule_id);
        tr.appendChild(idCell);

        const readinessCell = document.createElement('td');
        const readinessWrapper = document.createElement('div');
        readinessWrapper.className = 'production-schedule-readiness';

        const readinessBar = document.createElement('span');
        readinessBar.className = 'production-schedule-readiness-bar';
        const readinessFill = document.createElement('span');
        readinessFill.style.width = `${readinessPercent}%`;
        readinessBar.appendChild(readinessFill);

        const readinessValue = document.createElement('span');
        readinessValue.className = 'production-schedule-readiness-value';
        readinessValue.textContent = `${readinessPercent}%`;

        readinessWrapper.appendChild(readinessBar);
        readinessWrapper.appendChild(readinessValue);
        readinessCell.appendChild(readinessWrapper);
        tr.appendChild(readinessCell);

        const quantityCell = document.createElement('td');
        quantityCell.textContent = formatNumberForDisplay(row.planned_quantity);
        tr.appendChild(quantityCell);

        const createdCell = document.createElement('td');
        createdCell.textContent = row.created_at ? formatDateDDMMYY(row.created_at) : '-';
        tr.appendChild(createdCell);

        const dueDateCell = document.createElement('td');
        dueDateCell.textContent = formatDateDDMMYY(row.due_date);
        tr.appendChild(dueDateCell);

        const statusCell = document.createElement('td');
        const statusChip = document.createElement('span');
        statusChip.textContent = statusLabel;
        statusChip.className = `production-schedule-status-chip is-${statusClassSuffix}`;
        statusCell.appendChild(statusChip);
        tr.appendChild(statusCell);

        const actionCell = document.createElement('td');
        const actionGroup = document.createElement('div');
        actionGroup.className = 'production-schedule-action-group';

        const completeButton = document.createElement('button');
        completeButton.type = 'button';
        completeButton.className = 'production-schedule-complete-btn';
        completeButton.dataset.id = String(row.production_schedule_id);
        completeButton.textContent = 'Complete';
        actionGroup.appendChild(completeButton);

        if (canDelete) {
            const deleteButton = document.createElement('button');
            deleteButton.type = 'button';
            deleteButton.className = 'production-schedule-delete-btn';
            deleteButton.dataset.id = String(row.production_schedule_id);
            deleteButton.textContent = 'Remove';
            actionGroup.appendChild(deleteButton);
        }

        if (actionGroup.childNodes.length > 0) {
            actionCell.appendChild(actionGroup);
        } else {
            actionCell.textContent = '-';
        }
        tr.appendChild(actionCell);

        fragment.appendChild(tr);
    });

    tbody.appendChild(fragment);
    renderProductionSchedulePagination(totalItems, totalPages, startIndex, paginatedData.length);
}

function renderProductionScheduleAllocationTable() {
    const tbody = document.querySelector('#production-schedule-allocation-table tbody');
    if (!tbody) return;

    const selectedOrder = getSelectedProductionScheduleOrder();
    if (!selectedOrder) {
        if (productionScheduleAllocationCaption) {
            productionScheduleAllocationCaption.textContent = 'Select a pending production to inspect its reserved materials.';
        }
        tbody.innerHTML = "<tr><td class='no-data' colspan='8'>Select a production to inspect its material breakdown.</td></tr>";
        renderProductionScheduleAllocationPagination(0, 0, 0, 0);
        return;
    }

    if (productionScheduleAllocationCaption) {
        const statusText = selectedOrder.can_allocate_fully
            ? 'Fully allocated from current stock.'
            : selectedOrder.total_materials === 0
                ? 'No BOM relations were found for this product.'
                : `${formatNumberForDisplay(selectedOrder.missing_parts_count)} part(s) are short for this production.`;
        productionScheduleAllocationCaption.textContent = `${selectedOrder.product_name} • Qty ${formatNumberForDisplay(selectedOrder.planned_quantity)} • Due ${formatDateDDMMYY(selectedOrder.due_date)} • ${statusText}`;
    }

    const materials = Array.isArray(selectedOrder.allocation_materials) ? selectedOrder.allocation_materials : [];
    if (materials.length === 0) {
        tbody.innerHTML = "<tr><td class='no-data' colspan='8'>No BOM relations were found for the selected production.</td></tr>";
        renderProductionScheduleAllocationPagination(0, 0, 0, 0);
        return;
    }

    const totalItems = materials.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    productionScheduleAllocationPage = Math.min(Math.max(productionScheduleAllocationPage, 1), totalPages);
    const startIndex = (productionScheduleAllocationPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = materials.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    tbody.innerHTML = '';
    const fragment = document.createDocumentFragment();

    paginatedData.forEach((material) => {
        const tr = document.createElement('tr');
        if (!material.has_enough_reserved) {
            tr.className = 'low-stock';
        }

        const partIdentity = {
            part_id: material.part_id,
            part_name: material.part_name,
            tricoma_nr: material.tricoma_nr
        };

        const partCell = document.createElement('td');
        partCell.textContent = material.part_name || `Part ${material.part_id}`;
        tr.appendChild(partCell);

        const quantityPerProductCell = document.createElement('td');
        quantityPerProductCell.textContent = formatPartQuantityForDisplay(material.quantity_per_product, partIdentity);
        tr.appendChild(quantityPerProductCell);

        const requiredCell = document.createElement('td');
        requiredCell.textContent = formatPartQuantityForDisplay(material.required_quantity, partIdentity);
        tr.appendChild(requiredCell);

        const availableCell = document.createElement('td');
        availableCell.textContent = formatPartQuantityForDisplay(material.available_before_order, partIdentity);
        tr.appendChild(availableCell);

        const reservedCell = document.createElement('td');
        reservedCell.textContent = formatPartQuantityForDisplay(material.reserved_quantity, partIdentity);
        tr.appendChild(reservedCell);

        const shortageCell = document.createElement('td');
        shortageCell.textContent = formatPartQuantityForDisplay(material.shortage_quantity, partIdentity);
        tr.appendChild(shortageCell);

        const remainingCell = document.createElement('td');
        remainingCell.textContent = formatPartQuantityForDisplay(material.remaining_after_reservation, partIdentity);
        tr.appendChild(remainingCell);

        const statusCell = document.createElement('td');
        statusCell.textContent = material.has_enough_reserved ? 'Reserved' : 'Shortage';
        tr.appendChild(statusCell);

        fragment.appendChild(tr);
    });

    tbody.appendChild(fragment);
    renderProductionScheduleAllocationPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

// Clears all product-specific filtering/buildability state in the Current Stock view.
function clearCurrentStockProductFilter() {
    partStockProductId = null;
    resetPartStockPagination();
    partStockRequiredPartIds = new Set();
    partStockProductRequirements = [];
    renderCurrentStockMaterialsPanel();
}

function resetPartStockPagination() {
    partStockPage = 1;
}

function renderPartStockPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (partStockPageSummary) {
        if (totalItems <= 0) {
            partStockPageSummary.textContent = 'No matching parts';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            partStockPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} parts`;
        }
    }

    if (!partStockPaginationControls) {
        return;
    }

    partStockPaginationControls.innerHTML = '';

    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = 'part-stock-page-button';

        if (nav) {
            button.classList.add('is-nav');
        }

        if (active) {
            button.classList.add('is-active');
            button.setAttribute('aria-current', 'page');
        }

        button.disabled = disabled;
        button.addEventListener('click', () => {
            if (disabled || active || pageNumber === partStockPage) {
                return;
            }

            partStockPage = pageNumber;
            loadPartStockTable();
        });

        fragment.appendChild(button);
    };

    appendButton('Previous', partStockPage - 1, {
        disabled: partStockPage <= 1,
        nav: true
    });

    const maxVisiblePageButtons = 5;
    const firstPage = Math.max(1, Math.min(partStockPage - 2, totalPages - maxVisiblePageButtons + 1));
    const lastPage = Math.min(totalPages, firstPage + maxVisiblePageButtons - 1);

    for (let pageNumber = firstPage; pageNumber <= lastPage; pageNumber += 1) {
        appendButton(String(pageNumber), pageNumber, {
            active: pageNumber === partStockPage
        });
    }

    appendButton('Next', partStockPage + 1, {
        disabled: partStockPage >= totalPages,
        nav: true
    });

    partStockPaginationControls.appendChild(fragment);
}

function resetProductionPlannerPagination() {
    productionPlannerPage = 1;
}

function renderProductionPlannerPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (productionPlannerPageSummary) {
        if (totalItems <= 0) {
            productionPlannerPageSummary.textContent = 'No materials';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            productionPlannerPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} materials`;
        }
    }

    if (!productionPlannerPaginationControls) {
        return;
    }

    productionPlannerPaginationControls.innerHTML = '';

    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = 'part-stock-page-button';

        if (nav) {
            button.classList.add('is-nav');
        }

        if (active) {
            button.classList.add('is-active');
            button.setAttribute('aria-current', 'page');
        }

        button.disabled = disabled;
        button.addEventListener('click', () => {
            if (disabled || active || pageNumber === productionPlannerPage) {
                return;
            }

            productionPlannerPage = pageNumber;
            renderProductionPlannerTable();
        });

        fragment.appendChild(button);
    };

    appendButton('Previous', productionPlannerPage - 1, {
        disabled: productionPlannerPage <= 1,
        nav: true
    });

    const maxVisiblePageButtons = 5;
    const firstPage = Math.max(1, Math.min(productionPlannerPage - 2, totalPages - maxVisiblePageButtons + 1));
    const lastPage = Math.min(totalPages, firstPage + maxVisiblePageButtons - 1);

    for (let pageNumber = firstPage; pageNumber <= lastPage; pageNumber += 1) {
        appendButton(String(pageNumber), pageNumber, {
            active: pageNumber === productionPlannerPage
        });
    }

    appendButton('Next', productionPlannerPage + 1, {
        disabled: productionPlannerPage >= totalPages,
        nav: true
    });

    productionPlannerPaginationControls.appendChild(fragment);
}

function resetStockMovementPagination() {
    stockMovementsPage = 1;
}

function resetAppLogPagination() {
    appLogsPage = 1;
}

function renderStockMovementPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (stockMovementPageSummary) {
        if (totalItems <= 0) {
            stockMovementPageSummary.textContent = 'No transactions';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            stockMovementPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} transactions`;
        }
    }

    if (!stockMovementPaginationControls) {
        return;
    }

    stockMovementPaginationControls.innerHTML = '';

    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = 'part-stock-page-button';

        if (nav) {
            button.classList.add('is-nav');
        }

        if (active) {
            button.classList.add('is-active');
            button.setAttribute('aria-current', 'page');
        }

        button.disabled = disabled;
        button.addEventListener('click', () => {
            if (disabled || active || pageNumber === stockMovementsPage) {
                return;
            }

            stockMovementsPage = pageNumber;
            loadStockMovementTable();
        });

        fragment.appendChild(button);
    };

    appendButton('First', 1, {
        disabled: stockMovementsPage <= 1,
        nav: true
    });

    appendButton('Previous', stockMovementsPage - 1, {
        disabled: stockMovementsPage <= 1,
        nav: true
    });

    const maxVisiblePageButtons = 5;
    const firstPage = Math.max(1, Math.min(stockMovementsPage - 2, totalPages - maxVisiblePageButtons + 1));
    const lastPage = Math.min(totalPages, firstPage + maxVisiblePageButtons - 1);

    for (let pageNumber = firstPage; pageNumber <= lastPage; pageNumber += 1) {
        appendButton(String(pageNumber), pageNumber, {
            active: pageNumber === stockMovementsPage
        });
    }

    appendButton('Next', stockMovementsPage + 1, {
        disabled: stockMovementsPage >= totalPages,
        nav: true
    });

    appendButton('Last', totalPages, {
        disabled: stockMovementsPage >= totalPages,
        nav: true
    });

    stockMovementPaginationControls.appendChild(fragment);
}

function formatLogLevelLabel(level) {
    const normalized = String(level ?? '').trim().toLowerCase();
    if (!normalized) {
        return '-';
    }

    return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function formatAppLogDetails(detailsJson) {
    const rawValue = String(detailsJson ?? '').trim();
    if (!rawValue) {
        return '';
    }

    try {
        return JSON.stringify(JSON.parse(rawValue), null, 2);
    } catch (_error) {
        return rawValue;
    }
}

function renderAppLogPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (appLogPageSummary) {
        if (totalItems <= 0) {
            appLogPageSummary.textContent = 'Showing 0 log entries';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            appLogPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} log entries`;
        }
    }

    if (!appLogPaginationControls) {
        return;
    }

    appLogPaginationControls.innerHTML = '';

    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = 'part-stock-page-button';

        if (nav) {
            button.classList.add('is-nav');
        }

        if (active) {
            button.classList.add('is-active');
            button.setAttribute('aria-current', 'page');
        }

        button.disabled = disabled;
        button.addEventListener('click', () => {
            if (disabled || active || pageNumber === appLogsPage) {
                return;
            }

            appLogsPage = pageNumber;
            loadAppLogTable();
        });

        fragment.appendChild(button);
    };

    appendButton('Previous', appLogsPage - 1, {
        disabled: appLogsPage <= 1,
        nav: true
    });

    const maxVisiblePageButtons = 5;
    const firstPage = Math.max(1, Math.min(appLogsPage - 2, totalPages - maxVisiblePageButtons + 1));
    const lastPage = Math.min(totalPages, firstPage + maxVisiblePageButtons - 1);

    for (let pageNumber = firstPage; pageNumber <= lastPage; pageNumber += 1) {
        appendButton(String(pageNumber), pageNumber, {
            active: pageNumber === appLogsPage
        });
    }

    appendButton('Next', appLogsPage + 1, {
        disabled: appLogsPage >= totalPages,
        nav: true
    });

    appLogPaginationControls.appendChild(fragment);
}

function loadAppLogTable(data) {
    if (data) {
        appLogsData = data.map((row) => ({
            ...row,
            app_log_id: Number(row.app_log_id || 0),
            level: String(row.level || '').toLowerCase(),
            category: String(row.category || ''),
            message: String(row.message || ''),
            details_json: String(row.details_json || ''),
            actor_username: String(row.actor_username || ''),
            request_method: String(row.request_method || ''),
            request_path: String(row.request_path || ''),
            ip_address: String(row.ip_address || ''),
            created_at: row.created_at
        }));
    }

    const table = document.querySelector('#app-log-table tbody');
    if (!table) {
        return;
    }

    const totalItems = appLogsData.length;
    if (totalItems === 0) {
        table.innerHTML = "<tr><td class='no-data' colspan='7'>No log entries found</td></tr>";
        renderAppLogPagination(0, 0, 0, 0);
        return;
    }

    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    appLogsPage = Math.min(Math.max(appLogsPage, 1), totalPages);
    const startIndex = (appLogsPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = appLogsData.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    table.innerHTML = '';
    const fragment = document.createDocumentFragment();

    paginatedData.forEach(({ app_log_id, created_at, level, category, actor_username, message, details_json, request_method, request_path, ip_address }, idx) => {
        const tr = document.createElement('tr');

        const noCell = document.createElement('td');
        noCell.textContent = String(startIndex + idx + 1);
        noCell.title = `Log ID ${app_log_id}`;
        tr.appendChild(noCell);

        const dateCell = document.createElement('td');
        dateCell.textContent = formatDateTimeDDMMYYHHMM(created_at);
        dateCell.title = String(created_at || '');
        tr.appendChild(dateCell);

        const levelCell = document.createElement('td');
        const levelBadge = document.createElement('span');
        levelBadge.className = `log-level-badge log-level-${level || 'info'}`;
        levelBadge.textContent = formatLogLevelLabel(level);
        levelCell.appendChild(levelBadge);
        tr.appendChild(levelCell);

        const categoryCell = document.createElement('td');
        categoryCell.textContent = String(category || '-');
        tr.appendChild(categoryCell);

        const userCell = document.createElement('td');
        userCell.textContent = String(actor_username || '-');
        tr.appendChild(userCell);

        const messageCell = document.createElement('td');
        const messageWrap = document.createElement('div');
        messageWrap.className = 'log-book-message';

        const messageTitle = document.createElement('div');
        messageTitle.className = 'log-book-message-title';
        messageTitle.textContent = String(message || '-');
        messageWrap.appendChild(messageTitle);

        const formattedDetails = formatAppLogDetails(details_json);
        if (formattedDetails) {
            const details = document.createElement('details');
            details.className = 'log-book-details';

            const summary = document.createElement('summary');
            details.appendChild(summary);

            const pre = document.createElement('pre');
            pre.textContent = formattedDetails;
            details.appendChild(pre);

            messageWrap.appendChild(details);
        }

        messageCell.appendChild(messageWrap);
        tr.appendChild(messageCell);

        const requestCell = document.createElement('td');
        const requestWrap = document.createElement('div');
        requestWrap.className = 'log-book-request';

        const requestPathLabel = document.createElement('div');
        requestPathLabel.className = 'log-book-request-path';
        requestPathLabel.textContent = [String(request_method || '').trim(), String(request_path || '').trim()]
            .filter(Boolean)
            .join(' ') || '-';
        requestWrap.appendChild(requestPathLabel);

        if (ip_address) {
            const requestMeta = document.createElement('div');
            requestMeta.className = 'log-book-request-meta';
            requestMeta.textContent = `IP: ${ip_address}`;
            requestWrap.appendChild(requestMeta);
        }

        requestCell.appendChild(requestWrap);
        tr.appendChild(requestCell);

        fragment.appendChild(tr);
    });

    table.appendChild(fragment);
    renderAppLogPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

// Fetches buildability data for the currently selected product and updates UI state.
// The request sequence guard prevents slower responses from overwriting newer selections.
function refreshSelectedProductBuildability(options = {}) {
    const { silent = false, rerenderTable = true } = options;
    const productId = Number(partStockProductId);

    if (!Number.isInteger(productId) || productId <= 0) {
        return Promise.resolve();
    }

    const requestSeq = ++partStockBuildabilityRequestSeq;

    return apiRequest(`/products/${productId}/buildability`)
        .then((payload) => {
            if (requestSeq !== partStockBuildabilityRequestSeq) {
                return;
            }

            const responseData = payload?.data || {};
            const requirements = normalizeBuildabilityParts(responseData.requirements);

            partStockRequiredPartIds = new Set(
                requirements
                    .map((row) => Number(row.part_id))
                    .filter((id) => Number.isInteger(id) && id > 0)
            );
            partStockProductRequirements = requirements;
            renderCurrentStockMaterialsPanel();

            if (rerenderTable) {
                loadPartStockTable();
            }
        })
        .catch((err) => {
            if (requestSeq !== partStockBuildabilityRequestSeq) {
                return;
            }

            partStockRequiredPartIds = new Set();
            partStockProductRequirements = [];
            renderCurrentStockMaterialsPanel();

            if (rerenderTable) {
                loadPartStockTable();
            }

            console.error(err);
            if (!silent) {
                alert(err.message || 'Unable to load product buildability.');
            }
        });
}

function renderProductionPlannerTable(data = productionPlannerData, emptyMessage = 'Select a product and quantity to calculate material') {
    const tbody = document.querySelector('#production-planner-table tbody');
    if (!tbody) return;

    if (!Array.isArray(data) || data.length === 0) {
        tbody.innerHTML = `<tr><td class='no-data' colspan='6'>${escapeHtml(emptyMessage)}</td></tr>`;
        renderProductionPlannerPagination(0, 0, 0, 0);
        return;
    }

    const totalItems = data.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    productionPlannerPage = Math.min(Math.max(productionPlannerPage, 1), totalPages);
    const startIndex = (productionPlannerPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = data.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    tbody.innerHTML = '';
    const fragment = document.createDocumentFragment();

    paginatedData.forEach(({ part_id, part_name, tricoma_nr, quantity_per_product, required_quantity, tracked_stock, remaining_after_plan, has_enough_stock }) => {
        const tr = document.createElement('tr');
        if (!has_enough_stock) {
            tr.className = 'low-stock';
        }

        const partIdentity = { part_id, part_name, tricoma_nr };

        const partCell = document.createElement('td');
        partCell.textContent = String(part_name ?? '');
        tr.appendChild(partCell);

        const qtyPerProductCell = document.createElement('td');
        qtyPerProductCell.textContent = formatPartQuantityForDisplay(quantity_per_product, partIdentity);
        tr.appendChild(qtyPerProductCell);

        const requiredQtyCell = document.createElement('td');
        requiredQtyCell.textContent = formatPartQuantityForDisplay(required_quantity, partIdentity);
        tr.appendChild(requiredQtyCell);

        const currentStockCell = document.createElement('td');
        currentStockCell.textContent = formatPartQuantityForDisplay(tracked_stock, partIdentity);
        tr.appendChild(currentStockCell);

        const remainingCell = document.createElement('td');
        remainingCell.textContent = formatPartQuantityForDisplay(remaining_after_plan, partIdentity);
        tr.appendChild(remainingCell);

        const statusCell = document.createElement('td');
        statusCell.textContent = has_enough_stock ? 'OK' : 'Insufficient';
        tr.appendChild(statusCell);

        fragment.appendChild(tr);
    });

    tbody.appendChild(fragment);
    renderProductionPlannerPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

// Enables the planner PDF button only after a successful calculation exists in memory.
// This avoids exporting an empty document before the user has run a plan.
function updateProductionPlannerExportButton() {
    if (!productionPlannerExportPdfBtn) return;

    const hasPlan = Number.isInteger(Number(productionPlannerResult?.product_id))
        && Number.isFinite(Number(productionPlannerResult?.requested_quantity))
        && Array.isArray(productionPlannerResult?.materials)
        && productionPlannerResult.materials.length > 0;

    productionPlannerExportPdfBtn.disabled = !hasPlan;
}

// Resets the planner view when the selected product changes or the plan becomes stale.
// Keeping this centralized ensures table content, summary label, and export state stay aligned.
function resetProductionPlanner() {
    productionPlannerData = [];
    productionPlannerResult = null;
    resetProductionPlannerPagination();
    renderProductionPlannerBuildabilitySummary();
    renderProductionPlannerTable([]);
    updateProductionPlannerExportButton();
}

async function addProductionScheduleOrder() {
    const productId = readSelectedPositiveInteger(productionScheduleProductSelect, 'Please select a product.');
    if (productId == null) {
        return;
    }

    const quantity = readValidatedNumberInput(productionScheduleQtyInput, {
        fieldLabel: 'quantity',
        requiredMessage: 'Please enter a quantity.',
        invalidMessage: 'Please enter a valid quantity.',
        positive: true
    });
    if (quantity == null) {
        return;
    }

    const dueDate = readValidatedDateInput(productionScheduleDueDateInput, {
        fieldLabel: 'due date',
        requiredMessage: 'Please select a due date.',
        minDate: getLocalTodayIsoDate()
    });
    if (dueDate == null) {
        return;
    }

    const comments = readOptionalValidatedTextInput(productionScheduleCommentsInput, {
        fieldLabel: 'comments',
        maxLength: 500
    });
    if (comments == null) {
        return;
    }

    const originalLabel = productionScheduleAddBtn?.textContent || 'Add to Schedule';
    if (productionScheduleAddBtn) {
        productionScheduleAddBtn.disabled = true;
        productionScheduleAddBtn.textContent = 'Saving...';
    }

    try {
        const payload = await apiRequest('/production-schedule', {
            method: 'POST',
            body: {
                product_id: productId,
                quantity,
                due_date: dueDate,
                comments
            }
        });

        resetProductionSchedulePagination();
        resetProductionScheduleAllocationPagination();
        selectedProductionScheduleId = Number(payload?.data?.production_schedule_id || 0) || null;
        await refreshProductionSchedule();
        if (productionScheduleQtyInput) {
            productionScheduleQtyInput.value = '';
        }
        if (productionScheduleCommentsInput) {
            productionScheduleCommentsInput.value = '';
        }
        alert('Production added to the schedule.');
    } catch (err) {
        console.error(err);
        alert(err.message || 'Unable to add this production to the schedule.');
    } finally {
        if (productionScheduleAddBtn) {
            productionScheduleAddBtn.disabled = false;
            productionScheduleAddBtn.textContent = originalLabel;
        }
    }
}

// Produces filesystem-safe filename fragments for downloaded PDFs.
// The same helper is reused for planner exports and reorder-list exports.
function sanitizeFilenamePart(value, fallback = 'report') {
    const sanitized = String(value == null ? '' : value)
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 60);

    return sanitized || fallback;
}

// The reorder PDF is only meaningful once stock has been loaded from the backend.
function updatePartStockExportButton() {
    if (!partStockExportPdfBtn) return;
    partStockExportPdfBtn.disabled = !Array.isArray(partStockData) || partStockData.length === 0;
}

function updateStockMovementExportButtons() {
    const hasRows = Array.isArray(stockMovementsData) && stockMovementsData.length > 0;
    if (stockMovementExportCsvBtn) stockMovementExportCsvBtn.disabled = !hasRows;
    if (stockMovementExportPdfBtn) stockMovementExportPdfBtn.disabled = !hasRows;
}

function populateStockMovementFilterSelect(select, placeholderText, values, selectedValue = '') {
    if (!select) return;

    select.innerHTML = '';
    const placeholderOption = document.createElement('option');
    placeholderOption.value = '';
    placeholderOption.textContent = placeholderText;
    select.appendChild(placeholderOption);

    const uniqueValues = [...new Set((Array.isArray(values) ? values : [])
        .map((value) => String(value ?? '').trim())
        .filter(Boolean))]
        .sort((left, right) => left.localeCompare(right));

    uniqueValues.forEach((value) => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        select.appendChild(option);
    });

    select.value = uniqueValues.includes(String(selectedValue || '')) ? String(selectedValue) : '';
}

function setStockMovementFilterOptions(filterOptions = {}) {
    stockMovementFilterOptions = {
        movement_types: Array.isArray(filterOptions?.movement_types) ? filterOptions.movement_types : ['IN', 'OUT', 'OUT - Production'],
        created_by: Array.isArray(filterOptions?.created_by) ? filterOptions.created_by : [],
        source_products: Array.isArray(filterOptions?.source_products) ? filterOptions.source_products : []
    };

    populateStockMovementFilterSelect(
        stockMovementUserFilterSelect,
        'All users',
        stockMovementFilterOptions.created_by,
        stockMovementFilters.created_by
    );
    populateStockMovementFilterSelect(
        stockMovementProductFilterSelect,
        'All produced products',
        stockMovementFilterOptions.source_products,
        stockMovementFilters.source_product_name
    );

    if (stockMovementTypeFilterSelect) {
        stockMovementTypeFilterSelect.value = String(stockMovementFilters.movement_type || '');
    }
}

function syncStockMovementFiltersFromInputs() {
    stockMovementFilters = {
        query: sanitizeSearchQuery(stockMovementSearchInput?.value || ''),
        movement_type: String(stockMovementTypeFilterSelect?.value || '').trim(),
        created_by: String(stockMovementUserFilterSelect?.value || '').trim(),
        source_product_name: String(stockMovementProductFilterSelect?.value || '').trim(),
        date_from: String(stockMovementDateFromInput?.value || '').trim(),
        date_to: String(stockMovementDateToInput?.value || '').trim()
    };

    if (stockMovementSearchInput) {
        stockMovementSearchInput.value = stockMovementFilters.query;
    }
}

function syncAppLogFiltersFromInputs() {
    appLogFilters = {
        query: sanitizeSearchQuery(appLogSearchInput?.value || ''),
        level: String(appLogLevelFilterSelect?.value || '').trim().toLowerCase(),
        date_from: String(appLogDateFromInput?.value || '').trim(),
        date_to: String(appLogDateToInput?.value || '').trim()
    };

    if (appLogSearchInput) {
        appLogSearchInput.value = appLogFilters.query;
    }
}

function hasValidStockMovementDateRange(options = {}) {
    const { notify = false } = options;
    const { date_from: dateFrom, date_to: dateTo } = stockMovementFilters;

    if (dateFrom && dateTo && dateFrom > dateTo) {
        if (notify) {
            alert('The "from" date must be before or equal to the "to" date.');
        }
        return false;
    }

    return true;
}

function hasValidAppLogDateRange(options = {}) {
    const { notify = false } = options;
    const { date_from: dateFrom, date_to: dateTo } = appLogFilters;

    if (dateFrom && dateTo && dateFrom > dateTo) {
        if (notify) {
            alert('The "from" date must be before or equal to the "to" date.');
        }
        return false;
    }

    return true;
}

function requestStockMovementsWithCurrentFilters() {
    syncStockMovementFiltersFromInputs();

    if (!hasValidStockMovementDateRange({ notify: true })) {
        return;
    }

    if (stockMovementSearchController) {
        stockMovementSearchController.abort();
    }
    stockMovementSearchController = new AbortController();
    resetStockMovementPagination();

    refreshStockMovementsTable({ signal: stockMovementSearchController.signal })
        .catch((err) => {
            if (err?.name === 'AbortError') return;
            console.error('Failed to load stock movements:', err);
        });
}

function requestAppLogsWithCurrentFilters() {
    syncAppLogFiltersFromInputs();

    if (!hasValidAppLogDateRange({ notify: true })) {
        return;
    }

    if (appLogSearchController) {
        appLogSearchController.abort();
    }
    appLogSearchController = new AbortController();
    resetAppLogPagination();

    refreshAppLogs({ signal: appLogSearchController.signal })
        .catch((err) => {
            if (err?.name === 'AbortError') return;
            console.error('Failed to load application logs:', err);
        });
}

async function extractDownloadErrorMessage(response) {
    let errorMessage = `Request failed (${response.status})`;

    try {
        const payload = await response.json();
        errorMessage = payload?.error || payload?.detail || errorMessage;
    } catch (_err) {
        const text = await response.text();
        if (text) {
            errorMessage = text;
        }
    }

    return errorMessage;
}

function triggerBlobDownload(blob, filename) {
    const downloadUrl = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => window.URL.revokeObjectURL(downloadUrl), 1000);
}

function setAdminCsvStatus(message, options = {}) {
    if (!adminCsvToolsStatus) return;

    const { isError = false } = options;
    adminCsvToolsStatus.textContent = String(message || '');
    adminCsvToolsStatus.style.color = isError ? '#ffc2c8' : '';
}

async function readCsvFileText(fileInput, label) {
    const file = fileInput?.files?.[0];
    if (!file) {
        throw new Error(`Please choose a ${label} file first.`);
    }

    if (file.size > 5 * 1024 * 1024) {
        throw new Error(`${label} file must be 5 MB or smaller.`);
    }

    const text = await file.text();
    if (!String(text || '').trim()) {
        throw new Error(`${label} file is empty.`);
    }

    return text;
}

async function exportInventoryCatalogCsv(kind, button) {
    const originalLabel = button?.textContent || 'Export CSV';

    if (button) {
        button.disabled = true;
        button.textContent = 'Exporting...';
    }

    setAdminCsvStatus('Preparing CSV export...');

    try {
        const response = await fetch(`${API_BASE_URL}/admin/csv/${kind}/export`, {
            credentials: 'same-origin'
        });

        if (!response.ok) {
            if (response.status === 401) {
                handleUnauthorizedState('Session expired. Please sign in again.');
            }

            throw new Error(await extractDownloadErrorMessage(response));
        }

        const blob = await response.blob();
        const datePart = sanitizeFilenamePart(new Date().toISOString().slice(0, 10), 'today');
        triggerBlobDownload(blob, `${kind}-${datePart}.csv`);
        setAdminCsvStatus(`Exported ${kind.toUpperCase()} CSV successfully.`);
    } catch (err) {
        console.error(err);
        setAdminCsvStatus(err.message || `Unable to export ${kind.toUpperCase()} CSV.`, { isError: true });
        alert(err.message || `Unable to export ${kind.toUpperCase()} CSV.`);
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = originalLabel;
        }
    }
}

async function refreshAfterInventoryCsvImport(kind) {
    partsCache = null;
    productsCache = null;

    await Promise.all([
        refreshDashboardKpis(),
        refreshProductsDropdowns(true),
        refreshPartsDropdowns(true),
        refreshPartStockTable(),
        refreshFinishedProductsTable()
    ]);

    if (currentStockProductFilterSelect?.value) {
        await refreshSelectedProductBuildability({ silent: true, rerenderTable: true });
    }

    if (kind === 'bom' && adminEditPartSelect?.value) {
        await loadSelectedPartBomRelations();
    }

    if ((kind === 'parts' || kind === 'bom') && adminEditPartDetailsSelect?.value) {
        await loadSelectedPartDetails();
    }

    if (productionPlannerProductSelect?.value) {
        resetProductionPlanner();
    }
}

async function importInventoryCatalogCsv(kind, fileInput, button, label) {
    const originalLabel = button?.textContent || 'Import CSV';

    if (button) {
        button.disabled = true;
        button.textContent = 'Importing...';
    }

    setAdminCsvStatus(`Reading ${label}...`);

    try {
        const csvText = await readCsvFileText(fileInput, label);
        const payload = await apiRequest(`/admin/csv/${kind}/import`, {
            method: 'POST',
            body: {
                csv_text: csvText
            },
            timeoutMs: 30000
        });

        await refreshAfterInventoryCsvImport(kind);

        const createdCount = Number(payload?.data?.created_count || 0);
        const updatedCount = Number(payload?.data?.updated_count || 0);
        const rowCount = Number(payload?.data?.row_count || 0);
        const summary = `${label} imported successfully. Rows: ${rowCount}. Created: ${createdCount}. Updated: ${updatedCount}.`;

        if (fileInput) {
            fileInput.value = '';
        }

        setAdminCsvStatus(summary);
        alert(summary);
    } catch (err) {
        console.error(err);
        setAdminCsvStatus(err.message || `Unable to import ${label}.`, { isError: true });
        alert(err.message || `Unable to import ${label}.`);
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = originalLabel;
        }
    }
}

async function exportStockMovementsFile(format) {
    syncStockMovementFiltersFromInputs();

    if (!hasValidStockMovementDateRange({ notify: true })) {
        return;
    }

    const button = format === 'csv' ? stockMovementExportCsvBtn : stockMovementExportPdfBtn;
    const originalLabel = button?.textContent || `Export ${String(format || '').toUpperCase()}`;

    if (button) {
        button.disabled = true;
        button.textContent = 'Exporting...';
    }

    try {
        const queryString = buildStockMovementQueryString();
        const endpoint = `${API_BASE_URL}/stock-movements/export-${format}${queryString ? `?${queryString}` : ''}`;
        const response = await fetch(endpoint, {
            credentials: 'same-origin'
        });

        if (!response.ok) {
            if (response.status === 401) {
                handleUnauthorizedState('Session expired. Please sign in again.');
            }

            throw new Error(await extractDownloadErrorMessage(response));
        }

        const blob = await response.blob();
        const filename = `transactions-${sanitizeFilenamePart(new Date().toISOString().slice(0, 10), 'today')}.${format}`;
        triggerBlobDownload(blob, filename);
    } catch (err) {
        console.error(err);
        alert(err.message || `Unable to export transactions ${String(format || '').toUpperCase()}.`);
    } finally {
        if (button) {
            button.textContent = originalLabel;
        }
        updateStockMovementExportButtons();
    }
}

// Downloads a backend-generated reorder PDF based on current low-stock data.
// The server owns the low-stock calculation so browser filters cannot accidentally
// desync the exported procurement list from the source-of-truth stock snapshot.
async function exportPartStockReorderPdf() {
    const originalLabel = partStockExportPdfBtn?.textContent || 'Generate Reorder PDF';

    if (partStockExportPdfBtn) {
        partStockExportPdfBtn.disabled = true;
        partStockExportPdfBtn.textContent = 'Exporting...';
    }

    try {
        const response = await fetch(`${API_BASE_URL}/part-stock/export-reorder-pdf`, {
            credentials: 'same-origin'
        });

        // For binary downloads we still try to decode JSON/text errors first so the
        // user gets a readable alert instead of a silent failed download.
        if (!response.ok) {
            if (response.status === 401) {
                handleUnauthorizedState('Session expired. Please sign in again.');
            }

            let errorMessage = `Request failed (${response.status})`;

            try {
                const payload = await response.json();
                errorMessage = payload?.error || payload?.detail || errorMessage;
            } catch (_err) {
                const text = await response.text();
                if (text) {
                    errorMessage = text;
                }
            }

            throw new Error(errorMessage);
        }

        const blob = await response.blob();
        const filename = `reorder-list-low-stock-${sanitizeFilenamePart(new Date().toISOString().slice(0, 10), 'today')}.pdf`;
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');

        // A temporary anchor is the simplest browser-native way to trigger a download
        // without navigating away from the dashboard.
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.setTimeout(() => window.URL.revokeObjectURL(downloadUrl), 1000);
    } catch (err) {
        console.error(err);
        alert(err.message || 'Unable to export reorder PDF.');
    } finally {
        if (partStockExportPdfBtn) {
            partStockExportPdfBtn.textContent = originalLabel;
        }
        updatePartStockExportButton();
    }
}

// Downloads the missing-material PDF for the most recently calculated production plan.
// The function reuses cached planner state so the user does not have to re-enter inputs.
async function exportProductionPlannerPdf() {
    const productId = Number(productionPlannerResult?.product_id ?? productionPlannerProductSelect?.value);
    const quantity = Number(productionPlannerResult?.requested_quantity ?? productionPlannerQtyInput?.value);

    if (!Number.isInteger(productId) || productId <= 0) {
        alert('Please calculate a production plan first.');
        return;
    }

    if (!Number.isFinite(quantity) || quantity <= 0) {
        alert('Please calculate a production plan first.');
        return;
    }

    const originalLabel = productionPlannerExportPdfBtn?.textContent || 'Export PDF';
    if (productionPlannerExportPdfBtn) {
        productionPlannerExportPdfBtn.disabled = true;
        productionPlannerExportPdfBtn.textContent = 'Exporting...';
    }

    try {
        const response = await fetch(`${API_BASE_URL}/production-planner/export-pdf`, {
            method: 'POST',
            headers: { 'Content-type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({
                product_id: productId,
                quantity
            })
        });

        if (!response.ok) {
            if (response.status === 401) {
                handleUnauthorizedState('Session expired. Please sign in again.');
            }

            let errorMessage = `Request failed (${response.status})`;

            try {
                const payload = await response.json();
                errorMessage = payload?.error || payload?.detail || errorMessage;
            } catch (_err) {
                const text = await response.text();
                if (text) {
                    errorMessage = text;
                }
            }

            throw new Error(errorMessage);
        }

        const blob = await response.blob();
        const productName = productionPlannerResult?.product_name || productionPlannerProductSelect?.selectedOptions?.[0]?.textContent || 'product';
        const filename = `missing-materials-${sanitizeFilenamePart(productName, 'product')}-qty-${sanitizeFilenamePart(quantity, 'plan')}.pdf`;
        const downloadUrl = window.URL.createObjectURL(blob);
        const link = document.createElement('a');

        // Trigger the PDF download without leaving the page or opening a new tab.
        link.href = downloadUrl;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.setTimeout(() => window.URL.revokeObjectURL(downloadUrl), 1000);
    } catch (err) {
        console.error(err);
        alert(err.message || 'Unable to export production planner PDF.');
    } finally {
        if (productionPlannerExportPdfBtn) {
            productionPlannerExportPdfBtn.textContent = originalLabel;
        }
        updateProductionPlannerExportButton();
    }
}

// Requests the production-plan calculation from the backend using the selected
// product and quantity, then hydrates both the on-screen table and cached
// planner state used later by the PDF export button.
function calculateProductionPlanner() {
    const productId = readSelectedPositiveInteger(productionPlannerProductSelect, 'Please select a product.');
    if (productId == null) {
        return;
    }

    const quantity = readValidatedNumberInput(productionPlannerQtyInput, {
        fieldLabel: 'quantity',
        requiredMessage: 'Please enter a quantity.',
        invalidMessage: 'Please enter a valid quantity.',
        positive: true
    });
    if (quantity == null) {
        return;
    }

    apiRequest('/production-planner/calculate', {
        method: 'POST',
        body: {
            product_id: productId,
            quantity
        }
    })
        .then((payload) => {
            resetProductionPlannerPagination();
            const responseData = payload?.data || {};
            const materials = Array.isArray(responseData.materials) ? responseData.materials : [];
            const limitingParts = normalizeBuildabilityParts(responseData.limiting_parts);

            productionPlannerResult = {
                product_id: Number(responseData.product_id ?? productId),
                product_name: responseData.product_name || productionPlannerProductSelect?.selectedOptions?.[0]?.textContent || '',
                requested_quantity: Number(responseData.requested_quantity ?? quantity),
                max_buildable: Number(responseData.max_buildable ?? 0),
                materials: [],
                missing_materials: [],
                limiting_parts: limitingParts
            };

            if (materials.length === 0) {
                productionPlannerData = [];
                renderProductionPlannerBuildabilitySummary();
                renderProductionPlannerTable([], 'No BOM relations found for the selected product');
                updateProductionPlannerExportButton();
                return;
            }

            // Normalize all numeric/text fields once here so every downstream consumer
            // (table rendering, button state, PDF export) can rely on a stable shape.
            productionPlannerData = materials.map((row) => ({
                part_id: Number(row.part_id),
                part_name: row.part_name || `Part ${row.part_id}`,
                tricoma_nr: row.tricoma_nr || '',
                quantity_per_product: Number(row.quantity_per_product ?? 0),
                required_quantity: Number(row.required_quantity ?? 0),
                reorder_quantity: Number(row.reorder_quantity ?? 0),
                shortage_quantity: Number(row.shortage_quantity ?? 0),
                tracked_stock: Number(row.tracked_stock ?? 0),
                remaining_after_plan: Number(row.remaining_after_plan ?? 0),
                has_enough_stock: Boolean(row.has_enough_stock)
            }));
            productionPlannerResult = {
                ...productionPlannerResult,
                materials: productionPlannerData,
                missing_materials: Array.isArray(responseData.missing_materials) ? responseData.missing_materials : productionPlannerData.filter((row) => !row.has_enough_stock)
            };

            renderProductionPlannerBuildabilitySummary();
            renderProductionPlannerTable();
            updateProductionPlannerExportButton();
        })
        .catch((err) => {
            console.error(err);
            productionPlannerData = [];
            productionPlannerResult = null;
            renderProductionPlannerBuildabilitySummary();
            renderProductionPlannerTable([]);
            updateProductionPlannerExportButton();
            alert(err.message || 'Unable to calculate production materials.');
        });
}

async function refreshAdminUsersTable() {
    if (!isAdminUser()) {
        loadAdminUsersTable([]);
        return [];
    }

    const payload = await apiRequest('/users');
    loadAdminUsersTable(Array.isArray(payload?.data) ? payload.data : []);
    return adminUsersData;
}

function resetAdminUserForm() {
    adminEditingUserId = null;

    if (adminUserUsernameInput) adminUserUsernameInput.value = '';
    if (adminUserDisplayNameInput) adminUserDisplayNameInput.value = '';
    if (adminUserRoleSelect) adminUserRoleSelect.value = 'powermoon';
    if (adminUserPasswordInput) adminUserPasswordInput.value = '';
    if (adminSaveUserBtn) adminSaveUserBtn.textContent = 'Create User';
    if (adminCancelUserEditBtn) adminCancelUserEditBtn.hidden = true;
}

function loadAdminUsersTable(data) {
    if (data) {
        adminUsersData = data;
    }

    const tbody = document.querySelector('#admin-users-table tbody');
    if (!tbody) return;

    if (!Array.isArray(adminUsersData) || adminUsersData.length === 0) {
        tbody.innerHTML = "<tr><td class='no-data' colspan='5'>No users</td></tr>";
        return;
    }

    tbody.innerHTML = '';
    const fragment = document.createDocumentFragment();

    adminUsersData.forEach((user) => {
        const tr = document.createElement('tr');
        const isCurrentUser = Number(user?.user_id) === Number(currentUser?.user_id);

        const usernameCell = document.createElement('td');
        usernameCell.textContent = isCurrentUser
            ? `${String(user?.username || '')} (You)`
            : String(user?.username || '');
        tr.appendChild(usernameCell);

        const displayNameCell = document.createElement('td');
        displayNameCell.textContent = String(user?.display_name || '');
        tr.appendChild(displayNameCell);

        const roleCell = document.createElement('td');
        roleCell.textContent = USER_ROLE_LABELS[String(user?.role || '').toLowerCase()] || String(user?.role || '');
        tr.appendChild(roleCell);

        const updatedCell = document.createElement('td');
        updatedCell.textContent = formatDateDDMMYY(user?.updated_at);
        tr.appendChild(updatedCell);

        const actionCell = document.createElement('td');

        const editButton = document.createElement('button');
        editButton.type = 'button';
        editButton.className = 'admin-edit-user-btn';
        editButton.dataset.id = String(user?.user_id || '');
        editButton.textContent = 'Edit';
        actionCell.appendChild(editButton);

        const deleteButton = document.createElement('button');
        deleteButton.type = 'button';
        deleteButton.className = 'admin-delete-user-btn';
        deleteButton.dataset.id = String(user?.user_id || '');
        deleteButton.textContent = 'Delete';
        if (isCurrentUser) {
            deleteButton.disabled = true;
        }
        actionCell.appendChild(deleteButton);

        tr.appendChild(actionCell);
        fragment.appendChild(tr);
    });

    tbody.appendChild(fragment);
}

function loadUserIntoAdminForm(userId) {
    const user = adminUsersData.find(({ user_id }) => Number(user_id) === Number(userId));
    if (!user) {
        return;
    }

    adminEditingUserId = Number(user.user_id);

    if (adminUserUsernameInput) adminUserUsernameInput.value = user.username || '';
    if (adminUserDisplayNameInput) adminUserDisplayNameInput.value = user.display_name || '';
    if (adminUserRoleSelect) adminUserRoleSelect.value = String(user.role || 'powermoon').toLowerCase();
    if (adminUserPasswordInput) adminUserPasswordInput.value = '';
    if (adminSaveUserBtn) adminSaveUserBtn.textContent = 'Save User';
    if (adminCancelUserEditBtn) adminCancelUserEditBtn.hidden = false;
}
// Section 04: Stock Movement Creation.
// Creates a stock movement entry and refreshes dependent tables.
addMovementBtn.onclick = function () {
    const partSelect = document.querySelector('#movement-part-select');
    const qtyInput = document.querySelector('#movement-qty-input');

    const partId = readSelectedPositiveInteger(partSelect, 'Please select a part.');
    if (partId == null) {
        return;
    }

    const quantity = readValidatedNumberInput(qtyInput, {
        fieldLabel: 'quantity',
        requiredMessage: 'Please enter a quantity.',
        invalidMessage: 'Please enter a valid quantity.',
        nonZero: true
    });
    if (quantity == null) {
        return;
    }

    apiRequest('/stock-movements', {
        method: 'POST',
        body: { part_id: partId, quantity }
    })
    .then(() => {
        resetStockMovementPagination();
        return Promise.all([
        refreshDashboardKpis(),
        refreshStockMovementsTable(),
        refreshPartStockTable()
        ]);
    })
    .then(() => {
        qtyInput.value = '';
        partSelect.selectedIndex = 0;
    })
    .catch((err) => {
        console.error(err);
        alert(err.message || 'Unable to add stock movement.');
    });
}

const movementQtyInput = document.querySelector('#movement-qty-input');
if (movementQtyInput && addMovementBtn) {
    // Allow pressing Enter in the movement quantity field to submit quickly.
    movementQtyInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            addMovementBtn.click();
        }
    });
}

if (productionPlannerCalculateBtn) {
    productionPlannerCalculateBtn.addEventListener('click', calculateProductionPlanner);
}

if (productionScheduleAddBtn) {
    productionScheduleAddBtn.addEventListener('click', addProductionScheduleOrder);
}

if (productionPlannerExportPdfBtn) {
    productionPlannerExportPdfBtn.addEventListener('click', exportProductionPlannerPdf);
}

if (partStockExportPdfBtn) {
    partStockExportPdfBtn.addEventListener('click', exportPartStockReorderPdf);
}

if (stockMovementExportCsvBtn) {
    stockMovementExportCsvBtn.addEventListener('click', () => exportStockMovementsFile('csv'));
}

if (stockMovementExportPdfBtn) {
    stockMovementExportPdfBtn.addEventListener('click', () => exportStockMovementsFile('pdf'));
}

if (productionPlannerQtyInput && productionPlannerCalculateBtn) {
    productionPlannerQtyInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            productionPlannerCalculateBtn.click();
        }
    });
}

if (productionScheduleQtyInput && productionScheduleAddBtn) {
    productionScheduleQtyInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            productionScheduleAddBtn.click();
        }
    });
}

if (productionScheduleDueDateInput && productionScheduleAddBtn) {
    productionScheduleDueDateInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            productionScheduleAddBtn.click();
        }
    });
}

if (productionPlannerProductSelect) {
    productionPlannerProductSelect.addEventListener('change', resetProductionPlanner);
}

if (currentStockProductFilterSelect) {
    // Selecting a product narrows the part stock table to required parts only.
    currentStockProductFilterSelect.addEventListener('change', () => {
        const productId = Number(currentStockProductFilterSelect.value);

        if (!Number.isInteger(productId) || productId <= 0) {
            clearCurrentStockProductFilter();
            loadPartStockTable();
            return;
        }

        partStockProductId = productId;
        resetPartStockPagination();
        refreshSelectedProductBuildability({ silent: false, rerenderTable: true });
    });
}

if (currentStockResetFilterBtn) {
    // Restores the full part stock table and clears product buildability state.
    currentStockResetFilterBtn.addEventListener('click', () => {
        clearCurrentStockProductFilter();

        if (currentStockProductFilterSelect) {
            currentStockProductFilterSelect.selectedIndex = 0;
        }

        loadPartStockTable();
    });
}

// Section 05: Inventory Setup and Admin Management.
if (adminSaveUserBtn) {
    adminSaveUserBtn.addEventListener('click', () => {
        const username = readValidatedUsernameInput(adminUserUsernameInput, {
            fieldLabel: 'username',
            requiredMessage: 'Please enter a username.'
        });
        if (username == null) {
            return;
        }

        const displayName = normalizeInputTextValue(adminUserDisplayNameInput?.value, { collapseWhitespace: true }) || username;
        if (adminUserDisplayNameInput) {
            adminUserDisplayNameInput.value = displayName;
        }

        const role = readSelectedRole(adminUserRoleSelect);
        if (role == null) {
            return;
        }

        const password = readValidatedPasswordInput(adminUserPasswordInput, {
            fieldLabel: 'password',
            requiredMessage: 'Please enter a password.',
            allowEmpty: adminEditingUserId != null
        });
        if (password == null) {
            return;
        }

        const isEditing = Number.isInteger(adminEditingUserId) && adminEditingUserId > 0;
        const endpoint = isEditing ? `/users/${adminEditingUserId}` : '/users';
        const method = isEditing ? 'PUT' : 'POST';

        adminSaveUserBtn.disabled = true;

        apiRequest(endpoint, {
            method,
            body: {
                username,
                display_name: displayName,
                role,
                password
            }
        })
            .then(() => refreshAdminUsersTable())
            .then(() => {
                resetAdminUserForm();
                alert(isEditing ? 'User updated successfully.' : 'User created successfully.');
            })
            .catch((err) => {
                console.error(err);
                alert(err.message || 'Unable to save user.');
            })
            .finally(() => {
                adminSaveUserBtn.disabled = false;
            });
    });
}

if (adminCancelUserEditBtn) {
    adminCancelUserEditBtn.addEventListener('click', () => {
        resetAdminUserForm();
    });
}

const adminUsersTableBody = document.querySelector('#admin-users-table tbody');
if (adminUsersTableBody) {
    adminUsersTableBody.addEventListener('click', (event) => {
        const editButton = event.target.closest('.admin-edit-user-btn');
        if (editButton) {
            loadUserIntoAdminForm(editButton.dataset.id);
            return;
        }

        const deleteButton = event.target.closest('.admin-delete-user-btn');
        if (!deleteButton) return;

        const userId = Number(deleteButton.dataset.id);
        const user = adminUsersData.find((row) => Number(row.user_id) === userId);
        if (!Number.isInteger(userId) || userId <= 0 || !user) {
            return;
        }

        if (!confirm(`Delete user "${user.username}"?`)) {
            return;
        }

        apiRequest(`/users/${userId}`, {
            method: 'DELETE'
        })
            .then(() => refreshAdminUsersTable())
            .then(() => {
                if (Number(adminEditingUserId) === userId) {
                    resetAdminUserForm();
                }
                alert('User deleted successfully.');
            })
            .catch((err) => {
                console.error(err);
                alert(err.message || 'Unable to delete user.');
            });
    });

    loadAdminUsersTable([]);
}

if (adminExportProductsCsvBtn) {
    adminExportProductsCsvBtn.addEventListener('click', () => {
        exportInventoryCatalogCsv('products', adminExportProductsCsvBtn);
    });
}

if (adminImportProductsCsvBtn) {
    adminImportProductsCsvBtn.addEventListener('click', () => {
        importInventoryCatalogCsv('products', adminProductsCsvInput, adminImportProductsCsvBtn, 'Products CSV');
    });
}

if (adminExportPartsCsvBtn) {
    adminExportPartsCsvBtn.addEventListener('click', () => {
        exportInventoryCatalogCsv('parts', adminExportPartsCsvBtn);
    });
}

if (adminImportPartsCsvBtn) {
    adminImportPartsCsvBtn.addEventListener('click', () => {
        importInventoryCatalogCsv('parts', adminPartsCsvInput, adminImportPartsCsvBtn, 'Parts CSV');
    });
}

if (adminExportBomCsvBtn) {
    adminExportBomCsvBtn.addEventListener('click', () => {
        exportInventoryCatalogCsv('bom', adminExportBomCsvBtn);
    });
}

if (adminImportBomCsvBtn) {
    adminImportBomCsvBtn.addEventListener('click', () => {
        importInventoryCatalogCsv('bom', adminBomCsvInput, adminImportBomCsvBtn, 'BOM CSV');
    });
}

if (adminAddProductBtn) {
    // Creates a new product from the admin panel and refreshes all product dropdowns.
    adminAddProductBtn.onclick = function () {
        const productNameInput = document.querySelector('#admin-product-name-input');
        const productName = readValidatedTextInput(productNameInput, {
            fieldLabel: 'Product name',
            requiredMessage: 'Please enter a product name.',
            maxLength: 120
        });
        if (productName == null) {
            return;
        }

        apiRequest('/products', {
            method: 'POST',
            body: { product_name: productName }
        })
        .then(() => refreshProductsDropdowns(true))
        .then(() => {
            productNameInput.value = '';
            alert('Product added successfully.');
        })
        .catch((err) => {
            console.error(err);
            alert('Unable to add product.');
        });
    };
}

// Renders the temporary usage list used while creating a new part and BOM mappings.
function renderAdminPartUsages() {
    const tbody = document.querySelector('#admin-part-usage-table tbody');
    if (!tbody) return;

    if (adminPartUsages.length === 0) {
        tbody.innerHTML = "<tr><td class='no-data' colspan='3'>No product usage added</td></tr>";
        return;
    }

    let html = '';
    adminPartUsages.forEach(({ product_id, product_name, quantity_per_product }) => {
        html += '<tr>';
        html += `<td>${escapeHtml(product_name)}</td>`;
        html += `<td>${quantity_per_product}</td>`;
        html += `<td><button class='admin-remove-usage-btn' type='button' data-product-id='${product_id}'>Remove</button></td>`;
        html += '</tr>';
    });

    tbody.innerHTML = html;
}

// Renders the editable BOM relation list for the currently selected part.
function renderAdminEditPartUsages() {
    const tbody = document.querySelector('#admin-edit-part-usage-table tbody');
    if (!tbody) return;

    if (adminEditPartUsages.length === 0) {
        tbody.innerHTML = "<tr><td class='no-data' colspan='3'>No BOM relations</td></tr>";
        return;
    }

    let html = '';
    adminEditPartUsages.forEach(({ product_id, product_name, quantity_per_product }) => {
        html += '<tr>';
        html += `<td>${escapeHtml(product_name)}</td>`;
        html += `<td>${quantity_per_product}</td>`;
        html += `<td><button class='admin-edit-remove-usage-btn' type='button' data-product-id='${product_id}'>Remove</button></td>`;
        html += '</tr>';
    });

    tbody.innerHTML = html;
}

// Loads existing BOM relations for the selected part into the admin edit table.
function loadSelectedPartBomRelations() {
    const partId = readSelectedPositiveInteger(adminEditPartSelect, 'Please select a part first.');
    if (partId == null) {
        return Promise.resolve();
    }

    return apiRequest(`/parts/${partId}/bom-relations`)
        .then((payload) => {
            const usages = Array.isArray(payload?.data?.usages) ? payload.data.usages : [];
            adminEditPartUsages = usages.map((usage) => ({
                product_id: Number(usage.product_id),
                product_name: usage.product_name || `Product ${usage.product_id}`,
                quantity_per_product: Number(usage.quantity_per_product)
            }));
            renderAdminEditPartUsages();
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to load BOM relations.');
        });
}

// Loads editable detail fields for the selected part in the admin details panel.
function loadSelectedPartDetails() {
    const partId = Number(adminEditPartDetailsSelect?.value);
    if (!Number.isInteger(partId) || partId <= 0) {
        return Promise.resolve();
    }

    return apiRequest(`/parts/${partId}/details`)
        .then((payload) => {
            const details = payload?.data || {};

            const partNameInput = document.querySelector('#admin-edit-part-name-input');
            const tricomaInput = document.querySelector('#admin-edit-part-tricoma-input');
            const reorderLevelInput = document.querySelector('#admin-edit-part-reorder-level-input');
            const reorderQtyInput = document.querySelector('#admin-edit-part-reorder-qty-input');

            if (partNameInput) partNameInput.value = details.part_name ?? '';
            if (tricomaInput) tricomaInput.value = details.tricoma_nr ?? '';
            if (reorderLevelInput) reorderLevelInput.value = details.reorder_level ?? '';
            if (reorderQtyInput) reorderQtyInput.value = details.reorder_quantity ?? '';
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to load part details.');
        });
}

if (adminAddUsageBtn) {
    // Adds or updates a product usage mapping in the in-memory part creation list.
    adminAddUsageBtn.onclick = function () {
        const productSelect = document.querySelector('#admin-usage-product-select');
        const qtyInput = document.querySelector('#admin-usage-qty-input');

        const productId = readSelectedPositiveInteger(productSelect, 'Please select a product for usage mapping.');
        if (productId == null) {
            return;
        }

        const quantity = readValidatedNumberInput(qtyInput, {
            fieldLabel: 'usage quantity per product',
            requiredMessage: 'Please enter a usage quantity per product.',
            invalidMessage: 'Please enter a valid usage quantity per product.',
            positive: true
        });
        if (quantity == null) {
            return;
        }

        const selectedOption = productSelect.options[productSelect.selectedIndex];
        const productName = selectedOption?.textContent || `Product ${productId}`;

        const existingIndex = adminPartUsages.findIndex(({ product_id }) => String(product_id) === String(productId));

        if (existingIndex >= 0) {
            adminPartUsages[existingIndex].quantity_per_product = quantity;
        } else {
            adminPartUsages.push({
                product_id: Number(productId),
                product_name: productName,
                quantity_per_product: quantity
            });
        }

        qtyInput.value = '';
        productSelect.selectedIndex = 0;
        renderAdminPartUsages();
    };
}

const adminUsageTableBody = document.querySelector('#admin-part-usage-table tbody');
if (adminUsageTableBody) {
    // Allows removing staged BOM usage rows before a new part is saved.
    adminUsageTableBody.addEventListener('click', (event) => {
        const button = event.target.closest('.admin-remove-usage-btn');
        if (!button) return;

        const productId = button.dataset.productId;
        adminPartUsages = adminPartUsages.filter(({ product_id }) => String(product_id) !== String(productId));
        renderAdminPartUsages();
    });

    renderAdminPartUsages();
}

const adminEditUsageTableBody = document.querySelector('#admin-edit-part-usage-table tbody');
if (adminEditUsageTableBody) {
    // Allows removing existing BOM rows while editing a part's relations.
    adminEditUsageTableBody.addEventListener('click', (event) => {
        const button = event.target.closest('.admin-edit-remove-usage-btn');
        if (!button) return;

        const productId = button.dataset.productId;
        adminEditPartUsages = adminEditPartUsages.filter(({ product_id }) => String(product_id) !== String(productId));
        renderAdminEditPartUsages();
    });

    renderAdminEditPartUsages();
}

if (adminEditPartSelect) {
    // Changing the selected part reloads its BOM relation set.
    adminEditPartSelect.addEventListener('change', () => {
        if (!adminEditPartSelect.value) {
            adminEditPartUsages = [];
            renderAdminEditPartUsages();
            return;
        }
        loadSelectedPartBomRelations();
    });
}

if (adminEditPartDetailsSelect) {
    adminEditPartDetailsSelect.addEventListener('change', () => {
        const partId = Number(adminEditPartDetailsSelect.value);

        const partNameInput = document.querySelector('#admin-edit-part-name-input');
        const tricomaInput = document.querySelector('#admin-edit-part-tricoma-input');
        const reorderLevelInput = document.querySelector('#admin-edit-part-reorder-level-input');
        const reorderQtyInput = document.querySelector('#admin-edit-part-reorder-qty-input');

        if (!Number.isInteger(partId) || partId <= 0) {
            if (partNameInput) partNameInput.value = '';
            if (tricomaInput) tricomaInput.value = '';
            if (reorderLevelInput) reorderLevelInput.value = '';
            if (reorderQtyInput) reorderQtyInput.value = '';
            return;
        }

        loadSelectedPartDetails();
    });
}

if (adminSavePartDetailsBtn) {
    adminSavePartDetailsBtn.onclick = function () {
        const partId = readSelectedPositiveInteger(adminEditPartDetailsSelect, 'Please select a part to edit.');
        const partNameInput = document.querySelector('#admin-edit-part-name-input');
        const tricomaInput = document.querySelector('#admin-edit-part-tricoma-input');
        const reorderLevelInput = document.querySelector('#admin-edit-part-reorder-level-input');
        const reorderQtyInput = document.querySelector('#admin-edit-part-reorder-qty-input');

        if (partId == null) {
            return;
        }

        const partName = readValidatedTextInput(partNameInput, {
            fieldLabel: 'Part name',
            requiredMessage: 'Please enter a part name.',
            maxLength: 120
        });
        if (partName == null) {
            return;
        }

        const tricomaNr = readValidatedTextInput(tricomaInput, {
            fieldLabel: 'Tricoma number',
            requiredMessage: 'Please enter a Tricoma number.',
            maxLength: 80
        });
        if (tricomaNr == null) {
            return;
        }

        const reorderLevel = readValidatedNumberInput(reorderLevelInput, {
            fieldLabel: 'reorder level',
            requiredMessage: 'Please enter a reorder level.',
            invalidMessage: 'Please enter a valid reorder level.',
            nonNegative: true
        });
        if (reorderLevel == null) {
            return;
        }

        const reorderQuantity = readValidatedNumberInput(reorderQtyInput, {
            fieldLabel: 'reorder quantity',
            requiredMessage: 'Please enter a reorder quantity.',
            invalidMessage: 'Please enter a valid reorder quantity.',
            positive: true
        });
        if (reorderQuantity == null) {
            return;
        }

        apiRequest(`/parts/${partId}/details`, {
            method: 'PUT',
            body: {
                part_name: partName,
                tricoma_nr: tricomaNr,
                reorder_level: reorderLevel,
                reorder_quantity: reorderQuantity
            }
        })
        .then(() => Promise.all([
            refreshDashboardKpis(),
            refreshPartStockTable(),
            refreshPartsDropdowns(true)
        ]))
        .then(() => {
            alert('Part details updated successfully.');
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to update part details.');
        });
    };
}

if (adminEditAddUsageBtn) {
    // Adds or updates a BOM row in the edit list before saving back to the server.
    adminEditAddUsageBtn.onclick = function () {
        const partId = readSelectedPositiveInteger(adminEditPartSelect, 'Select a part before editing BOM relations.');
        if (partId == null) {
            return;
        }

        const productSelect = document.querySelector('#admin-edit-usage-product-select');
        const qtyInput = document.querySelector('#admin-edit-usage-qty-input');

        const productId = readSelectedPositiveInteger(productSelect, 'Please select a product for BOM relation.');
        if (productId == null) {
            return;
        }

        const quantity = readValidatedNumberInput(qtyInput, {
            fieldLabel: 'quantity per product',
            requiredMessage: 'Please enter a quantity per product.',
            invalidMessage: 'Please enter a valid quantity per product.',
            positive: true
        });
        if (quantity == null) {
            return;
        }

        const selectedOption = productSelect.options[productSelect.selectedIndex];
        const productName = selectedOption?.textContent || `Product ${productId}`;

        const existingIndex = adminEditPartUsages.findIndex(({ product_id }) => String(product_id) === String(productId));

        if (existingIndex >= 0) {
            adminEditPartUsages[existingIndex].quantity_per_product = quantity;
            adminEditPartUsages[existingIndex].product_name = productName;
        } else {
            adminEditPartUsages.push({
                product_id: Number(productId),
                product_name: productName,
                quantity_per_product: quantity
            });
        }

        qtyInput.value = '';
        productSelect.selectedIndex = 0;
        renderAdminEditPartUsages();
    };
}

if (adminSavePartBomBtn) {
    // Persists the edited BOM relation list for the selected part.
    adminSavePartBomBtn.onclick = function () {
        const partId = readSelectedPositiveInteger(adminEditPartSelect, 'Please select a part to save BOM relations.');
        if (partId == null) {
            return;
        }

        apiRequest(`/parts/${partId}/bom-relations`, {
            method: 'PUT',
            body: {
                usages: adminEditPartUsages.map(({ product_id, quantity_per_product }) => ({
                    product_id,
                    quantity_per_product
                }))
            }
        })
        .then(() => {
            return Promise.all([
                refreshDashboardKpis(),
                refreshPartStockTable(),
                refreshProductsDropdowns(true)
            ]).then(() => {
                alert('BOM relations updated successfully.');
            });
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to save BOM relations.');
        });
    };
}

if (adminAddPartBtn) {
    // Creates a new part together with its BOM usage mappings.
    adminAddPartBtn.onclick = function () {
        const partNameInput = document.querySelector('#admin-part-name-input');
        const tricomaInput = document.querySelector('#admin-part-tricoma-input');
        const reorderLevelInput = document.querySelector('#admin-part-reorder-level-input');
        const reorderQtyInput = document.querySelector('#admin-part-reorder-qty-input');

        const partName = readValidatedTextInput(partNameInput, {
            fieldLabel: 'Part name',
            requiredMessage: 'Please enter a part name.',
            maxLength: 120
        });
        if (partName == null) {
            return;
        }

        const tricomaNr = readValidatedTextInput(tricomaInput, {
            fieldLabel: 'Tricoma number',
            requiredMessage: 'Please enter a Tricoma number.',
            maxLength: 80
        });
        if (tricomaNr == null) {
            return;
        }

        const reorderLevel = readValidatedNumberInput(reorderLevelInput, {
            fieldLabel: 'reorder level',
            requiredMessage: 'Please enter a reorder level.',
            invalidMessage: 'Please enter a valid reorder level.',
            nonNegative: true
        });
        if (reorderLevel == null) {
            return;
        }

        const reorderQuantity = readValidatedNumberInput(reorderQtyInput, {
            fieldLabel: 'reorder quantity',
            requiredMessage: 'Please enter a reorder quantity.',
            invalidMessage: 'Please enter a valid reorder quantity.',
            positive: true
        });
        if (reorderQuantity == null) {
            return;
        }

        if (adminPartUsages.length === 0) {
            alert('Please add at least one product usage mapping.');
            return;
        }

        apiRequest('/parts', {
            method: 'POST',
            body: {
                part_name: partName,
                tricoma_nr: tricomaNr,
                reorder_level: reorderLevel,
                reorder_quantity: reorderQuantity,
                usages: adminPartUsages.map(({ product_id, quantity_per_product }) => ({
                    product_id,
                    quantity_per_product
                }))
            }
        })
        .then(() => {
            partsCache = null;
            return Promise.all([
                refreshDashboardKpis(),
                refreshPartsDropdowns(true),
                refreshPartStockTable()
            ]).then(() => {
                partNameInput.value = '';
                tricomaInput.value = '';
                reorderLevelInput.value = '';
                reorderQtyInput.value = '';
                adminPartUsages = [];
                renderAdminPartUsages();
                alert('Part added successfully.');
            });
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to add part.');
        });
    };
}

if (adminDeletePartBtn) {
    // Deletes a part and refreshes the related dropdowns and stock table.
    adminDeletePartBtn.onclick = function () {
        const partSelect = document.querySelector('#admin-delete-part-select');
        const partId = readSelectedPositiveInteger(partSelect, 'Please select a part to delete.');
        if (partId == null) {
            return;
        }

        if (!confirm('Delete this part from parts and bom? This cannot be undone.')) {
            return;
        }

        apiRequest(`/parts/${partId}`, {
            method: 'DELETE'
        })
        .then(() => {
            partsCache = null;
            return Promise.all([
                refreshDashboardKpis(),
                refreshPartsDropdowns(true),
                refreshPartStockTable()
            ]).then(() => {
                alert('Part deleted successfully.');
            });
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to delete part.');
        });
    };
}

if (adminDeleteProductBtn) {
    // Deletes a product and refreshes dependent UI state.
    adminDeleteProductBtn.onclick = function () {
        const productSelect = document.querySelector('#admin-delete-product-select');
        const productId = readSelectedPositiveInteger(productSelect, 'Please select a product to delete.');
        if (productId == null) {
            return;
        }

        if (!confirm('Delete this product and its relations in bom and finished_products? This cannot be undone.')) {
            return;
        }

        apiRequest(`/products/${productId}`, {
            method: 'DELETE'
        })
        .then(() => {
            adminPartUsages = adminPartUsages.filter(({ product_id }) => String(product_id) !== String(productId));
            renderAdminPartUsages();
            adminEditPartUsages = adminEditPartUsages.filter(({ product_id }) => String(product_id) !== String(productId));
            renderAdminEditPartUsages();

            productsCache = null;
            return Promise.all([
                refreshDashboardKpis(),
                refreshProductsDropdowns(true),
                refreshFinishedProductsTable(),
                refreshPartStockTable(),
                refreshMostUsedPartsReport(mostUsedPartsYear),
                refreshMostProducedProductsReport(mostProducedProductsYear)
            ]).then(() => {
                alert('Product deleted successfully.');
            });
        })
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to delete product.');
        });
    };
}

// Section 06: Search and Filter Handlers.
const finishedProductSearchInput = document.querySelector('#finished-product-search-input');

// Filters finished products by product name or formatted date.
const searchFinishedProducts = (query) => {
    const trimmed = sanitizeSearchQuery(query);

    if (finishedSearchController) {
        finishedSearchController.abort();
    }
    finishedSearchController = new AbortController();

    const endpoint = trimmed
        ? `/finished-products/search/${encodeURIComponent(trimmed)}`
        : '/finished-products';

    apiRequest(endpoint, { signal: finishedSearchController.signal })
        .then((data) => {
            const rows = Array.isArray(data?.data) ? data.data : [];
            loadFinishedProductsTable(rows);
        })
        .catch((err) => {
            if (err?.name === 'AbortError') return;
            console.error('Failed to search finished products:', err);
        });
};

// Simple debounce helper to avoid firing a search on every keystroke immediately.
const debounce = (fn, delay = 250) => {
    let timer;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), delay);
    };
};

// Filters the current stock table by part id, part name, or Tricoma number.
const searchPartStock = (query) => {
    partStockSearchQuery = sanitizeSearchQuery(query).toLowerCase();
    resetPartStockPagination();
    loadPartStockTable();
};

const debouncedPartStockSearch = debounce((value) => searchPartStock(value));

if (partStockSearchInput) {
    partStockSearchInput.oninput = function () {
        debouncedPartStockSearch(this.value);
    };

    partStockSearchInput.onkeydown = function (event) {
        if (event.key === 'Enter') {
            event.preventDefault();
            searchPartStock(this.value);
        }
    };
}

const debouncedSearch = debounce((value) => searchFinishedProducts(value));

if (finishedProductSearchInput) {
    finishedProductSearchInput.oninput = function () {
        debouncedSearch(this.value);
    };

    finishedProductSearchInput.onkeydown = function (event) {
        if (event.key === 'Enter') {
            event.preventDefault();
            searchFinishedProducts(this.value);
        }
    };
}

// Subsection: Stock movement search and filtering.
const searchStockMovements = (query) => {
    if (stockMovementSearchInput) {
        stockMovementSearchInput.value = sanitizeSearchQuery(query);
    }
    requestStockMovementsWithCurrentFilters();
};

const debouncedStockMovementSearch = debounce((value) => searchStockMovements(value));

if (stockMovementSearchInput) {
    stockMovementSearchInput.oninput = function () {
        debouncedStockMovementSearch(this.value);
    };

    stockMovementSearchInput.onkeydown = function (event) {
        if (event.key === 'Enter') {
            event.preventDefault();
            searchStockMovements(this.value);
        }
    };
}

const searchAppLogs = (query) => {
    if (appLogSearchInput) {
        appLogSearchInput.value = sanitizeSearchQuery(query);
    }
    requestAppLogsWithCurrentFilters();
};

const debouncedAppLogSearch = debounce((value) => searchAppLogs(value));

if (appLogSearchInput) {
    appLogSearchInput.oninput = function () {
        debouncedAppLogSearch(this.value);
    };

    appLogSearchInput.onkeydown = function (event) {
        if (event.key === 'Enter') {
            event.preventDefault();
            searchAppLogs(this.value);
        }
    };
}

[
    stockMovementTypeFilterSelect,
    stockMovementUserFilterSelect,
    stockMovementProductFilterSelect,
    stockMovementDateFromInput,
    stockMovementDateToInput
].filter(Boolean).forEach((element) => {
    element.addEventListener('change', () => {
        requestStockMovementsWithCurrentFilters();
    });
});

[
    appLogLevelFilterSelect,
    appLogDateFromInput,
    appLogDateToInput
].filter(Boolean).forEach((element) => {
    element.addEventListener('change', () => {
        requestAppLogsWithCurrentFilters();
    });
});

if (stockMovementResetFiltersBtn) {
    stockMovementResetFiltersBtn.addEventListener('click', () => {
        stockMovementFilters = {
            query: '',
            movement_type: '',
            created_by: '',
            source_product_name: '',
            date_from: '',
            date_to: ''
        };

        if (stockMovementSearchInput) stockMovementSearchInput.value = '';
        if (stockMovementTypeFilterSelect) stockMovementTypeFilterSelect.value = '';
        if (stockMovementUserFilterSelect) stockMovementUserFilterSelect.value = '';
        if (stockMovementProductFilterSelect) stockMovementProductFilterSelect.value = '';
        if (stockMovementDateFromInput) stockMovementDateFromInput.value = '';
        if (stockMovementDateToInput) stockMovementDateToInput.value = '';

        requestStockMovementsWithCurrentFilters();
    });
}

if (appLogResetFiltersBtn) {
    appLogResetFiltersBtn.addEventListener('click', () => {
        appLogFilters = {
            query: '',
            level: '',
            date_from: '',
            date_to: ''
        };

        if (appLogSearchInput) appLogSearchInput.value = '';
        if (appLogLevelFilterSelect) appLogLevelFilterSelect.value = '';
        if (appLogDateFromInput) appLogDateFromInput.value = '';
        if (appLogDateToInput) appLogDateToInput.value = '';

        requestAppLogsWithCurrentFilters();
    });
}

if (appLogRefreshBtn) {
    appLogRefreshBtn.addEventListener('click', () => {
        requestAppLogsWithCurrentFilters();
    });
}

function requestMostUsedPartsReportFromInput() {
    const year = readValidatedYearInput(mostUsedPartsYearInput, 'year');
    if (year == null) {
        return;
    }

    refreshMostUsedPartsReport(year)
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to load most-used parts.');
        });
}

function requestMostProducedProductsReportFromInput() {
    const year = readValidatedYearInput(mostProducedProductsYearInput, 'year');
    if (year == null) {
        return;
    }

    refreshMostProducedProductsReport(year)
        .catch((err) => {
            console.error(err);
            alert(err.message || 'Unable to load most-produced products.');
        });
}

if (mostUsedPartsApplyBtn) {
    mostUsedPartsApplyBtn.addEventListener('click', requestMostUsedPartsReportFromInput);
}

if (mostUsedPartsYearInput) {
    mostUsedPartsYearInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            requestMostUsedPartsReportFromInput();
        }
    });
}

if (mostProducedProductsApplyBtn) {
    mostProducedProductsApplyBtn.addEventListener('click', requestMostProducedProductsReportFromInput);
}

if (mostProducedProductsYearInput) {
    mostProducedProductsYearInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            requestMostProducedProductsReportFromInput();
        }
    });
}

// Section 07: Dropdown Population Helpers.
// Populates every product-related dropdown from a single shared product list.
// This keeps admin forms, planner forms, and stock filters in sync after refreshes.
function loadProductDropdown(data) {
    if (!Array.isArray(data)) return;

    // The old manual Finished Products add form was removed, so this helper now
    // only maintains the remaining operational/admin product selectors.
    const usageSelect = document.querySelector('#admin-usage-product-select');
    const adminDeleteSelect = document.querySelector('#admin-delete-product-select');
    const adminEditUsageSelect = document.querySelector('#admin-edit-usage-product-select');
    const currentStockProductSelect = document.querySelector('#current-stock-product-filter-select');
    const productionPlannerSelect = document.querySelector('#production-planner-product-select');
    const productionScheduleSelect = document.querySelector('#production-schedule-product-select');
    const previousCurrentStockProductId = partStockProductId ? String(partStockProductId) : '';
    const previousProductionPlannerProductId = productionPlannerSelect?.value || '';
    const previousProductionScheduleProductId = productionScheduleSelect?.value || '';

    if (!usageSelect && !adminDeleteSelect && !adminEditUsageSelect && !currentStockProductSelect && !productionPlannerSelect && !productionScheduleSelect) return;

    const populateSelect = (select, placeholderText) => {
        if (!select) return;

        select.innerHTML = '';

        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = placeholderText;
        placeholder.disabled = true;
        placeholder.selected = true;
        select.appendChild(placeholder);

        data.forEach(({ product_id, product_name }) => {
            const option = document.createElement('option');
            option.value = product_id;
            option.textContent = product_name;
            select.appendChild(option);
        });
    };

    populateSelect(usageSelect, '-- Select product for usage --');
    populateSelect(adminDeleteSelect, '-- Select product to delete --');
    populateSelect(adminEditUsageSelect, '-- Select product for BOM edit --');
    populateSelect(currentStockProductSelect, '-- Filter part stock by product --');
    populateSelect(productionPlannerSelect, '-- Select a product to plan --');
    populateSelect(productionScheduleSelect, '-- Select a product to schedule --');

    if (currentStockProductSelect && partStockProductId) {
        const hasSelected = data.some(({ product_id }) => Number(product_id) === Number(partStockProductId));
        if (hasSelected) {
            currentStockProductSelect.value = previousCurrentStockProductId;
        } else {
            clearCurrentStockProductFilter();
        }
    }

    if (productionPlannerSelect && previousProductionPlannerProductId) {
        const hasSelected = data.some(({ product_id }) => Number(product_id) === Number(previousProductionPlannerProductId));
        if (hasSelected) {
            productionPlannerSelect.value = previousProductionPlannerProductId;
        } else {
            resetProductionPlanner();
        }
    }

    if (productionScheduleSelect && previousProductionScheduleProductId) {
        const hasSelected = data.some(({ product_id }) => Number(product_id) === Number(previousProductionScheduleProductId));
        productionScheduleSelect.value = hasSelected ? previousProductionScheduleProductId : '';
    }
}

// Populates all part selectors used outside the main stock table.
// These controls live in different panels but all depend on the same part catalog.
function loadPartDropdown(data) {
    if (!Array.isArray(data)) return;

    const movementSelect = document.querySelector('#movement-part-select');
    const adminDeleteSelect = document.querySelector('#admin-delete-part-select');
    const adminEditSelect = document.querySelector('#admin-edit-part-select');
    const adminEditDetailsSelect = document.querySelector('#admin-edit-part-details-select');

    if (!movementSelect && !adminDeleteSelect && !adminEditSelect && !adminEditDetailsSelect) return;

    const populateSelect = (select, placeholderText) => {
        if (!select) return;

        select.innerHTML = '';

        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = placeholderText;
        placeholder.disabled = true;
        placeholder.selected = true;
        select.appendChild(placeholder);

        data.forEach(({ part_id, part_name }) => {
            const option = document.createElement('option');
            option.value = part_id;
            option.textContent = part_name;
            select.appendChild(option);
        });
    };

    populateSelect(movementSelect, '-- Select a part --');
    populateSelect(adminDeleteSelect, '-- Select part to delete --');
    populateSelect(adminEditSelect, '-- Select part to edit BOM --');
    populateSelect(adminEditDetailsSelect, '-- Select part to edit details --');
}

// Section 08: Finished Products Table Helpers.
// Sends a DELETE request to remove a finished product record.
function deleteFinishedProduct(finProductId) {
    return apiRequest(`/finished-products/${finProductId}`, {
        method: 'DELETE'
    });
}

// Renders the finished products table from cached or freshly fetched data.
function loadFinishedProductsTable(data) {
    // Keep a copy of the latest fetched data so sorting/filtering can be re-applied.
    if (data) {
        finishedProductsData = data;
    }

    const table = document.querySelector('#finished-products-table tbody');
    const sortedData = sortData(finishedProductsData, finishedProductsSort.key, finishedProductsSort.direction);

    if (sortedData.length === 0) {
        table.innerHTML = "<tr><td class='no-data' colspan='9'>No results</td></tr>";
        return;
    }

    table.innerHTML = '';
    const fragment = document.createDocumentFragment();

    sortedData.forEach(function ({fin_product_id, product_name, fin_product_qty, fin_product_date, production_schedule_id, due_date, comments, created_by}, idx) {
        const tr = document.createElement('tr');

        const noCell = document.createElement('td');
        noCell.textContent = String(idx + 1);
        tr.appendChild(noCell);

        const productCell = document.createElement('td');
        productCell.textContent = String(product_name ?? '');
        tr.appendChild(productCell);

        const qtyCell = document.createElement('td');
        qtyCell.textContent = String(fin_product_qty ?? '');
        tr.appendChild(qtyCell);

        const dateCell = document.createElement('td');
        dateCell.textContent = formatDateDDMMYY(fin_product_date);
        tr.appendChild(dateCell);

        const scheduleCell = document.createElement('td');
        scheduleCell.textContent = production_schedule_id ? `#${production_schedule_id}` : 'Legacy';
        tr.appendChild(scheduleCell);

        const dueDateCell = document.createElement('td');
        dueDateCell.textContent = due_date ? formatDateDDMMYY(due_date) : '-';
        tr.appendChild(dueDateCell);

        const commentsCell = document.createElement('td');
        commentsCell.textContent = comments ? getProductionScheduleCommentPreview(comments, 90) : '-';
        commentsCell.className = 'production-schedule-comments-cell';
        tr.appendChild(commentsCell);

        const createdByCell = document.createElement('td');
        createdByCell.textContent = created_by || '-';
        tr.appendChild(createdByCell);

        const actionCell = document.createElement('td');
        if (isAdminUser()) {
            const button = document.createElement('button');
            button.className = 'delete-finished-btn';
            button.dataset.id = String(fin_product_id);
            button.textContent = 'Delete';
            actionCell.appendChild(button);
        } else {
            actionCell.textContent = '-';
        }
        tr.appendChild(actionCell);

        fragment.appendChild(tr);
    });

    table.appendChild(fragment);
}

// Sets up event delegation for the finished products delete buttons.
// This keeps the handler attached once even when the table is re-rendered.
function setupFinishedProductsDeleteHandler() {
    const tbody = document.querySelector('#finished-products-table tbody');
    if (!tbody) return;

    tbody.addEventListener('click', (event) => {
        const button = event.target.closest('.delete-finished-btn');
        if (!button) return;

        const finProductId = button.dataset.id;
        if (!finProductId) return;

        if (!confirm('Delete this finished product entry?')) return;

        deleteFinishedProduct(finProductId)
            .then(() => {
                // Refresh the views that depend on finished product data.
                return Promise.all([
                    refreshDashboardKpis(),
                    refreshFinishedProductsTable(),
                    refreshProductionSchedule(),
                    refreshStockMovementsTable(),
                    refreshPartStockTable(),
                    refreshMostUsedPartsReport(mostUsedPartsYear),
                    refreshMostProducedProductsReport(mostProducedProductsYear)
                ]);
            })
            .catch((err) => console.error(err));
    });
}

function deleteProductionScheduleOrderRequest(productionScheduleId) {
    return apiRequest(`/production-schedule/${productionScheduleId}`, {
        method: 'DELETE'
    });
}

function completeProductionScheduleOrderRequest(productionScheduleId) {
    return apiRequest(`/production-schedule/${productionScheduleId}/complete`, {
        method: 'POST'
    });
}

function setupProductionScheduleTableHandler() {
    const pendingTbody = document.querySelector('#production-schedule-table tbody');
    if (!pendingTbody) return;

    const handleRowSelection = (event) => {
        const row = event.target.closest('tr[data-id]');
        if (!row) {
            return;
        }

        selectProductionScheduleOrder(row.dataset.id);
    };

    if (pendingTbody) {
        pendingTbody.addEventListener('click', (event) => {
        // Completing a scheduled order creates the finished-product record and
        // removes that row from the pending queue.
        const completeButton = event.target.closest('.production-schedule-complete-btn');
        if (completeButton) {
            event.stopPropagation();

            const productionScheduleId = completeButton.dataset.id;
            if (!productionScheduleId) return;

            if (!confirm('Complete this scheduled production and create a finished product entry?')) return;

            completeProductionScheduleOrderRequest(productionScheduleId)
                .then(async () => {
                    resetProductionSchedulePagination();
                    resetProductionScheduleAllocationPagination();
                    selectedProductionScheduleId = Number(productionScheduleId);

                    const refreshJobs = [
                        refreshProductionSchedule(),
                        refreshDashboardKpis(),
                        refreshStockMovementsTable(),
                        refreshPartStockTable(),
                        refreshMostUsedPartsReport(mostUsedPartsYear),
                        refreshMostProducedProductsReport(mostProducedProductsYear)
                    ];

                    if (isAdminUser()) {
                        refreshJobs.push(refreshFinishedProductsTable());
                    }

                    await Promise.all(refreshJobs);
                })
                .catch((err) => {
                    console.error(err);
                    alert(err.message || 'Unable to complete this scheduled production.');
                });
            return;
        }

        const deleteButton = event.target.closest('.production-schedule-delete-btn');
        if (deleteButton) {
            event.stopPropagation();

            const productionScheduleId = deleteButton.dataset.id;
            if (!productionScheduleId) return;

            if (!confirm('Remove this planned production from the schedule?')) return;

            deleteProductionScheduleOrderRequest(productionScheduleId)
                .then(() => {
                    resetProductionSchedulePagination();
                    resetProductionScheduleAllocationPagination();

                    if (Number(selectedProductionScheduleId) === Number(productionScheduleId)) {
                        selectedProductionScheduleId = null;
                    }

                    return refreshProductionSchedule();
                })
                .catch((err) => {
                    console.error(err);
                    alert(err.message || 'Unable to remove this planned production.');
                });
            return;
        }

        handleRowSelection(event);
        });
    }
}

// Section 09: Stock Movement Table Helpers.
// Sends a DELETE request to remove a stock movement record.
function deleteStockMovement(stockMovementId) {
    return apiRequest(`/stock-movements/${stockMovementId}`, {
        method: 'DELETE'
    });
}

// Sets up event delegation for the stock movement delete buttons.
function setupStockMovementDeleteHandler() {
    const tbody = document.querySelector('#stock-movement-table tbody');
    if (!tbody) return;

    tbody.addEventListener('click', (event) => {
        const button = event.target.closest('.delete-stockmovement-btn');
        if (!button) return;

        const stockMovementId = button.dataset.id;
        if (!stockMovementId) return;

        if (!confirm('Delete this stock movement entry?')) return;

        deleteStockMovement(stockMovementId)
            .then(() => {
                return Promise.all([
                    refreshDashboardKpis(),
                    refreshStockMovementsTable(),
                    refreshPartStockTable()
                ]);
            })
            .catch((err) => console.error(err));
    });
}

function loadStockMovementTable(data) {
    // Keep a copy of the latest fetched data so sorting/filtering can be re-applied.
    if (data) {
        // The backend may return manual stock movements or derived production-consumption
        // rows. Normalize them into one shape so rendering and sorting stay simple.
        stockMovementsData = data.map((row) => {
            const qty = Number(row.stock_movement_qty);
            let movementType = String(row.movement_type || '').trim();

            if (!movementType) {
                movementType = '0';
                if (qty > 0) movementType = 'IN';
                if (qty < 0) movementType = 'OUT';
            }

            return {
                ...row,
                stock_movement_id: Number(row.stock_movement_id ?? 0),
                stock_movement_qty: qty,
                movement_type: movementType,
                transaction_source: String(row.transaction_source || 'movement'),
                source_product_name: String(row.source_product_name || ''),
                can_delete: Boolean(row.can_delete)
            };
        });
    }

    updateStockMovementExportButtons();

    const table = document.querySelector('#stock-movement-table tbody');
    const sortedData = sortData(stockMovementsData, stockMovementsSort.key, stockMovementsSort.direction);
    const totalItems = sortedData.length;
    // Show a no-results row when sorting produces an empty table.
    if (totalItems === 0) {
        table.innerHTML = "<tr><td class='no-data' colspan='8'>No results</td></tr>";
        renderStockMovementPagination(0, 0, 0, 0);
        return;
    }

    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    stockMovementsPage = Math.min(Math.max(stockMovementsPage, 1), totalPages);
    const startIndex = (stockMovementsPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = sortedData.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    table.innerHTML = '';
    const fragment = document.createDocumentFragment();

    // Build table rows with a delete action button.
    paginatedData.forEach(function ({stock_movement_id, part_name, stock_movement_qty, movement_type, created_by, stock_movement_date, transaction_source, source_product_name, can_delete}, idx) {
        const tr = document.createElement('tr');

        const noCell = document.createElement('td');
        noCell.textContent = String(startIndex + idx + 1);
        tr.appendChild(noCell);

        const partCell = document.createElement('td');
        partCell.textContent = String(part_name ?? '');
        if (transaction_source === 'assembly' && source_product_name) {
            partCell.title = `Used in ${source_product_name}`;
        }
        tr.appendChild(partCell);

        const qtyCell = document.createElement('td');
        qtyCell.textContent = formatPartQuantityForDisplay(stock_movement_qty, { part_name });
        tr.appendChild(qtyCell);

        const typeCell = document.createElement('td');
        typeCell.textContent = String(movement_type ?? '');
        tr.appendChild(typeCell);

        const userCell = document.createElement('td');
        userCell.textContent = String(created_by ?? '');
        tr.appendChild(userCell);

        const productCell = document.createElement('td');
        productCell.textContent = String(source_product_name || '-');
        if (source_product_name) {
            productCell.title = String(source_product_name);
        }
        tr.appendChild(productCell);

        const dateCell = document.createElement('td');
        dateCell.textContent = formatDateDDMMYY(stock_movement_date);
        tr.appendChild(dateCell);

        const actionCell = document.createElement('td');
        if (isAdminUser() && can_delete) {
            const button = document.createElement('button');
            button.className = 'delete-stockmovement-btn';
            button.dataset.id = String(stock_movement_id);
            button.textContent = 'Delete';
            actionCell.appendChild(button);
        } else {
            actionCell.textContent = '-';
        }
        tr.appendChild(actionCell);

        fragment.appendChild(tr);
    });

    // Insert the generated rows into the table body.
    table.appendChild(fragment);
    renderStockMovementPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

function renderMostUsedPartsSummary() {
    if (mostUsedPartsYearInput) {
        mostUsedPartsYearInput.value = String(mostUsedPartsMeta.year || mostUsedPartsYear);
    }

    if (mostUsedPartsTopCaption) {
        mostUsedPartsTopCaption.textContent = `Highest-consumption individual parts in ${mostUsedPartsMeta.year || mostUsedPartsYear}.`;
    }

    const topTenParts = mostUsedPartsData.slice(0, 10);

    if (mostUsedPartsTopCount) {
        mostUsedPartsTopCount.textContent = String(topTenParts.length);
    }

    if (mostUsedPartsTopList) {
        mostUsedPartsTopList.innerHTML = '';

        if (topTenParts.length === 0) {
            const emptyState = document.createElement('div');
            emptyState.className = 'buildable-limiting-empty';
            emptyState.textContent = `No part usage data for ${mostUsedPartsMeta.year || mostUsedPartsYear}`;
            mostUsedPartsTopList.appendChild(emptyState);
        } else {
            const fragment = document.createDocumentFragment();

            topTenParts.forEach(({ part_id, part_name, tricoma_nr, total_quantity_used }, index) => {
                const item = document.createElement('div');
                item.className = 'buildable-limiting-item';

                const rank = document.createElement('span');
                rank.className = 'buildable-limiting-index';
                rank.textContent = String(index + 1).padStart(2, '0');
                item.appendChild(rank);

                const body = document.createElement('div');
                body.className = 'buildable-limiting-body';

                const title = document.createElement('strong');
                title.className = 'buildable-limiting-name';
                title.textContent = String(part_name ?? '');
                body.appendChild(title);

                const meta = document.createElement('span');
                meta.className = 'buildable-limiting-meta';
                meta.textContent = `Total used: ${formatPartQuantityForDisplay(total_quantity_used, { part_id, part_name, tricoma_nr })}`;
                body.appendChild(meta);

                item.appendChild(body);
                fragment.appendChild(item);
            });

            mostUsedPartsTopList.appendChild(fragment);
        }
    }

    if (mostUsedPartsTotalTypes) {
        mostUsedPartsTotalTypes.textContent = formatNumberForDisplay(mostUsedPartsMeta.total_distinct_parts);
    }
}

function renderMostUsedPartsPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (mostUsedPartsPageSummary) {
        if (totalItems <= 0) {
            mostUsedPartsPageSummary.textContent = 'Showing 0 parts';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            mostUsedPartsPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} parts`;
        }
    }

    if (!mostUsedPartsPaginationControls) {
        return;
    }

    mostUsedPartsPaginationControls.innerHTML = '';
    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = 'part-stock-page-button';

        if (nav) {
            button.classList.add('is-nav');
        }

        if (active) {
            button.classList.add('is-active');
            button.setAttribute('aria-current', 'page');
        }

        button.disabled = disabled;
        button.addEventListener('click', () => {
            if (disabled || active || pageNumber === mostUsedPartsPage) {
                return;
            }

            mostUsedPartsPage = pageNumber;
            renderMostUsedPartsTable();
        });

        fragment.appendChild(button);
    };

    appendButton('Previous', mostUsedPartsPage - 1, {
        disabled: mostUsedPartsPage <= 1,
        nav: true
    });

    const maxVisiblePageButtons = 5;
    const firstPage = Math.max(1, Math.min(mostUsedPartsPage - 2, totalPages - maxVisiblePageButtons + 1));
    const lastPage = Math.min(totalPages, firstPage + maxVisiblePageButtons - 1);

    for (let pageNumber = firstPage; pageNumber <= lastPage; pageNumber += 1) {
        appendButton(String(pageNumber), pageNumber, {
            active: pageNumber === mostUsedPartsPage
        });
    }

    appendButton('Next', mostUsedPartsPage + 1, {
        disabled: mostUsedPartsPage >= totalPages,
        nav: true
    });

    mostUsedPartsPaginationControls.appendChild(fragment);
}

function renderMostUsedPartsTable() {
    const table = document.querySelector('#most-used-parts-table tbody');
    if (!table) {
        return;
    }

    const totalItems = mostUsedPartsData.length;
    if (totalItems === 0) {
        table.innerHTML = `<tr><td class='no-data' colspan='5'>No part usage data for ${mostUsedPartsMeta.year || mostUsedPartsYear}</td></tr>`;
        renderMostUsedPartsPagination(0, 0, 0, 0);
        return;
    }

    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    mostUsedPartsPage = Math.min(Math.max(mostUsedPartsPage, 1), totalPages);
    const startIndex = (mostUsedPartsPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = mostUsedPartsData.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    table.innerHTML = '';
    const fragment = document.createDocumentFragment();

    paginatedData.forEach(({ part_id, part_name, tricoma_nr, total_quantity_used, production_runs }, idx) => {
        const tr = document.createElement('tr');

        const noCell = document.createElement('td');
        noCell.textContent = String(startIndex + idx + 1);
        tr.appendChild(noCell);

        const partCell = document.createElement('td');
        partCell.textContent = String(part_name ?? '');
        tr.appendChild(partCell);

        const tricomaCell = document.createElement('td');
        tricomaCell.textContent = String(tricoma_nr ?? '-');
        tr.appendChild(tricomaCell);

        const totalCell = document.createElement('td');
        totalCell.textContent = formatPartQuantityForDisplay(total_quantity_used, { part_id, part_name, tricoma_nr });
        tr.appendChild(totalCell);

        const runsCell = document.createElement('td');
        runsCell.textContent = formatNumberForDisplay(production_runs);
        tr.appendChild(runsCell);

        fragment.appendChild(tr);
    });

    table.appendChild(fragment);
    renderMostUsedPartsPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

function renderMostProducedProductsSummary() {
    if (mostProducedProductsYearInput) {
        mostProducedProductsYearInput.value = String(mostProducedProductsMeta.year || mostProducedProductsYear);
    }

    if (mostProducedProductsTotalProduced) {
        mostProducedProductsTotalProduced.textContent = formatNumberForDisplay(mostProducedProductsMeta.total_quantity_produced);
    }

    if (mostProducedProductsTotalTypes) {
        mostProducedProductsTotalTypes.textContent = formatNumberForDisplay(mostProducedProductsMeta.total_distinct_products);
    }

    if (mostProducedProductsTotalCaption) {
        mostProducedProductsTotalCaption.textContent = `Total finished-product units produced in ${mostProducedProductsMeta.year || mostProducedProductsYear}.`;
    }
}

function renderMostProducedProductsPagination(totalItems, totalPages, startIndex, visibleCount) {
    if (mostProducedProductsPageSummary) {
        if (totalItems <= 0) {
            mostProducedProductsPageSummary.textContent = 'Showing 0 products';
        } else {
            const firstItem = startIndex + 1;
            const lastItem = startIndex + visibleCount;
            mostProducedProductsPageSummary.textContent = `Showing ${firstItem}-${lastItem} of ${totalItems} products`;
        }
    }

    if (!mostProducedProductsPaginationControls) {
        return;
    }

    mostProducedProductsPaginationControls.innerHTML = '';
    if (totalPages <= 1) {
        return;
    }

    const fragment = document.createDocumentFragment();
    const appendButton = (label, pageNumber, options = {}) => {
        const { disabled = false, active = false, nav = false } = options;
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.className = 'part-stock-page-button';

        if (nav) {
            button.classList.add('is-nav');
        }

        if (active) {
            button.classList.add('is-active');
            button.setAttribute('aria-current', 'page');
        }

        button.disabled = disabled;
        button.addEventListener('click', () => {
            if (disabled || active || pageNumber === mostProducedProductsPage) {
                return;
            }

            mostProducedProductsPage = pageNumber;
            renderMostProducedProductsTable();
        });

        fragment.appendChild(button);
    };

    appendButton('Previous', mostProducedProductsPage - 1, {
        disabled: mostProducedProductsPage <= 1,
        nav: true
    });

    const maxVisiblePageButtons = 5;
    const firstPage = Math.max(1, Math.min(mostProducedProductsPage - 2, totalPages - maxVisiblePageButtons + 1));
    const lastPage = Math.min(totalPages, firstPage + maxVisiblePageButtons - 1);

    for (let pageNumber = firstPage; pageNumber <= lastPage; pageNumber += 1) {
        appendButton(String(pageNumber), pageNumber, {
            active: pageNumber === mostProducedProductsPage
        });
    }

    appendButton('Next', mostProducedProductsPage + 1, {
        disabled: mostProducedProductsPage >= totalPages,
        nav: true
    });

    mostProducedProductsPaginationControls.appendChild(fragment);
}

function renderMostProducedProductsTable() {
    const table = document.querySelector('#most-produced-products-table tbody');
    if (!table) {
        return;
    }

    const totalItems = mostProducedProductsData.length;
    if (totalItems === 0) {
        table.innerHTML = `<tr><td class='no-data' colspan='4'>No production data for ${mostProducedProductsMeta.year || mostProducedProductsYear}</td></tr>`;
        renderMostProducedProductsPagination(0, 0, 0, 0);
        return;
    }

    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    mostProducedProductsPage = Math.min(Math.max(mostProducedProductsPage, 1), totalPages);
    const startIndex = (mostProducedProductsPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = mostProducedProductsData.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    table.innerHTML = '';
    const fragment = document.createDocumentFragment();

    paginatedData.forEach(({ product_name, total_quantity_produced, production_runs }, idx) => {
        const tr = document.createElement('tr');

        const noCell = document.createElement('td');
        noCell.textContent = String(startIndex + idx + 1);
        tr.appendChild(noCell);

        const productCell = document.createElement('td');
        productCell.textContent = String(product_name ?? '');
        tr.appendChild(productCell);

        const totalCell = document.createElement('td');
        totalCell.textContent = formatNumberForDisplay(total_quantity_produced);
        tr.appendChild(totalCell);

        const runsCell = document.createElement('td');
        runsCell.textContent = formatNumberForDisplay(production_runs);
        tr.appendChild(runsCell);

        fragment.appendChild(tr);
    });

    table.appendChild(fragment);
    renderMostProducedProductsPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

// Section 10: Current Stock Table Helpers.
// Renders the current stock table, applying the selected product filter,
// optional low-stock filter, and current sort state.
function loadPartStockTable(data) {
    if (data) {
        partStockData = data;
        updatePartStockExportButton();

        if (partStockProductId) {
            // Defer rendering to the buildability callback so the table is only
            // drawn once with fully updated filter state.
            refreshSelectedProductBuildability({ silent: true, rerenderTable: true });
            return;
        }
    }

    const table = document.querySelector('#part-stock-table tbody');
    // Apply the product-part filter first when a product is selected.
    let filteredData = partStockData;
    if (partStockProductId) {
        filteredData = filteredData.filter(({ part_id }) => partStockRequiredPartIds.has(Number(part_id)));
    }

    // Apply the low-stock filter when enabled.
    if (partStockFilterLow) {
        filteredData = filteredData.filter(({ tracked_stock, reorder_level }) => Number(tracked_stock) <= Number(reorder_level));
    }

    // Apply text search by id, part name, or Tricoma number.
    if (partStockSearchQuery) {
        filteredData = filteredData.filter(({ part_id, part_name, tricoma_nr }) => {
            const searchable = [part_id, part_name, tricoma_nr]
                .map((value) => String(value == null ? '' : value).toLowerCase())
                .join(' ');

            return searchable.includes(partStockSearchQuery);
        });
    }

    // Sort the filtered data according to the current sort state.
    const sortedData = sortData(filteredData, partStockSort.key, partStockSort.direction);
    const totalItems = sortedData.length;

    // Show a no-results row when filtering removes every item.
    if (totalItems === 0) {
        const emptyMessage = partStockFilterLow && !partStockSearchQuery && !partStockProductId
            ? 'No low-stock parts'
            : 'No results';

        table.innerHTML = `<tr><td class='no-data' colspan='8'>${emptyMessage}</td></tr>`;
        renderPartStockPagination(0, 0, 0, 0);
        return;
    }

    const totalPages = Math.max(1, Math.ceil(totalItems / PART_STOCK_PAGE_SIZE));
    partStockPage = Math.min(Math.max(partStockPage, 1), totalPages);
    const startIndex = (partStockPage - 1) * PART_STOCK_PAGE_SIZE;
    const paginatedData = sortedData.slice(startIndex, startIndex + PART_STOCK_PAGE_SIZE);

    // Build table rows and highlight any low-stock entries.
    table.innerHTML = '';
    const fragment = document.createDocumentFragment();

    // Mark rows as low stock when tracked stock falls below the reorder level.
    paginatedData.forEach(function ({part_id, part_name, tricoma_nr, tracked_stock, total_movements, total_used_in_assembly, reorder_level, reorder_quantity}) {
        const lowStock = Number(tracked_stock) <= Number(reorder_level);
        const tr = document.createElement('tr');
        if (lowStock) {
            tr.className = 'low-stock';
        }
        const partIdentity = { part_id, part_name, tricoma_nr };

        const idCell = document.createElement('td');
        idCell.textContent = String(part_id ?? '');
        tr.appendChild(idCell);

        const nameCell = document.createElement('td');
        nameCell.textContent = String(part_name ?? '');
        tr.appendChild(nameCell);

        const trackedCell = document.createElement('td');
        trackedCell.textContent = formatPartQuantityForDisplay(tracked_stock, partIdentity);
        tr.appendChild(trackedCell);

        const reorderLevelCell = document.createElement('td');
        reorderLevelCell.textContent = formatPartQuantityForDisplay(reorder_level, partIdentity);
        tr.appendChild(reorderLevelCell);

        const reorderQtyCell = document.createElement('td');
        reorderQtyCell.textContent = formatPartQuantityForDisplay(reorder_quantity, partIdentity);
        tr.appendChild(reorderQtyCell);

        const tricomaCell = document.createElement('td');
        tricomaCell.textContent = String(tricoma_nr ?? '');
        tr.appendChild(tricomaCell);

        const movementCell = document.createElement('td');
        movementCell.textContent = formatPartQuantityForDisplay(total_movements, partIdentity);
        tr.appendChild(movementCell);

        const usedCell = document.createElement('td');
        usedCell.textContent = formatPartQuantityForDisplay(total_used_in_assembly, partIdentity);
        tr.appendChild(usedCell);

        fragment.appendChild(tr);
    });

    // Insert the generated rows into the table body.
    table.appendChild(fragment);
    renderPartStockPagination(totalItems, totalPages, startIndex, paginatedData.length);
}

// Section 11: Tab Switching.
// Collect tab button controls and content panels.
const tabButtons = document.querySelectorAll('.tab-button');
const tabPanels = document.querySelectorAll('.tab-panel');

function activateTabPanel(targetPanelName) {
    if (!canAccessPanel(targetPanelName)) {
        return;
    }

    activePanelName = targetPanelName;

    tabButtons.forEach((button) => {
        const buttonPanelName = button.dataset.panelTarget || button.getAttribute('aria-controls');
        const isActive = buttonPanelName === targetPanelName;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', String(isActive));
    });

    // Inventory Setup intentionally spans multiple blocks, so activation is
    // keyed off the shared logical `data-panel` name rather than a single node.
    tabPanels.forEach((panel) => {
        const panelName = panel.dataset.panel || panel.id;
        const isActive = panelName === targetPanelName;
        panel.classList.toggle('active', isActive);
        panel.hidden = !isActive;
    });

    if (targetPanelName === 'dashboard' && currentUser && !isPasswordChangeRequired()) {
        refreshDashboardKpis().catch((error) => {
            console.error('Failed to refresh dashboard KPIs:', error);
        });
    }

    if (targetPanelName === 'production-schedule' && currentUser && !isPasswordChangeRequired()) {
        refreshProductionSchedule().catch((error) => {
            console.error('Failed to refresh production schedule:', error);
        });
    }

    if (targetPanelName === 'inventory-setup' && currentUser && !isPasswordChangeRequired()) {
        Promise.all([
            refreshProductsDropdowns(true),
            refreshPartsDropdowns(true)
        ]).catch((error) => {
            console.error('Failed to refresh inventory setup data:', error);
        });
    }
}

// Activate the matching panel group when a tab button is clicked.
tabButtons.forEach((button) => {
    button.addEventListener('click', () => {
        const targetPanelName = button.dataset.panelTarget || button.getAttribute('aria-controls');
        activateTabPanel(targetPanelName);
    });
});
