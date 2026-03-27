const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const BASE_URL = process.env.SMOKE_TEST_BASE_URL || 'http://localhost:5555';
const ADMIN_USERNAME = process.env.SMOKE_TEST_ADMIN_USERNAME || 'admin';
const ADMIN_PASSWORD = process.env.SMOKE_TEST_ADMIN_PASSWORD || 'Admin123!';
const POWERMOON_USERNAME = process.env.SMOKE_TEST_POWERMOON_USERNAME || 'powermoon';
const POWERMOON_PASSWORD = process.env.SMOKE_TEST_POWERMOON_PASSWORD || 'Powermoon123!';

function buildUrl(relativePath) {
    return new URL(relativePath, BASE_URL).toString();
}

function parseHeaderBlock(headerText) {
    const blocks = String(headerText || '')
        .trim()
        .split(/\r?\n\r?\n/)
        .map((block) => block.trim())
        .filter(Boolean);
    const lastBlock = blocks[blocks.length - 1] || '';
    const lines = lastBlock.split(/\r?\n/).filter(Boolean);
    const statusLine = lines.shift() || '';
    const statusMatch = statusLine.match(/^HTTP\/\d+(?:\.\d+)?\s+(\d+)/i);
    const status = Number(statusMatch?.[1] || 0);
    const headers = new Map();

    lines.forEach((line) => {
        const separatorIndex = line.indexOf(':');
        if (separatorIndex < 0) return;
        const key = line.slice(0, separatorIndex).trim().toLowerCase();
        const value = line.slice(separatorIndex + 1).trim();
        headers.set(key, value);
    });

    return { status, headers };
}

function stringifyDebugPayload(payload) {
    if (payload == null) return '';
    if (Buffer.isBuffer(payload)) {
        return payload.subarray(0, 160).toString('utf8');
    }
    if (typeof payload === 'string') {
        return payload.slice(0, 300);
    }
    try {
        return JSON.stringify(payload).slice(0, 300);
    } catch (_error) {
        return String(payload);
    }
}

class ApiClient {
    constructor(name) {
        this.name = name;
        this.tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `powermoon-smoke-${name}-`));
        this.cookieJarPath = path.join(this.tempDir, 'cookies.txt');
        fs.writeFileSync(this.cookieJarPath, '');
    }

    dispose() {
        fs.rmSync(this.tempDir, { recursive: true, force: true });
    }

    request(relativePath, options = {}) {
        const {
            method = 'GET',
            body,
            expectedStatus = 200,
            parseAs = 'auto'
        } = options;
        const headerDumpPath = path.join(this.tempDir, `headers-${Date.now()}-${Math.random()}.txt`);
        const args = [
            '-sS',
            '-X', method,
            '-D', headerDumpPath,
            '-o', '-',
            '-c', this.cookieJarPath,
            '-b', this.cookieJarPath,
            buildUrl(relativePath)
        ];

        if (body !== undefined) {
            args.splice(args.length - 1, 0, '-H', 'Content-Type: application/json', '-d', JSON.stringify(body));
        }

        const result = spawnSync('curl', args, { encoding: null });
        const headerText = fs.existsSync(headerDumpPath) ? fs.readFileSync(headerDumpPath, 'utf8') : '';
        if (fs.existsSync(headerDumpPath)) {
            fs.unlinkSync(headerDumpPath);
        }

        if (result.error) {
            throw result.error;
        }

        if (result.status !== 0) {
            throw new Error(`${this.name} ${method} ${relativePath} curl failed: ${String(result.stderr || '')}`);
        }

        const { status, headers } = parseHeaderBlock(headerText);
        const rawBody = Buffer.from(result.stdout || []);
        let payload;

        if (parseAs === 'buffer') {
            payload = rawBody;
        } else if (parseAs === 'text') {
            payload = rawBody.toString('utf8');
        } else if (parseAs === 'json') {
            payload = JSON.parse(rawBody.toString('utf8') || 'null');
        } else {
            const contentType = headers.get('content-type') || '';
            if (contentType.includes('application/json')) {
                payload = JSON.parse(rawBody.toString('utf8') || 'null');
            } else if (contentType.includes('application/pdf') || contentType.includes('text/csv')) {
                payload = rawBody;
            } else {
                payload = rawBody.toString('utf8');
            }
        }

        if (status !== expectedStatus) {
            throw new Error(
                `${this.name} ${method} ${relativePath} expected ${expectedStatus} but got ${status}. ${stringifyDebugPayload(payload)}`
            );
        }

        return { status, headers, data: payload };
    }

    login(username, password) {
        return this.request('/auth/login', {
            method: 'POST',
            body: { username, password },
            expectedStatus: 200,
            parseAs: 'json'
        });
    }
}

test('API smoke suite covers auth, permissions, stock, production, planner, and PDFs', { timeout: 120000 }, async (t) => {
    const admin = new ApiClient('admin');
    const powermoon = new ApiClient('powermoon');
    const tempUser = new ApiClient('temp-user');
    const uniqueSuffix = `${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    const tempProductName = `SMOKE_TEST_PRODUCT_${uniqueSuffix}`;
    const tempPartName = `SMOKE_TEST_PART_${uniqueSuffix}`;
    const tempTricoma = String((Date.now() % 100000000) + 1000);
    const csvProductName = `SMOKE_CSV_PRODUCT_${uniqueSuffix}`;
    const csvPartName = `SMOKE_CSV_PART_${uniqueSuffix}`;
    const csvTricoma = String((Date.now() % 100000000) + 5000);
    const tempUsername = `smoke_user_${Date.now()}`;
    const tempUserDisplayName = `Smoke User ${uniqueSuffix}`;
    const tempUserInitialPassword = `TempUser!${uniqueSuffix}`;
    const tempUserUpdatedPassword = `TempUserUpdated!${uniqueSuffix}`;
    let tempProductId = null;
    let tempPartId = null;
    let tempFinishedProductId = null;
    const tempCompletedFinishedProductIds = [];
    const tempScheduleOrderIds = [];
    let tempUserId = null;
    let csvProductId = null;
    let csvPartId = null;

    t.after(() => {
        admin.dispose();
        powermoon.dispose();
        tempUser.dispose();
    });

    try {
        await t.test('app is reachable', async () => {
            const response = admin.request('/', { expectedStatus: 302, parseAs: 'text' });
            assert.equal(response.headers.get('location'), '/client/index.html');
        });

        await t.test('login, auth/me, and logout work', async () => {
            const loginResponse = admin.login(ADMIN_USERNAME, ADMIN_PASSWORD);
            assert.equal(loginResponse.data.success, true);
            assert.equal(loginResponse.data.data.user.username, ADMIN_USERNAME);

            const meResponse = admin.request('/auth/me', { expectedStatus: 200, parseAs: 'json' });
            assert.equal(meResponse.data.success, true);
            assert.equal(meResponse.data.data.user.role, 'admin');

            const logoutResponse = admin.request('/auth/logout', {
                method: 'POST',
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(logoutResponse.data.success, true);

            const unauthorizedResponse = admin.request('/auth/me', {
                expectedStatus: 401,
                parseAs: 'json'
            });
            assert.equal(unauthorizedResponse.data.error, 'Authentication required');

            admin.login(ADMIN_USERNAME, ADMIN_PASSWORD);
            powermoon.login(POWERMOON_USERNAME, POWERMOON_PASSWORD);
        });

        await t.test('admin and powermoon permissions are enforced', async () => {
            const adminUsersResponse = admin.request('/users', { expectedStatus: 200, parseAs: 'json' });
            assert.equal(adminUsersResponse.data.success, true);
            assert.ok(Array.isArray(adminUsersResponse.data.data));

            const adminDashboardResponse = admin.request('/dashboard/kpis', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(typeof adminDashboardResponse.data.data.low_stock_count, 'number');
            assert.equal(typeof adminDashboardResponse.data.data.production_this_month, 'number');

            const adminLogsResponse = admin.request('/admin/logs', { expectedStatus: 200, parseAs: 'json' });
            assert.equal(adminLogsResponse.data.success, true);
            assert.ok(Array.isArray(adminLogsResponse.data.data));
            assert.ok(Array.isArray(adminLogsResponse.data.meta.levels));

            const powermoonUsersResponse = powermoon.request('/users', {
                expectedStatus: 403,
                parseAs: 'json'
            });
            assert.equal(powermoonUsersResponse.data.error, 'You do not have permission to perform this action');

            const powermoonLogsResponse = powermoon.request('/admin/logs', {
                expectedStatus: 403,
                parseAs: 'json'
            });
            assert.equal(powermoonLogsResponse.data.error, 'You do not have permission to perform this action');

            const powermoonFinishedProductsResponse = powermoon.request('/finished-products', {
                expectedStatus: 403,
                parseAs: 'json'
            });
            assert.equal(powermoonFinishedProductsResponse.data.error, 'You do not have permission to perform this action');

            const powermoonDashboardResponse = powermoon.request('/dashboard/kpis', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(typeof powermoonDashboardResponse.data.data.low_stock_count, 'number');
            assert.equal(typeof powermoonDashboardResponse.data.data.production_this_month, 'number');

            const powermoonTransactionsResponse = powermoon.request('/stock-movements', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.ok(Array.isArray(powermoonTransactionsResponse.data.data));
        });

        await t.test('newly created users must change password before using the app', async () => {
            const createUserResponse = admin.request('/users', {
                method: 'POST',
                body: {
                    username: tempUsername,
                    display_name: tempUserDisplayName,
                    role: 'powermoon',
                    password: tempUserInitialPassword
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            tempUserId = Number(createUserResponse.data.data.user_id);
            assert.ok(Number.isInteger(tempUserId) && tempUserId > 0);
            assert.equal(createUserResponse.data.data.must_change_password, true);

            const loginResponse = tempUser.login(tempUsername, tempUserInitialPassword);
            assert.equal(loginResponse.data.success, true);
            assert.equal(loginResponse.data.data.user.username, tempUsername);
            assert.equal(loginResponse.data.data.user.must_change_password, true);

            const meResponse = tempUser.request('/auth/me', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(meResponse.data.data.user.must_change_password, true);

            const blockedResponse = tempUser.request('/part-stock', {
                expectedStatus: 403,
                parseAs: 'json'
            });
            assert.equal(blockedResponse.data.error, 'Password change required before continuing');

            const passwordChangeResponse = tempUser.request('/auth/change-password', {
                method: 'PUT',
                body: {
                    current_password: tempUserInitialPassword,
                    new_password: tempUserUpdatedPassword
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(passwordChangeResponse.data.success, true);
            assert.equal(passwordChangeResponse.data.data.user.must_change_password, false);

            const allowedAfterChangeResponse = tempUser.request('/stock-movements', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.ok(Array.isArray(allowedAfterChangeResponse.data.data));
        });

        await t.test('stock movements, planner, finished products, and exports work end-to-end', async () => {
            const createProductResponse = admin.request('/products', {
                method: 'POST',
                body: { product_name: tempProductName },
                expectedStatus: 200,
                parseAs: 'json'
            });
            tempProductId = Number(createProductResponse.data.data.product_id);
            assert.ok(Number.isInteger(tempProductId) && tempProductId > 0);

            const createPartResponse = admin.request('/parts', {
                method: 'POST',
                body: {
                    part_name: tempPartName,
                    tricoma_nr: tempTricoma,
                    reorder_level: 0,
                    reorder_quantity: 1,
                    usages: [
                        {
                            product_id: tempProductId,
                            quantity_per_product: 2
                        }
                    ]
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            tempPartId = Number(createPartResponse.data.data.part_id);
            assert.ok(Number.isInteger(tempPartId) && tempPartId > 0);

            const buildabilityResponse = admin.request(`/products/${tempProductId}/buildability`, {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(buildabilityResponse.data.success, true);
            assert.equal(Number(buildabilityResponse.data.data.product_id), tempProductId);
            assert.equal(Array.isArray(buildabilityResponse.data.data.requirements), true);
            assert.equal(buildabilityResponse.data.data.requirements.length, 1);
            assert.equal(Number(buildabilityResponse.data.data.requirements[0].part_id), tempPartId);

            const createMovementResponse = admin.request('/stock-movements', {
                method: 'POST',
                body: {
                    part_id: tempPartId,
                    quantity: 10
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(Number(createMovementResponse.data.data.stock_movement_qty), 10);

            const stockBeforeProduction = admin.request('/part-stock', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            const stockBeforeRow = stockBeforeProduction.data.data.find((row) => Number(row.part_id) === tempPartId);
            assert.ok(stockBeforeRow);
            assert.equal(Number(stockBeforeRow.tracked_stock), 10);
            assert.equal(Number(stockBeforeRow.total_movements), 10);
            assert.equal(Number(stockBeforeRow.total_used_in_assembly), 0);

            const plannerResponse = admin.request('/production-planner/calculate', {
                method: 'POST',
                body: {
                    product_id: tempProductId,
                    quantity: 6
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(plannerResponse.data.success, true);
            assert.equal(Number(plannerResponse.data.data.max_buildable), 5);
            assert.equal(plannerResponse.data.data.can_fulfill, false);
            assert.equal(plannerResponse.data.data.missing_materials.length, 1);
            assert.equal(Number(plannerResponse.data.data.missing_materials[0].shortage_quantity), 2);

            const plannerPdfResponse = admin.request('/production-planner/export-pdf', {
                method: 'POST',
                body: {
                    product_id: tempProductId,
                    quantity: 6
                },
                expectedStatus: 200,
                parseAs: 'buffer'
            });
            assert.match(plannerPdfResponse.headers.get('content-type') || '', /application\/pdf/i);
            assert.equal(plannerPdfResponse.data.subarray(0, 4).toString('utf8'), '%PDF');

            const scheduleDueDate1 = '2030-01-10';
            const scheduleDueDate2 = '2030-01-11';
            const scheduleComment1 = 'Priority customer order';
            const scheduleComment2 = 'Second batch for the queue';

            const createScheduleAdminResponse = admin.request('/production-schedule', {
                method: 'POST',
                body: {
                    product_id: tempProductId,
                    quantity: 3,
                    due_date: scheduleDueDate1,
                    comments: scheduleComment1
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            const adminScheduleId = Number(createScheduleAdminResponse.data.data.production_schedule_id);
            assert.ok(Number.isInteger(adminScheduleId) && adminScheduleId > 0);
            tempScheduleOrderIds.push(adminScheduleId);

            const createSchedulePowermoonResponse = powermoon.request('/production-schedule', {
                method: 'POST',
                body: {
                    product_id: tempProductId,
                    quantity: 4,
                    due_date: scheduleDueDate2,
                    comments: scheduleComment2
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            const powermoonScheduleId = Number(createSchedulePowermoonResponse.data.data.production_schedule_id);
            assert.ok(Number.isInteger(powermoonScheduleId) && powermoonScheduleId > 0);
            tempScheduleOrderIds.push(powermoonScheduleId);

            const scheduleOverviewResponse = admin.request('/production-schedule', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            const tempScheduleRows = scheduleOverviewResponse.data.data
                .filter((row) => Number(row.product_id) === tempProductId)
                .sort((left, right) => Number(left.production_schedule_id) - Number(right.production_schedule_id));

            assert.equal(tempScheduleRows.length, 2);
            assert.equal(tempScheduleRows[0].due_date, scheduleDueDate1);
            assert.equal(tempScheduleRows[0].comments, scheduleComment1);
            assert.equal(tempScheduleRows[0].allocation_status, 'Allocated');
            assert.equal(Number(tempScheduleRows[0].missing_parts_count), 0);
            assert.equal(Number(tempScheduleRows[0].allocation_materials[0].reserved_quantity), 6);

            assert.equal(tempScheduleRows[1].due_date, scheduleDueDate2);
            assert.equal(tempScheduleRows[1].comments, scheduleComment2);
            assert.equal(tempScheduleRows[1].allocation_status, 'Shortage');
            assert.equal(Number(tempScheduleRows[1].missing_parts_count), 1);
            assert.equal(Number(tempScheduleRows[1].allocation_materials[0].shortage_quantity), 4);

            const completeScheduleResponse = admin.request(`/production-schedule/${adminScheduleId}/complete`, {
                method: 'POST',
                expectedStatus: 200,
                parseAs: 'json'
            });
            const completedFinishedProductId = Number(completeScheduleResponse.data.data.finished_product?.fin_product_id);
            assert.ok(Number.isInteger(completedFinishedProductId) && completedFinishedProductId > 0);
            tempCompletedFinishedProductIds.push(completedFinishedProductId);
            tempScheduleOrderIds.shift();

            const scheduleOverviewAfterCompleteResponse = admin.request('/production-schedule', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            const completedScheduleRow = scheduleOverviewAfterCompleteResponse.data.data
                .find((row) => Number(row.production_schedule_id) === adminScheduleId);
            assert.ok(completedScheduleRow);
            assert.equal(completedScheduleRow.comments, scheduleComment1);
            assert.equal(completedScheduleRow.schedule_status, 'completed');
            assert.equal(completedScheduleRow.allocation_status, 'Completed');
            assert.ok(Number(scheduleOverviewAfterCompleteResponse.data.meta.completed_orders) >= 1);
            assert.ok(Number(scheduleOverviewAfterCompleteResponse.data.meta.pending_orders) >= 1);

            const powermoonDeleteOwnScheduleResponse = powermoon.request(`/production-schedule/${powermoonScheduleId}`, {
                method: 'DELETE',
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(Number(powermoonDeleteOwnScheduleResponse.data.data.production_schedule_id), powermoonScheduleId);
            tempScheduleOrderIds.pop();

            const reorderPdfResponse = admin.request('/part-stock/export-reorder-pdf', {
                expectedStatus: 200,
                parseAs: 'buffer'
            });
            assert.match(reorderPdfResponse.headers.get('content-type') || '', /application\/pdf/i);
            assert.equal(reorderPdfResponse.data.subarray(0, 4).toString('utf8'), '%PDF');

            const createFinishedProductResponse = admin.request('/finished-products', {
                method: 'POST',
                body: {
                    product_id: tempProductId,
                    quantity: 2
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            tempFinishedProductId = Number(createFinishedProductResponse.data.data.fin_product_id);
            assert.ok(Number.isInteger(tempFinishedProductId) && tempFinishedProductId > 0);

            const finishedProductSearch = admin.request(
                `/finished-products/search/${encodeURIComponent(tempProductName)}`,
                {
                    expectedStatus: 200,
                    parseAs: 'json'
                }
            );
            assert.ok(
                finishedProductSearch.data.data.some((row) => Number(row.fin_product_id) === tempFinishedProductId)
            );

            const stockAfterProduction = admin.request('/part-stock', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            const stockAfterRow = stockAfterProduction.data.data.find((row) => Number(row.part_id) === tempPartId);
            assert.ok(stockAfterRow);
            assert.equal(Number(stockAfterRow.tracked_stock), 0);
            assert.equal(Number(stockAfterRow.total_movements), 10);
            assert.equal(Number(stockAfterRow.total_used_in_assembly), 10);

            const productionTransactions = admin.request(
                `/stock-movements?movement_type=${encodeURIComponent('OUT - Production')}&created_by=${encodeURIComponent(ADMIN_USERNAME)}&source_product_name=${encodeURIComponent(tempProductName)}`,
                {
                    expectedStatus: 200,
                    parseAs: 'json'
                }
            );
            assert.equal(productionTransactions.data.data.length, 2);
            const productionQuantities = productionTransactions.data.data
                .filter((row) => row.part_name === tempPartName)
                .map((row) => Number(row.stock_movement_qty))
                .sort((left, right) => left - right);
            assert.deepEqual(productionQuantities, [-6, -4]);
            assert.ok(productionTransactions.data.data.every((row) => row.source_product_name === tempProductName));

            const inTransactions = admin.request(
                `/stock-movements?query=${encodeURIComponent(tempPartName)}&movement_type=IN&created_by=${encodeURIComponent(ADMIN_USERNAME)}`,
                {
                    expectedStatus: 200,
                    parseAs: 'json'
                }
            );
            assert.equal(inTransactions.data.data.length, 1);
            assert.equal(Number(inTransactions.data.data[0].stock_movement_qty), 10);

            const transactionsCsvResponse = admin.request(
                `/stock-movements/export-csv?movement_type=${encodeURIComponent('OUT - Production')}&source_product_name=${encodeURIComponent(tempProductName)}`,
                {
                    expectedStatus: 200,
                    parseAs: 'buffer'
                }
            );
            assert.match(transactionsCsvResponse.headers.get('content-type') || '', /text\/csv/i);
            const csvText = transactionsCsvResponse.data.toString('utf8');
            assert.match(csvText, /Produced Product/);
            assert.match(csvText, new RegExp(tempProductName));
            assert.match(csvText, new RegExp(tempPartName));

            const transactionsPdfResponse = admin.request(
                `/stock-movements/export-pdf?movement_type=${encodeURIComponent('OUT - Production')}&source_product_name=${encodeURIComponent(tempProductName)}`,
                {
                    expectedStatus: 200,
                    parseAs: 'buffer'
                }
            );
            assert.match(transactionsPdfResponse.headers.get('content-type') || '', /application\/pdf/i);
            assert.equal(transactionsPdfResponse.data.subarray(0, 4).toString('utf8'), '%PDF');
        });

        await t.test('products, parts, and BOM can be exported and imported as CSV', async () => {
            const exportProductsResponse = admin.request('/admin/csv/products/export', {
                expectedStatus: 200,
                parseAs: 'buffer'
            });
            assert.match(exportProductsResponse.headers.get('content-type') || '', /text\/csv/i);
            assert.match(exportProductsResponse.data.toString('utf8'), /product_id/);

            const exportPartsResponse = admin.request('/admin/csv/parts/export', {
                expectedStatus: 200,
                parseAs: 'buffer'
            });
            assert.match(exportPartsResponse.headers.get('content-type') || '', /text\/csv/i);
            assert.match(exportPartsResponse.data.toString('utf8'), /tricoma_nr/);

            const exportBomResponse = admin.request('/admin/csv/bom/export', {
                expectedStatus: 200,
                parseAs: 'buffer'
            });
            assert.match(exportBomResponse.headers.get('content-type') || '', /text\/csv/i);
            assert.match(exportBomResponse.data.toString('utf8'), /quantity_per_product/);

            const productsImportResponse = admin.request('/admin/csv/products/import', {
                method: 'POST',
                body: {
                    csv_text: `product_id;product_name\n;${csvProductName}`
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(Number(productsImportResponse.data.data.created_count), 1);

            const productsAfterImport = admin.request('/products', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            const importedProduct = productsAfterImport.data.data.find((row) => row.product_name === csvProductName);
            assert.ok(importedProduct);
            csvProductId = Number(importedProduct.product_id);

            const partsImportResponse = admin.request('/admin/csv/parts/import', {
                method: 'POST',
                body: {
                    csv_text: `part_id;part_name;tricoma_nr;reorder_level;reorder_quantity\n;${csvPartName};${csvTricoma};0;1`
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(Number(partsImportResponse.data.data.created_count), 1);

            const partsAfterImport = admin.request('/parts', {
                expectedStatus: 200,
                parseAs: 'json'
            });
            const importedPart = partsAfterImport.data.data.find((row) => row.part_name === csvPartName);
            assert.ok(importedPart);
            csvPartId = Number(importedPart.part_id);

            const bomImportResponse = admin.request('/admin/csv/bom/import', {
                method: 'POST',
                body: {
                    csv_text: [
                        'product_id;product_name;part_id;part_name;tricoma_nr;quantity_per_product',
                        `${csvProductId};${csvProductName};${csvPartId};${csvPartName};${csvTricoma};3`
                    ].join('\n')
                },
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(Number(bomImportResponse.data.data.created_count), 1);

            const csvBuildabilityResponse = admin.request(`/products/${csvProductId}/buildability`, {
                expectedStatus: 200,
                parseAs: 'json'
            });
            assert.equal(csvBuildabilityResponse.data.success, true);
            assert.equal(csvBuildabilityResponse.data.data.requirements.length, 1);
            assert.equal(Number(csvBuildabilityResponse.data.data.requirements[0].part_id), csvPartId);
            assert.equal(Number(csvBuildabilityResponse.data.data.requirements[0].quantity_per_product), 3);
        });
    } finally {
        while (tempScheduleOrderIds.length > 0) {
            const scheduleOrderId = tempScheduleOrderIds.pop();
            try {
                admin.request(`/production-schedule/${scheduleOrderId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            } catch (_error) {}
        }

        while (tempCompletedFinishedProductIds.length > 0) {
            const finishedProductId = tempCompletedFinishedProductIds.pop();
            try {
                admin.request(`/finished-products/${finishedProductId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            } catch (_error) {}
        }

        try {
            if (tempFinishedProductId) {
                admin.request(`/finished-products/${tempFinishedProductId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            }
        } catch (_error) {}

        try {
            if (tempPartId) {
                admin.request(`/parts/${tempPartId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            }
        } catch (_error) {}

        try {
            if (tempProductId) {
                admin.request(`/products/${tempProductId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            }
        } catch (_error) {}

        try {
            if (csvPartId) {
                admin.request(`/parts/${csvPartId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            }
        } catch (_error) {}

        try {
            if (csvProductId) {
                admin.request(`/products/${csvProductId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            }
        } catch (_error) {}

        try {
            if (tempUserId) {
                admin.request(`/users/${tempUserId}`, {
                    method: 'DELETE',
                    expectedStatus: 200,
                    parseAs: 'json'
                });
            }
        } catch (_error) {}
    }
});
