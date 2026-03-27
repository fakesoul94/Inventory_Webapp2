// ============================================================================
// Authentication and Session Helpers
// ============================================================================
// Purpose:
// - Hash and verify user passwords without extra dependencies.
// - Issue and validate signed session cookies.
// - Keep auth-specific normalization in one small module.
// ============================================================================

const crypto = require('crypto');

const USER_ROLES = Object.freeze({
    ADMIN: 'admin',
    POWERMOON: 'powermoon'
});

const SESSION_COOKIE_NAME = 'powermoon_session';
const SESSION_IDLE_TIMEOUT_MS = 1000 * 60 * 5;
const PASSWORD_KEY_LENGTH = 64;
const PASSWORD_SALT_BYTES = 16;
const SESSION_SECRET = (() => {
    const configuredSecret = String(process.env.APP_SESSION_SECRET || '').trim();
    if (!configuredSecret) {
        throw new Error('APP_SESSION_SECRET is required before starting the server.');
    }

    return configuredSecret;
})();

function signSessionPayload(encodedPayload) {
    return crypto
        .createHmac('sha256', SESSION_SECRET)
        .update(encodedPayload)
        .digest('base64url');
}

function createPasswordHash(password) {
    const salt = crypto.randomBytes(PASSWORD_SALT_BYTES).toString('base64url');
    const derivedKey = crypto.scryptSync(password, salt, PASSWORD_KEY_LENGTH).toString('base64url');
    return `scrypt$${salt}$${derivedKey}`;
}

function verifyPassword(password, storedHash) {
    if (typeof storedHash !== 'string' || !storedHash.startsWith('scrypt$')) {
        return false;
    }

    const [, salt, expectedHash] = storedHash.split('$');
    if (!salt || !expectedHash) {
        return false;
    }

    const actualHash = crypto.scryptSync(password, salt, PASSWORD_KEY_LENGTH).toString('base64url');
    const expectedBuffer = Buffer.from(expectedHash, 'utf8');
    const actualBuffer = Buffer.from(actualHash, 'utf8');

    if (expectedBuffer.length !== actualBuffer.length) {
        return false;
    }

    return crypto.timingSafeEqual(expectedBuffer, actualBuffer);
}

function createSessionToken(user) {
    const now = Date.now();
    const expiresAt = now + SESSION_IDLE_TIMEOUT_MS;
    const payload = {
        user_id: Number(user?.user_id),
        session_version: Number(user?.session_version || 1),
        issued_at: now,
        expires_at: expiresAt
    };
    const encodedPayload = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
    const signature = signSessionPayload(encodedPayload);

    return {
        token: `${encodedPayload}.${signature}`,
        expiresAt,
        payload
    };
}

function verifySessionToken(token) {
    if (typeof token !== 'string' || token.length === 0 || !token.includes('.')) {
        return null;
    }

    const [encodedPayload, providedSignature] = token.split('.');
    if (!encodedPayload || !providedSignature) {
        return null;
    }

    const expectedSignature = signSessionPayload(encodedPayload);
    const providedBuffer = Buffer.from(providedSignature, 'utf8');
    const expectedBuffer = Buffer.from(expectedSignature, 'utf8');

    if (providedBuffer.length !== expectedBuffer.length) {
        return null;
    }

    if (!crypto.timingSafeEqual(providedBuffer, expectedBuffer)) {
        return null;
    }

    let payload;
    try {
        payload = JSON.parse(Buffer.from(encodedPayload, 'base64url').toString('utf8'));
    } catch (_error) {
        return null;
    }

    const userId = Number(payload?.user_id);
    const sessionVersion = Number(payload?.session_version);
    const issuedAt = Number(payload?.issued_at);
    const expiresAt = Number(payload?.expires_at);

    if (!Number.isInteger(userId) || userId <= 0) {
        return null;
    }

    if (!Number.isInteger(sessionVersion) || sessionVersion <= 0) {
        return null;
    }

    if (!Number.isFinite(issuedAt) || issuedAt <= 0) {
        return null;
    }

    if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
        return null;
    }

    return {
        user_id: userId,
        session_version: sessionVersion,
        issued_at: issuedAt,
        expires_at: expiresAt
    };
}

function parseCookies(cookieHeader) {
    const cookies = {};
    const rawHeader = String(cookieHeader || '');

    if (!rawHeader) {
        return cookies;
    }

    rawHeader.split(';').forEach((pair) => {
        const separatorIndex = pair.indexOf('=');
        if (separatorIndex < 0) return;

        const name = pair.slice(0, separatorIndex).trim();
        const value = pair.slice(separatorIndex + 1).trim();

        if (!name) return;
        cookies[name] = decodeURIComponent(value);
    });

    return cookies;
}

function buildSessionCookie(token, expiresAt, options = {}) {
    const {
        secure = false
    } = options;

    const cookieParts = [
        `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}`,
        'Path=/',
        'HttpOnly',
        'SameSite=Lax',
        `Expires=${new Date(expiresAt).toUTCString()}`
    ];

    if (secure) {
        cookieParts.push('Secure');
    }

    return cookieParts.join('; ');
}

function buildExpiredSessionCookie(options = {}) {
    const {
        secure = false
    } = options;

    const cookieParts = [
        `${SESSION_COOKIE_NAME}=`,
        'Path=/',
        'HttpOnly',
        'SameSite=Lax',
        'Expires=Thu, 01 Jan 1970 00:00:00 GMT'
    ];

    if (secure) {
        cookieParts.push('Secure');
    }

    return cookieParts.join('; ');
}

function buildPublicUser(user) {
    if (!user) return null;

    return {
        user_id: Number(user.user_id),
        username: String(user.username || ''),
        display_name: String(user.display_name || user.username || ''),
        role: String(user.role || ''),
        must_change_password: Boolean(user.must_change_password)
    };
}

function getDefaultSeedUsers() {
    const adminPassword = String(process.env.DEFAULT_ADMIN_PASSWORD || '');
    const powermoonPassword = String(process.env.DEFAULT_POWERMOON_PASSWORD || '');

    if (!adminPassword || !powermoonPassword) {
        throw new Error('DEFAULT_ADMIN_PASSWORD and DEFAULT_POWERMOON_PASSWORD are required before seeding initial users.');
    }

    return [
        {
            username: 'admin',
            display_name: 'Administrator',
            role: USER_ROLES.ADMIN,
            password: adminPassword
        },
        {
            username: 'powermoon',
            display_name: 'Powermoon',
            role: USER_ROLES.POWERMOON,
            password: powermoonPassword
        }
    ];
}

module.exports = {
    USER_ROLES,
    SESSION_COOKIE_NAME,
    SESSION_IDLE_TIMEOUT_MS,
    buildExpiredSessionCookie,
    buildPublicUser,
    buildSessionCookie,
    createPasswordHash,
    createSessionToken,
    getDefaultSeedUsers,
    parseCookies,
    verifyPassword,
    verifySessionToken
};
