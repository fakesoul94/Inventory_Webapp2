const test = require('node:test');
const assert = require('node:assert/strict');

process.env.APP_SESSION_SECRET = process.env.APP_SESSION_SECRET || 'unit-test-session-secret';

const {
    SESSION_COOKIE_NAME,
    SESSION_IDLE_TIMEOUT_MS,
    buildExpiredSessionCookie,
    buildSessionCookie,
    createPasswordHash,
    createSessionToken,
    parseCookies,
    verifyPassword,
    verifySessionToken
} = require('../authService');

test('authService hashes and verifies passwords', () => {
    const password = 'SmokeTest123!';
    const passwordHash = createPasswordHash(password);

    assert.notEqual(passwordHash, password);
    assert.equal(verifyPassword(password, passwordHash), true);
    assert.equal(verifyPassword('WrongPassword123!', passwordHash), false);
});

test('authService parses and verifies a valid session token', () => {
    const token = createSessionToken({ user_id: 99, session_version: 3 });
    const payload = verifySessionToken(token.token);

    assert.equal(payload.user_id, 99);
    assert.equal(payload.session_version, 3);
    assert.ok(payload.expires_at > payload.issued_at);
});

test('authService invalidates expired session tokens without waiting 5 minutes', () => {
    const originalDateNow = Date.now;
    const token = createSessionToken({ user_id: 7, session_version: 1 });

    try {
        Date.now = () => token.expiresAt + 1;
        assert.equal(verifySessionToken(token.token), null);
    } finally {
        Date.now = originalDateNow;
    }
});

test('authService builds and expires session cookies predictably', () => {
    const expiresAt = Date.now() + SESSION_IDLE_TIMEOUT_MS;
    const cookie = buildSessionCookie('signed-token-value', expiresAt, { secure: true });
    const expiredCookie = buildExpiredSessionCookie({ secure: true });
    const parsedCookies = parseCookies(`${cookie}; theme=dark`);

    assert.match(cookie, new RegExp(`^${SESSION_COOKIE_NAME}=signed-token-value`));
    assert.match(cookie, /HttpOnly/);
    assert.match(cookie, /SameSite=Lax/);
    assert.match(cookie, /Secure/);
    assert.match(expiredCookie, new RegExp(`^${SESSION_COOKIE_NAME}=`));
    assert.match(expiredCookie, /Thu, 01 Jan 1970 00:00:00 GMT/);
    assert.match(expiredCookie, /Secure/);
    assert.equal(parsedCookies[SESSION_COOKIE_NAME], 'signed-token-value');
});
