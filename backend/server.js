const path = require('path');
// 1. Load Environment Variables immediately
require('dotenv').config({ path: path.join(__dirname, '.env') });

const express = require('express');
const axios = require('axios');
const cors = require('cors');
const querystring = require('querystring');
const cookieSession = require('cookie-session');

// 2. Import Services (must exist)
const userService = require('./services/userService');
const analyticsService = require('./services/analyticsService');
const friendService = require('./services/friendService');
const recommendationService = require('./services/recommendationService');
const cacheService = require('./services/cacheService');
const analyticsAlgo = require('./utils/analyticsAlgo');
const swaggerUi = require('swagger-ui-express');
const swaggerSpecs = require('./config/swagger');

// 3. Initialize App & Constants
const app = express();
const PORT = process.env.PORT || 8888;

// Sanitize Envs
const CLIENT_ID = (process.env.CLIENT_ID || process.env.SPOTIFY_CLIENT_ID || '').trim();
const CLIENT_SECRET = (process.env.CLIENT_SECRET || process.env.SPOTIFY_CLIENT_SECRET || '').trim();
let REDIRECT_URI = (process.env.REDIRECT_URI || process.env.SPOTIFY_REDIRECT_URI || (process.env.RENDER_EXTERNAL_URL ? `${process.env.RENDER_EXTERNAL_URL}/callback` : '')).trim();

// Auto-fix common configuration error: missing /callback
if (REDIRECT_URI && !REDIRECT_URI.endsWith('/callback')) {
    console.log(`[WARN] REDIRECT_URI '${REDIRECT_URI}' seems to be missing '/callback'. Appending it automatically.`);
    REDIRECT_URI = `${REDIRECT_URI}/callback`;
}

// Diagnostics Log
console.log('--- SERVER CONFIG ---');
console.log(`CLIENT_ID: ${CLIENT_ID ? 'Set (Length ' + CLIENT_ID.length + ')' : 'MISSING'}`);
console.log(`REDIRECT_URI: ${REDIRECT_URI || 'MISSING'}`);
console.log('---------------------');

// 4. Middlewares (Global)
app.set('trust proxy', 1);
app.use(cookieSession({
    name: 'musicmind-session-v2',
    keys: [process.env.COOKIE_KEY || 'default_secret_key_change_me'],
    maxAge: 24 * 60 * 60 * 1000,
    secure: false,
    httpOnly: true,
    sameSite: 'lax'
}));

// Ensure FRONTEND_URL has protocol (Render provides 'host' property without https://)
const rawFrontendUrl = process.env.FRONTEND_URL || 'http://localhost:5173';
const frontendUrl = rawFrontendUrl.startsWith('http') ? rawFrontendUrl : `https://${rawFrontendUrl}`;

app.use(cors({
    origin: frontendUrl,
    credentials: true
}));
app.use(express.json());

// 5. Custom Middlewares (Definitions)

/**
 * Middleware to check authentication status.
 * Defined BEFORE usage in routes to avoid ReferenceError.
 */
async function checkAuth(req, res, next) {
    if (!req.session || !req.session.access_token) {
        return res.status(401).json({ error: 'Not authenticated' });
    }

    if (Date.now() > req.session.expires_at) {
        console.log('🔄 Token expired. Refreshing...');
        try {
            // Refresh Token Request - Using Body Params for stability
            const params = new URLSearchParams();
            params.append('grant_type', 'refresh_token');
            params.append('refresh_token', req.session.refresh_token);
            params.append('client_id', CLIENT_ID);
            params.append('client_secret', CLIENT_SECRET);

            const refreshResponse = await axios.post('https://accounts.spotify.com/api/token', params, {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
            });

            const { access_token, expires_in } = refreshResponse.data;
            req.session.access_token = access_token;
            req.session.expires_at = Date.now() + (expires_in * 1000) - 60000;
            console.log('✅ Token refreshed.');
        } catch (error) {
            console.error('❌ Failed to refresh:', error.response?.data || error.message);
            req.session = null;
            return res.status(401).json({ error: 'Session expired' });
        }
    }
    next();
}

// 6. Routes

// --- Documentation ---
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpecs));


// --- Auth Routes ---

app.get('/login', (req, res) => {
    console.log(`[LOGIN] Initiating login. Client ID: ${CLIENT_ID ? 'Present' : 'MISSING'}, Redirect URI: ${REDIRECT_URI}`);

    if (!CLIENT_ID) {
        console.error('CRITICAL: CLIENT_ID is missing in server environment.');
        return res.status(500).send(`
            <h1>Server Configuration Error</h1>
            <p><strong>Missing Client ID:</strong> The <code>SPOTIFY_CLIENT_ID</code> environment variable is not set.</p>
            <p>Please check your Render Dashboard > Environment.</p>
        `);
    }

    if (!REDIRECT_URI) {
        return res.status(500).send('<h1>Server Configuration Error</h1><p>Missing REDIRECT_URI.</p>');
    }

    const scope = 'user-read-private user-read-email user-top-read';
    // Strictly use the sanitized env var for redirect
    const authUrl = `https://accounts.spotify.com/authorize?response_type=code&client_id=${CLIENT_ID}&scope=${encodeURIComponent(scope)}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`;
    res.redirect(authUrl);
});

app.get('/callback', async (req, res) => {
    const code = req.query.code || null;
    const error = req.query.error || null;

    if (error) return res.send(`Spotify Error: ${error}`);
    if (!code) return res.status(400).send('Authentication Error: No authentication code returned from Spotify.');

    try {
        console.log(`[CALLBACK] Exchanging code for token. Redirect URI: ${REDIRECT_URI}`);

        const params = new URLSearchParams();
        params.append('grant_type', 'authorization_code');
        params.append('code', code);
        params.append('redirect_uri', REDIRECT_URI);
        // Client ID/Secret removed from body, using Basic Auth header instead

        const authString = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');

        const response = await axios.post('https://accounts.spotify.com/api/token', params, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': `Basic ${authString}`
            }
        });

        const { access_token, refresh_token, expires_in } = response.data;

        // Get Profile
        const profileRes = await axios.get('https://api.spotify.com/v1/me', {
            headers: { Authorization: `Bearer ${access_token}` }
        });

        // Save User
        await userService.upsertUser(profileRes.data, refresh_token);

        // Update Session
        req.session.access_token = access_token;
        req.session.refresh_token = refresh_token;
        req.session.expires_at = Date.now() + (expires_in * 1000) - 60000;

        res.redirect('http://localhost:5173/dashboard?login=success');

    } catch (err) {
        console.error('Login Callback Error:', err.message);
        const debugInfo = {
            message: 'Spotify Auth Failed',
            error: err.response?.data || err.message,
            debug: {
                used_redirect_uri: REDIRECT_URI,
                used_client_id: CLIENT_ID ? `${CLIENT_ID.substring(0, 5)}...` : 'MISSING',
                grant_type: 'authorization_code'
            }
        };

        // Return clear HTML error for browser
        res.status(500).send(`
            <div style="font-family: monospace; background: #f0f0f0; padding: 20px;">
                <h2 style="color: #d32f2f;">Authentication Error</h2>
                <p><strong>Spotify Message:</strong> ${JSON.stringify(err.response?.data || err.message)}</p>
                <hr/>
                <h3>Debug Info (Check your Dashboard)</h3>
                <p><strong>Redirect URI sent to Spotify:</strong> <br/><code>${REDIRECT_URI}</code></p>
                <p><strong>Client ID:</strong> ${CLIENT_ID}</p>
                <hr/>
                <p><em>Make sure the Redirect URI above EXACTLY matches what is in your Spotify Developer Dashboard.</em></p>
            </div>
        `);
    }
});

app.get('/logout', async (req, res) => {
    if (req.session && req.session.access_token) {
        const token = req.session.access_token;
        await Promise.all([
            cacheService.del(`profile:${token}`),
            cacheService.del(`dashboard:${token}`)
        ]);
        console.log('🧹 Cache Cleared for Session');
    }
    req.session = null;
    res.redirect(frontendUrl);
});

// Aliases
app.get('/auth/login', (req, res) => res.redirect('/login'));
app.get('/auth/me', checkAuth, (req, res) => res.redirect('/api/user/profile'));
app.get('/auth/logout', (req, res) => res.redirect('/logout'));

// --- User & Analytics API ---

/**
 * @swagger
 * /api/user/profile:
 *   get:
 *     summary: Get current user profile
 *     tags: [User]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: User profile data
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 id:
 *                   type: string
 *                 display_name:
 *                   type: string
 *       401:
 *         description: Not authenticated
 */
app.get('/api/user/profile', checkAuth, async (req, res) => {
    const cacheKey = `profile:${req.session.access_token}`;

    try {
        // 1. Check Cache
        const cached = await cacheService.get(cacheKey);
        if (cached) {
            console.log('⚡ Using Cached Profile');
            return res.json(cached);
        }

        const response = await axios.get('https://api.spotify.com/v1/me', {
            headers: { Authorization: `Bearer ${req.session.access_token}` }
        });

        // 2. Set Cache (30 min)
        await cacheService.set(cacheKey, response.data, 1800);

        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

/**
 * @swagger
 * /api/analytics/dashboard:
 *   get:
 *     summary: Get analytics dashboard data
 *     tags: [Analytics]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Dashboard data including top tracks, genres, and mood score
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 moodScore:
 *                   type: integer
 *                 topGenres:
 *                   type: array
 *                   items:
 *                     type: object
 */
app.get('/api/analytics/dashboard', checkAuth, async (req, res) => {
    const cacheKey = `dashboard:${req.session.access_token}`;

    try {
        // 1. Check Cache
        const cached = await cacheService.get(cacheKey);
        if (cached) {
            console.log('⚡ Using Cached Dashboard');
            return res.json(cached);
        }

        const headers = { Authorization: `Bearer ${req.session.access_token}` };

        // Parallel Fetch for speed
        const [artistsRes, tracksRes] = await Promise.all([
            axios.get('https://api.spotify.com/v1/me/top/artists?limit=50&time_range=long_term', { headers }),
            axios.get('https://api.spotify.com/v1/me/top/tracks?limit=50&time_range=short_term', { headers })
        ]);

        const topGenres = analyticsAlgo.getTopGenres(artistsRes.data.items);
        const trackIds = tracksRes.data.items.map(t => t.id).join(',');

        let moodScore = 50;
        try {
            const audioRes = await axios.get(`https://api.spotify.com/v1/audio-features?ids=${trackIds}`, { headers });
            moodScore = analyticsAlgo.calculateMoodScore(audioRes.data.audio_features);
        } catch (e) {
            console.error('Audio Features warning:', e.message);
        }

        const analyticsData = {
            topGenres,
            moodScore,
            topTracks: tracksRes.data.items.slice(0, 5).map(t => ({
                name: t.name,
                artist: t.artists[0].name,
                image: t.album.images[0]?.url,
                preview: t.preview_url
            })),
            generatedAt: new Date().toISOString()
        };

        // Async save to DB
        const meRes = await axios.get('https://api.spotify.com/v1/me', { headers });
        analyticsService.saveAnalytics(meRes.data.id, analyticsData).catch(e => console.error('DB Save Error:', e));

        // 2. Set Cache (1 hour)
        await cacheService.set(cacheKey, analyticsData, 3600);

        res.json(analyticsData);
    } catch (error) {
        console.error('Dashboard Error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * @swagger
 * /api/recommend/generate:
 *   post:
 *     summary: Trigger recommendation generation
 *     tags: [Recommendations]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Trigger successful
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 status:
 *                   type: string
 *                 message:
 *                   type: string
 */
app.post('/api/recommend/generate', checkAuth, async (req, res) => {
    try {
        const meRes = await axios.get('https://api.spotify.com/v1/me', {
            headers: { Authorization: `Bearer ${req.session.access_token}` }
        });
        const result = await recommendationService.generateRecommendations(meRes.data.id);
        res.json(result);
    } catch (error) {
        console.error('Recommendation Error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// --- Friends API ---

app.get('/friends/search', checkAuth, async (req, res) => {
    try {
        const query = req.query.q;
        if (!query) return res.status(400).json({ error: "Query required" });
        const me = await axios.get('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${req.session.access_token}` } });
        const users = await friendService.searchUsers(query, me.data.id);
        res.json(users);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/friends/request', checkAuth, async (req, res) => {
    try {
        const { receiverId } = req.body;
        const me = await axios.get('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${req.session.access_token}` } });
        const result = await friendService.sendRequest(me.data.id, receiverId);
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/friends', checkAuth, async (req, res) => {
    try {
        const me = await axios.get('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${req.session.access_token}` } });
        const friends = await friendService.getFriends(me.data.id);
        res.json(friends);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/friends/compare', checkAuth, async (req, res) => {
    try {
        const { targetUserId } = req.body;
        const me = await axios.get('https://api.spotify.com/v1/me', { headers: { Authorization: `Bearer ${req.session.access_token}` } });
        const result = await friendService.compareUsers(me.data.id, targetUserId);
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// --- Test Panel ---
app.get('/test-panel', (req, res) => {
    res.send(`
    <html>
    <body style="font-family:sans-serif; padding:20px;">
        <h1>MusicMind Debug Panel</h1>
        <p>Status: Server Running</p>
        <button onclick="window.location.href='/login'">Login (Spotify)</button>
        <button onclick="fetch('/api/analytics/dashboard').then(r=>r.json()).then(d=>document.body.append(JSON.stringify(d)))">Data</button>
    </body>
    </html>
    `);
});

app.get('/', (req, res) => res.send('<h1>MusicMind Backend</h1><a href="/login">Login</a>'));

// 7. Start Server
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
