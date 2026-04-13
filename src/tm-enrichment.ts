import * as dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import * as cheerio from 'cheerio';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const AIRTABLE_API_KEY  = process.env.AIRTABLE_API_KEY!;
const RAPIDAPI_KEY      = process.env.RAPIDAPI_KEY || '';
const USE_PROXY         = !!RAPIDAPI_KEY;                                       // auto-detect proxy mode
const AIRTABLE_BASE     = process.env.AIRTABLE_BASE     || 'apprT24SuAvV8oZXX';
const AIRTABLE_TABLE    = process.env.AIRTABLE_TABLE    || 'tblKxel0FfAjklhPe';
const AIRTABLE_VIEW     = process.env.AIRTABLE_VIEW     || 'viwO4B0htcTlCH69M'; // "Transfermarket Get"
const PROFILE_LIMIT     = parseInt(process.env.PROFILE_LIMIT || '0');           // 0 = all
const CONCURRENCY       = parseInt(process.env.CONCURRENCY   || (USE_PROXY ? '10' : '1'));
const FETCH_DELAY_MS    = parseInt(process.env.FETCH_DELAY   || (USE_PROXY ? '0' : '2000')); // zero delay if proxy

const AIRTABLE_BATCH    = 10; // Airtable max patch size

// ---------------------------------------------------------------------------
// HTTP clients
// ---------------------------------------------------------------------------

const TM_HEADERS = {
    'User-Agent':                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language':           'en-US,en;q=0.9',
    'Accept-Encoding':           'gzip, deflate, br',
    'Referer':                   'https://www.transfermarkt.com/',
    'sec-ch-ua':                 '"Chromium";v="120", "Not_A Brand";v="24", "Google Chrome";v="120"',
    'sec-ch-ua-mobile':          '?0',
    'sec-ch-ua-platform':        '"macOS"',
    'sec-fetch-dest':            'document',
    'sec-fetch-mode':            'navigate',
    'sec-fetch-site':            'same-origin',
    'sec-fetch-user':            '?1',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control':             'max-age=0',
    'Connection':                'keep-alive',
};

const TM_JSON_HEADERS = {
    ...TM_HEADERS,
    'Accept':           'application/json, text/plain, */*',
    'sec-fetch-dest':   'empty',
    'sec-fetch-mode':   'cors',
    'sec-fetch-site':   'same-origin',
    'X-Requested-With': 'XMLHttpRequest',
};

const airtable = axios.create({
    baseURL: `https://api.airtable.com/v0/${AIRTABLE_BASE}/${AIRTABLE_TABLE}`,
    headers: {
        'Authorization': `Bearer ${AIRTABLE_API_KEY}`,
        'Content-Type':  'application/json',
    },
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface AirtableRecord {
    id: string;
    fields: Record<string, any>;
}

interface TmData {
    fullName:          string;
    displayName:       string;
    shirtNumber:       string;
    dateOfBirth:       string;
    placeOfBirth:      string;
    birthCountry:      string;   // country name, used to derive CC
    citizenships:      string;
    height:            string;
    position:          string;
    foot:              string;
    clubName:          string;
    clubLink:          string;
    leagueNames:       string;
    contractExpiry:    string;
    marketValue:       string;
    marketValueDate:   string;
    currentIntlTeam:   string;
    nationalCaps:      string;
    nationalGoals:     string;
    trophies:          string;
    agentName:         string;
    agentLink:         string;
    headshot:          string;
    images:            string;   // gallery image URLs, newline-separated
    instagram:         string;
    facebook:          string;
    tiktok:            string;
    twitter:           string;
    website:           string;
    appearancesByComp: string;  // JSON summary string
    totalAppearances:  number;
    goals:             number;
    assists:           number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function createLimiter(concurrency: number) {
    let active = 0;
    const queue: (() => void)[] = [];
    return function limit<T>(fn: () => Promise<T>): Promise<T> {
        return new Promise((resolve, reject) => {
            const run = async () => {
                active++;
                try   { resolve(await fn()); }
                catch (e) { reject(e); }
                finally {
                    active--;
                    if (queue.length > 0) queue.shift()!();
                }
            };
            active < concurrency ? run() : queue.push(run);
        });
    };
}

// Extract numeric player ID from any TM URL format
// e.g. https://www.transfermarkt.com/kai-havertz/profil/spieler/309400 → 309400
function extractPlayerId(url: string): string | null {
    const m = url.match(/\/spieler\/(\d+)/);
    return m ? m[1] : null;
}

// Extract slug from URL e.g. "kai-havertz"
function extractSlug(url: string): string | null {
    const m = url.match(/transfermarkt\.[a-z.]+\/([^/]+)\//);
    return m ? m[1] : null;
}

// Normalise to .com URL (records may have .co.uk etc.)
function normaliseTmUrl(url: string): string {
    return url.replace(/transfermarkt\.[a-z.]+\//, 'transfermarkt.com/');
}

function cleanText(s: string | undefined | null): string {
    return (s || '').replace(/\s+/g, ' ').trim();
}

// "1,79 m" → "179", "N/A" or blank → ""
function formatHeight(raw: string): string {
    if (!raw || raw === 'N/A') return '';
    const m = raw.replace(',', '.').match(/[\d.]+/);
    if (!m) return '';
    return String(Math.round(parseFloat(m[0]) * 100));
}

// "left" → "Left", "N/A" → ""
function formatFoot(raw: string): string {
    if (!raw || raw === 'N/A') return '';
    return raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
}

// Country name → ISO 3166-1 alpha-2 code
const COUNTRY_CODES: Record<string, string> = {
    'Afghanistan':'AF','Albania':'AL','Algeria':'DZ','Andorra':'AD','Angola':'AO',
    'Antigua and Barbuda':'AG','Argentina':'AR','Armenia':'AM','Australia':'AU',
    'Austria':'AT','Azerbaijan':'AZ','Bahamas':'BS','Bahrain':'BH','Bangladesh':'BD',
    'Barbados':'BB','Belarus':'BY','Belgium':'BE','Belize':'BZ','Benin':'BJ',
    'Bhutan':'BT','Bolivia':'BO','Bosnia-Herzegovina':'BA','Bosnia and Herzegovina':'BA',
    'Botswana':'BW','Brazil':'BR','Brunei':'BN','Bulgaria':'BG','Burkina Faso':'BF',
    'Burundi':'BI','Cambodia':'KH','Cameroon':'CM','Canada':'CA','Cape Verde':'CV',
    'Central African Republic':'CF','Chad':'TD','Chile':'CL','China':'CN','Colombia':'CO',
    'Comoros':'KM','Congo':'CG','Congo DR':'CD','Costa Rica':'CR','Croatia':'HR',
    'Cuba':'CU','Cyprus':'CY','Czech Republic':'CZ','Czechia':'CZ','Denmark':'DK',
    'Djibouti':'DJ','Dominica':'DM','Dominican Republic':'DO','Ecuador':'EC','Egypt':'EG',
    'El Salvador':'SV','Equatorial Guinea':'GQ','Eritrea':'ER','Estonia':'EE',
    'Eswatini':'SZ','Ethiopia':'ET','Fiji':'FJ','Finland':'FI','France':'FR','Gabon':'GA',
    'Gambia':'GM','Georgia':'GE','Germany':'DE','Ghana':'GH','Greece':'GR','Grenada':'GD',
    'Guatemala':'GT','Guinea':'GN','Guinea-Bissau':'GW','Guyana':'GY','Haiti':'HT',
    'Honduras':'HN','Hungary':'HU','Iceland':'IS','India':'IN','Indonesia':'ID','Iran':'IR',
    'Iraq':'IQ','Ireland':'IE','Israel':'IL','Italy':'IT','Ivory Coast':'CI',
    "Côte d'Ivoire":'CI','Jamaica':'JM','Japan':'JP','Jordan':'JO','Kazakhstan':'KZ',
    'Kenya':'KE','Kosovo':'XK','Kuwait':'KW','Kyrgyzstan':'KG','Laos':'LA','Latvia':'LV',
    'Lebanon':'LB','Lesotho':'LS','Liberia':'LR','Libya':'LY','Liechtenstein':'LI',
    'Lithuania':'LT','Luxembourg':'LU','Madagascar':'MG','Malawi':'MW','Malaysia':'MY',
    'Maldives':'MV','Mali':'ML','Malta':'MT','Mauritania':'MR','Mauritius':'MU',
    'Mexico':'MX','Moldova':'MD','Monaco':'MC','Mongolia':'MN','Montenegro':'ME',
    'Morocco':'MA','Mozambique':'MZ','Myanmar':'MM','Namibia':'NA','Nepal':'NP',
    'Netherlands':'NL','New Zealand':'NZ','Nicaragua':'NI','Niger':'NE','Nigeria':'NG',
    'North Korea':'KP','North Macedonia':'MK','Norway':'NO','Oman':'OM','Pakistan':'PK',
    'Palestine':'PS','Panama':'PA','Papua New Guinea':'PG','Paraguay':'PY','Peru':'PE',
    'Philippines':'PH','Poland':'PL','Portugal':'PT','Qatar':'QA','Romania':'RO',
    'Russia':'RU','Rwanda':'RW','Saudi Arabia':'SA','Senegal':'SN','Serbia':'RS',
    'Sierra Leone':'SL','Singapore':'SG','Slovakia':'SK','Slovenia':'SI','Somalia':'SO',
    'South Africa':'ZA','South Korea':'KR','South Sudan':'SS','Spain':'ES','Sri Lanka':'LK',
    'Sudan':'SD','Suriname':'SR','Sweden':'SE','Switzerland':'CH','Syria':'SY',
    'Taiwan':'TW','Tajikistan':'TJ','Tanzania':'TZ','Thailand':'TH','Togo':'TG',
    'Trinidad and Tobago':'TT','Tunisia':'TN','Turkey':'TR','Turkmenistan':'TM',
    'Uganda':'UG','Ukraine':'UA','United Arab Emirates':'AE','England':'GB',
    'United Kingdom':'GB','Scotland':'GB','Wales':'GB','Northern Ireland':'GB',
    'United States':'US','USA':'US','Uruguay':'UY','Uzbekistan':'UZ','Venezuela':'VE',
    'Vietnam':'VN','Yemen':'YE','Zambia':'ZM','Zimbabwe':'ZW',
};

function countryToCode(country: string): string {
    return COUNTRY_CODES[country.trim()] || '';
}

// Parse market value string like "€50.00m" or "€800k" → keep as string
function parseMarketValue($: cheerio.CheerioAPI): string {
    const raw = cleanText($('.data-header__market-value-wrapper').first().text());
    // Remove the "Last update:..." suffix
    return raw.replace(/Last update:.*/i, '').trim();
}

// ---------------------------------------------------------------------------
// Global TM request queue — serialises ALL requests to transfermarkt.com
// regardless of how many concurrent workers are running, with a minimum
// delay between each request to avoid triggering bot detection.
// ---------------------------------------------------------------------------
let tmQueue = Promise.resolve();

function tmRequest<T>(fn: () => Promise<T>): Promise<T> {
    const result: Promise<T> = tmQueue.then(async () => {
        await sleep(FETCH_DELAY_MS);
        return fn();
    });
    // Chain off the settled promise (ignore errors so the queue keeps moving)
    tmQueue = result.then(() => {}, () => {});
    return result;
}

// ---------------------------------------------------------------------------
// Step 1 — Scrape HTML profile page
// ---------------------------------------------------------------------------

async function fetchProfileHtml(tmUrl: string): Promise<string | null> {
    return tmRequest(async () => {
        const maxRetries = USE_PROXY ? 2 : 1;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                let res;
                if (USE_PROXY) {
                    // Route through ProxyCrawl Crawling API on RapidAPI
                    res = await axios.get('https://proxycrawl-crawling.p.rapidapi.com/', {
                        params: { url: tmUrl },
                        headers: {
                            'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com',
                            'x-rapidapi-key': RAPIDAPI_KEY,
                        },
                        timeout: 30000,
                    });
                } else {
                    // Direct request (works from residential IPs)
                    res = await axios.get(tmUrl, { headers: TM_HEADERS, timeout: 15000 });
                }
                // Check if we got a CAPTCHA / block page
                const html = res.data as string;
                if (html.includes('Access Denied') || html.includes('captcha')) {
                    console.warn(`  [BLOCKED] ${tmUrl} — anti-bot page detected`);
                    if (attempt < maxRetries - 1) {
                        const delay = (attempt + 1) * 5000;
                        console.warn(`  [RETRY ${attempt+1}/${maxRetries}] waiting ${delay}ms...`);
                        await sleep(delay);
                        continue;
                    }
                    return null;
                }
                return html;
            } catch (e: any) {
                const status = e.response?.status;
                if ((status === 403 || status === 429 || status === 503) && attempt < maxRetries - 1) {
                    const delay = (attempt + 1) * 5000;
                    console.warn(`  [RETRY ${attempt+1}/${maxRetries}] HTTP ${status} — waiting ${delay}ms...`);
                    await sleep(delay);
                    continue;
                }
                console.warn(`  [HTTP ${status || 'ERR'}] ${tmUrl}: ${e.message}`);
                return null;
            }
        }
        return null;
    });
}

function parseProfileHtml(html: string, tmUrl: string): Partial<TmData> {
    const $ = cheerio.load(html);
    const data: Partial<TmData> = {};

    // ---- Name ----
    const headlineEl = $('h1.data-header__headline-wrapper');
    // Remove the shirt number span to get clean name
    headlineEl.find('.data-header__shirt-number').remove();
    data.displayName = cleanText(headlineEl.text());

    // Full legal name from facts table
    data.fullName = cleanText(
        $('.info-table span.info-table__content--bold').filter((_, el) => {
            const label = cleanText($(el).prev('span.info-table__content--regular').text());
            return label.toLowerCase().includes('name in home');
        }).first().text()
    ) || data.displayName;

    // ---- Shirt number ----
    data.shirtNumber = cleanText($('.data-header__shirt-number').first().text()).replace('#', '');

    // ---- Header data-header labels ----
    const headerLabels: Record<string, string> = {};
    $('li.data-header__label, span.data-header__label').each((_, el) => {
        const label = cleanText($(el).clone().children().remove().end().text()).toLowerCase();
        const value = cleanText($(el).find('.data-header__content').text());
        if (label && value) headerLabels[label] = value;
    });

    // Active players: club is an <a> inside .data-header__club
    // Retired players: .data-header__club contains plain text "Retired" with no <a>
    const clubAnchor = $('.data-header__club a').first();
    data.clubName    = cleanText(clubAnchor.attr('title') || clubAnchor.text() || $('.data-header__club').first().text());
    const clubHref   = clubAnchor.attr('href') || '';
    data.clubLink    = clubHref ? `https://www.transfermarkt.com${clubHref}` : '';
    data.contractExpiry = headerLabels['contract expires'] || '';

    // League
    data.leagueNames = cleanText($('.data-header__league a').first().text());

    // Market value
    data.marketValue     = parseMarketValue($);
    data.marketValueDate = cleanText($('.data-header__last-update').text()).replace(/last update[:\s]*/i, '');

    // ---- Facts & data info-table ----
    $('.info-table__content--regular').each((_, el) => {
        const label     = cleanText($(el).text()).toLowerCase();
        const boldEl    = $(el).next('.info-table__content--bold');
        const value     = cleanText(boldEl.text());

        if (label.includes('date of birth'))  data.dateOfBirth  = value.replace(/\(\d+\)/, '').trim();
        if (label.includes('place of birth')) {
            data.placeOfBirth = value;
            // Extract birth country from the flag img title attribute
            const countryName = boldEl.find('img').first().attr('title') || '';
            data.birthCountry = countryName;
        }
        if (label.includes('height'))         data.height       = formatHeight(value);
        if (label.includes('citizenship'))    data.citizenships = value;
        if (label.includes('position'))       data.position     = value;
        if (label.includes('foot'))           data.foot         = formatFoot(value);
        if (label.includes('agent')) {
            data.agentName = value;
            const agentHref = boldEl.find('a[href*="/berater/"]').first().attr('href') || '';
            data.agentLink  = agentHref ? `https://www.transfermarkt.com${agentHref}` : '';
        }
    });

    // ---- Social media ----
    // Only look inside the info-table to avoid picking up Transfermarkt's own
    // footer/header social links (e.g. @transfermarkt_official, @TMuk_news)
    const isTmAccount = (url: string) => url.toLowerCase().includes('transfermarkt');

    $('.info-table__content--bold a[href]').each((_, el) => {
        const href  = $(el).attr('href') || '';
        const lower = href.toLowerCase();
        if (isTmAccount(href)) return; // skip TM's own accounts
        if (lower.includes('instagram.com'))                      data.instagram = href;
        else if (lower.includes('twitter.com') || lower.includes('x.com')) data.twitter = href;
        else if (lower.includes('facebook.com'))                  data.facebook  = href;
        else if (lower.includes('tiktok.com'))                    data.tiktok    = href;
        else if (!lower.includes('transfermarkt') && href.startsWith('http')) data.website = href; // official site only
    });

    // ---- International ----
    // "Current international" label
    const intlEl = $('li.data-header__label').filter((_, el) =>
        cleanText($(el).text()).toLowerCase().includes('current international')
    ).first();
    data.currentIntlTeam = cleanText(intlEl.find('a').first().text());

    const capsGoalsEl = $('li.data-header__label').filter((_, el) =>
        cleanText($(el).text()).toLowerCase().includes('caps/goals')
    ).first();
    const capsGoalsLinks = capsGoalsEl.find('a');
    data.nationalCaps  = cleanText(capsGoalsLinks.eq(0).text());
    data.nationalGoals = cleanText(capsGoalsLinks.eq(1).text());

    // ---- Trophies / Awards ----
    const trophyTitles: string[] = [];
    $('.data-header__success-data img[title]').each((_, el) => {
        const t = $(el).attr('title');
        if (t) trophyTitles.push(cleanText(t));
    });
    // Also try text-based trophy list
    if (trophyTitles.length === 0) {
        $('.data-header__success-data span').each((_, el) => {
            const t = cleanText($(el).text());
            if (t) trophyTitles.push(t);
        });
    }
    data.trophies = trophyTitles.join(', ');

    // ---- Headshot ----
    // Player headshot is .data-header__profile-image — NOT the club crest in .data-header__box--big
    const headshotEl = $('img.data-header__profile-image').first();
    data.headshot = headshotEl.attr('src') || headshotEl.attr('data-src') || '';

    return data;
}

// ---------------------------------------------------------------------------
// Step 2 — Fetch performance stats from ceapi
// ---------------------------------------------------------------------------

interface PerfEntry {
    competition:  string;
    season:       string;
    apps:         number;
    goals:        number;
    assists:      number;
    yellowCards:  number;
    redCards:     number;
    minutesPlayed: number;
}

async function fetchPerformance(playerId: string): Promise<{ entries: PerfEntry[]; totals: { apps: number; goals: number; assists: number } }> {
    return tmRequest(async () => {
        const maxRetries = USE_PROXY ? 2 : 1;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                const url = `https://www.transfermarkt.com/ceapi/player/performance/${playerId}`;
                let res;
                if (USE_PROXY) {
                    res = await axios.get('https://proxycrawl-crawling.p.rapidapi.com/', {
                        params: { url },
                        headers: {
                            'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com',
                            'x-rapidapi-key': RAPIDAPI_KEY,
                        },
                        timeout: 30000,
                    });
                } else {
                    res = await axios.get(url, { headers: TM_JSON_HEADERS, timeout: 10000 });
                }

                // If ProxyCrawl fails, it might return an HTML string showing Cloudflare / Captcha
                if (typeof res.data === 'string' && (res.data.includes('Access Denied') || res.data.includes('captcha'))) {
                    console.warn(`  [PERF BLOCKED] player ${playerId} — anti-bot detected`);
                    if (attempt < maxRetries - 1) { await sleep((attempt + 1) * 3000); continue; }
                    throw new Error('Blocked by anti-bot');
                }

                const rows: any[] = res.data?.performances || res.data || [];

        const entries: PerfEntry[] = rows.map((r: any) => ({
            competition:   r.competitionDescription || r.competition || '',
            season:        r.nameSeason || r.season || '',
            apps:          parseInt(r.gamesPlayed || r.appearances || 0),
            goals:         parseInt(r.goalsScored || r.goals || 0),
            assists:       parseInt(r.assists || 0),
            yellowCards:   parseInt(r.yellowCards || 0),
            redCards:      parseInt(r.redCards || 0),
            minutesPlayed: parseInt(r.minutesPlayed || 0),
        }));

        const totals = entries.reduce((acc, e) => ({
            apps:    acc.apps    + e.apps,
            goals:   acc.goals   + e.goals,
            assists: acc.assists + e.assists,
        }), { apps: 0, goals: 0, assists: 0 });

        return { entries, totals };
            } catch (e: any) {
                const status = e.response?.status;
                if ((status === 403 || status === 429 || status === 502 || status === 503) && attempt < maxRetries - 1) {
                    await sleep((attempt + 1) * 3000);
                    continue;
                }
                console.warn(`  [PERF ERR] player ${playerId}:`, status || e.message);
                return { entries: [], totals: { apps: 0, goals: 0, assists: 0 } };
            }
        }
        return { entries: [], totals: { apps: 0, goals: 0, assists: 0 } };
    });
}

// ---------------------------------------------------------------------------
// Step 3 — Fetch gallery images from the Transfermarkt internal API
// ---------------------------------------------------------------------------
// Gallery images are rendered by a Svelte web component (<tm-image-gallery>)
// that calls: GET https://tmapi-alpha.transfermarkt.technology/player/{id}/gallery
// Response: { data: { images: [ { url, title, source, isPremium } ] } }

async function fetchGalleryImages(playerId: string): Promise<string[]> {
    return tmRequest(async () => {
        const maxRetries = USE_PROXY ? 2 : 1;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                const url = `https://tmapi-alpha.transfermarkt.technology/player/${playerId}/gallery`;
                let res;
                if (USE_PROXY) {
                    res = await axios.get('https://proxycrawl-crawling.p.rapidapi.com/', {
                        params: { url },
                        headers: {
                            'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com',
                            'x-rapidapi-key': RAPIDAPI_KEY,
                        },
                        timeout: 30000,
                    });
                } else {
                    res = await axios.get(url, {
                        headers: { ...TM_JSON_HEADERS, 'Origin': 'https://www.transfermarkt.com' },
                        timeout: 10000,
                    });
                }
                
                if (typeof res.data === 'string' && (res.data.includes('Access Denied') || res.data.includes('captcha'))) {
                    console.warn(`  [GALLERY BLOCKED] player ${playerId} — anti-bot detected`);
                    if (attempt < maxRetries - 1) { await sleep((attempt + 1) * 3000); continue; }
                    throw new Error('Blocked by anti-bot');
                }

                const images: any[] = res.data?.data?.images || [];
                return images.map((img: any) => img.url).filter(Boolean);
            } catch (e: any) {
                const status = e.response?.status;
                if ((status === 403 || status === 429 || status === 502 || status === 503) && attempt < maxRetries - 1) {
                    await sleep((attempt + 1) * 3000);
                    continue;
                }
                console.warn(`  [GALLERY ERR] player ${playerId}:`, status || e.message);
                return [];
            }
        }
        return [];
    });
}

// ---------------------------------------------------------------------------
// Airtable — fetch records from the view
// ---------------------------------------------------------------------------

async function fetchAirtableRecords(): Promise<AirtableRecord[]> {
    console.log(`📡 Fetching Airtable records from "Transfermarket Get" view...`);
    const allRecords: AirtableRecord[] = [];
    let offset: string | undefined;

    do {
        const params: Record<string, any> = { view: AIRTABLE_VIEW, pageSize: 100 };
        if (offset) params.offset = offset;
        const res = await airtable.get('', { params });
        allRecords.push(...res.data.records);
        offset = res.data.offset;
        if (PROFILE_LIMIT > 0 && allRecords.length >= PROFILE_LIMIT) break;
    } while (offset);

    const limited = PROFILE_LIMIT > 0 ? allRecords.slice(0, PROFILE_LIMIT) : allRecords;
    console.log(`✅ Found ${limited.length} records to process.\n`);
    return limited;
}

// ---------------------------------------------------------------------------
// Airtable — batch update
// ---------------------------------------------------------------------------

let updateQueue: Array<{ id: string; fields: Record<string, any> }> = [];
let flushLock = false;

async function flushUpdates(force = false): Promise<void> {
    if (flushLock) return;
    if (!force && updateQueue.length < AIRTABLE_BATCH) return;
    flushLock = true;
    try {
        while (updateQueue.length >= AIRTABLE_BATCH || (force && updateQueue.length > 0)) {
            const batch = updateQueue.splice(0, AIRTABLE_BATCH);
            try {
                await airtable.patch('', { records: batch });
                console.log(`💾 Flushed ${batch.length} records to Airtable.`);
            } catch (err: any) {
                console.error('❌ Airtable batch error:', err.response?.data || err.message);
                updateQueue.unshift(...batch);
                break;
            }
        }
    } finally {
        flushLock = false;
    }
}

// ---------------------------------------------------------------------------
// Process a single player record
// ---------------------------------------------------------------------------

async function processRecord(record: AirtableRecord, index: number, total: number): Promise<void> {
    const tmUrl = cleanText(record.fields['SOC Transfermarkt'] || record.fields['TM Link'] || '');
    if (!tmUrl) {
        console.log(`[SKIP ${index+1}/${total}] No TM URL`);
        return;
    }

    const normUrl  = normaliseTmUrl(tmUrl);
    const playerId = extractPlayerId(normUrl);
    const slug     = extractSlug(normUrl);

    if (!playerId || !slug) {
        console.log(`[SKIP ${index+1}/${total}] Can't parse URL: ${tmUrl}`);
        return;
    }

    console.log(`\n🔍 [${index+1}/${total}] ${slug} (${playerId})`);

    try {
        // Fetch sequentially — parallel requests trigger TM's bot detection
        const profileUrl = `https://www.transfermarkt.com/${slug}/profil/spieler/${playerId}`;
        const html        = await fetchProfileHtml(profileUrl);
        const perfData    = await fetchPerformance(playerId);
        const galleryUrls = await fetchGalleryImages(playerId);

        if (!html) {
            console.warn(`  ❌ Failed to fetch profile HTML`);
            updateQueue.push({ id: record.id, fields: {
                'TM Data Status': 'Error',
                'TM Last Check':  new Date().toISOString().split('T')[0],
            }});
            await flushUpdates();
            return;
        }

        const profile = parseProfileHtml(html, normUrl);

        // Build appearances summary string (most recent seasons first, top 10)
        let appearancesByComp = '';
        if (perfData.entries.length > 0) {
            const lines = perfData.entries
                .slice(0, 10)
                .map(e => `${e.season} ${e.competition}: ${e.apps} apps, ${e.goals}G, ${e.assists}A`);
            appearancesByComp = lines.join('\n');
        }

        if (galleryUrls.length > 0) console.log(`  🖼  Gallery: ${galleryUrls.length} images`);
        console.log(`  ✅ ${profile.displayName} | ${profile.clubName} | ${profile.marketValue} | ${perfData.totals.apps} apps`);

        // Map to Airtable fields
        // Note: TM Total Appearances / Goals / Assists are text fields — send as strings
        const fields: Record<string, any> = {
            'TM Full Name':                  profile.fullName            || '',
            'TM Display Name':               profile.displayName         || '',
            'TM Shirt Numbers':              profile.shirtNumber         || '',
            'TM Date of Birth':              profile.dateOfBirth         || '',
            'TM Place of Birth':             profile.placeOfBirth        || '',
            'TM CC':                         countryToCode(profile.birthCountry || ''),
            'TM Citizenships':               profile.citizenships        || '',
            'TM Height':                     profile.height              || '',
            'TM Position':                   profile.position            || '',
            'TM Preferred Foot':             profile.foot                || '',
            'TM Club Name':                  profile.clubName            || '',
            'TM Club Link':                  profile.clubLink            || '',
            'TM League Names':               profile.leagueNames         || '',
            'TM Contract Expiry Date':       profile.contractExpiry      || '',
            'TM Current Market Value':       profile.marketValue         || '',
            'TM Last Updated Date':          profile.marketValueDate     || '',
            'RM Current International Team': profile.currentIntlTeam     || '',
            'TM National Caps':              profile.nationalCaps        || '',
            'TM National Goals':             profile.nationalGoals       || '',
            'TM Trophies won':               profile.trophies            || '',
            'TM Player Agent/Agency Name':   profile.agentName           || '',
            'TM Player Agent/Agency Link':   profile.agentLink           || '',
            'TM Headshot':                   profile.headshot            || '',
            'TM Images (gallery)':           galleryUrls.join('\n'),
            'TM Instagram':                  profile.instagram           || '',
            'TM Facebook':                   profile.facebook            || '',
            'TM Tiktok':                     profile.tiktok              || '',
            'TM Twitter':                    profile.twitter             || '',
            'TM Website':                    profile.website             || '',
            'TM Appearances by comp':        appearancesByComp           || '',
            'TM Total Appearances':          String(perfData.totals.apps    || ''),
            'TM Goals':                      String(perfData.totals.goals   || ''),
            'TM Assists':                    String(perfData.totals.assists || ''),
            'TM Data Status':                'Complete',
            'TM Last Check':                 new Date().toISOString().split('T')[0],
            'TM Update':                     new Date().toISOString(),
        };

        updateQueue.push({ id: record.id, fields });
        await flushUpdates();

    } catch (e: any) {
        console.error(`  [FAIL] ${slug}:`, e.message);
        updateQueue.push({ id: record.id, fields: {
            'TM Data Status': 'Error',
            'TM Last Check':  new Date().toISOString().split('T')[0],
        }});
        await flushUpdates();
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Health check — verify TM is accessible before processing batch
// ---------------------------------------------------------------------------

async function healthCheck(): Promise<boolean> {
    console.log('\n🏥 Running health check...');
    const testUrl = 'https://www.transfermarkt.com/kylian-mbappe/profil/spieler/342229';
    try {
        let res;
        if (USE_PROXY) {
            res = await axios.get('https://proxycrawl-crawling.p.rapidapi.com/', {
                params: { url: testUrl },
                headers: {
                    'x-rapidapi-host': 'proxycrawl-crawling.p.rapidapi.com',
                    'x-rapidapi-key': RAPIDAPI_KEY,
                },
                timeout: 30000,
            });
        } else {
            res = await axios.get(testUrl, { headers: TM_HEADERS, timeout: 15000 });
        }
        const html = res.data as string;
        const isBlocked = html.includes('captcha') || html.includes('Access Denied');
        if (isBlocked) {
            console.error('❌ Health check: TM returned CAPTCHA / block page');
            return false;
        }
        const hasContent = html.includes('data-header__headline-wrapper');
        if (!hasContent) {
            console.warn('⚠️  Health check: page loaded but missing expected selectors');
        }
        console.log('✅ Health check passed — TM is accessible via ' + (USE_PROXY ? 'proxy' : 'direct'));
        return true;
    } catch (e: any) {
        console.error(`❌ Health check failed: ${e.response?.status || e.message}`);
        return false;
    }
}

async function run(): Promise<void> {
    const startTime = Date.now();
    console.log('==========================================');
    console.log('⚽ Transfermarkt Enrichment Scraper');
    console.log(`   Concurrency: ${CONCURRENCY} | Limit: ${PROFILE_LIMIT > 0 ? PROFILE_LIMIT : 'all'} | Delay: ${FETCH_DELAY_MS}ms`);
    console.log(`   Proxy: ${USE_PROXY ? 'RapidAPI ProxyCrawl' : 'DIRECT (no proxy)'}`);
    console.log('==========================================\n');

    // Health check before processing
    const healthy = await healthCheck();
    if (!healthy) {
        console.error('\n🛑 Aborting — TM is not accessible. Check proxy / API key.');
        process.exit(1);
    }

    const records = await fetchAirtableRecords();
    if (records.length === 0) {
        console.log('No records to process.');
        return;
    }

    const limit = createLimiter(CONCURRENCY);

    await Promise.all(
        records.map((record, i) =>
            limit(() => processRecord(record, i, records.length))
        )
    );

    await flushUpdates(true);

    const secs = Math.round((Date.now() - startTime) / 1000);
    console.log(`\n✅ Done in ${secs}s`);
}

run().catch(e => {
    console.error('Fatal:', e);
    process.exit(1);
});
