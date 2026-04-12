import * as dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import * as cheerio from 'cheerio';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const AIRTABLE_API_KEY  = process.env.AIRTABLE_API_KEY!;
const AIRTABLE_BASE     = process.env.AIRTABLE_BASE     || 'apprT24SuAvV8oZXX';
const AIRTABLE_TABLE    = process.env.AIRTABLE_TABLE    || 'tblKxel0FfAjklhPe';
const AIRTABLE_VIEW     = process.env.AIRTABLE_VIEW     || 'viwO4B0htcTlCH69M'; // "Transfermarket Get"
const PROFILE_LIMIT     = parseInt(process.env.PROFILE_LIMIT || '0');           // 0 = all
const CONCURRENCY       = parseInt(process.env.CONCURRENCY   || '3');
const FETCH_DELAY_MS    = parseInt(process.env.FETCH_DELAY   || '1500');        // polite delay per request

const AIRTABLE_BATCH    = 10; // Airtable max patch size

// ---------------------------------------------------------------------------
// HTTP clients
// ---------------------------------------------------------------------------

const TM_HEADERS = {
    'User-Agent':      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
};

const TM_JSON_HEADERS = {
    ...TM_HEADERS,
    'Accept': 'application/json, text/plain, */*',
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

// Parse market value string like "€50.00m" or "€800k" → keep as string
function parseMarketValue($: cheerio.CheerioAPI): string {
    const raw = cleanText($('.data-header__market-value-wrapper').first().text());
    // Remove the "Last update:..." suffix
    return raw.replace(/Last update:.*/i, '').trim();
}

// ---------------------------------------------------------------------------
// Step 1 — Scrape HTML profile page
// ---------------------------------------------------------------------------

async function fetchProfileHtml(tmUrl: string): Promise<string | null> {
    await sleep(FETCH_DELAY_MS);
    try {
        const res = await axios.get(tmUrl, { headers: TM_HEADERS, timeout: 15000 });
        return res.data as string;
    } catch (e: any) {
        console.warn(`  [HTTP ${e.response?.status || 'ERR'}] ${tmUrl}: ${e.message}`);
        return null;
    }
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

    data.clubName       = cleanText($('.data-header__club a').first().attr('title') || $('.data-header__club a').first().text());
    const clubHref      = $('.data-header__club a').first().attr('href') || '';
    data.clubLink       = clubHref ? `https://www.transfermarkt.com${clubHref}` : '';
    data.contractExpiry = headerLabels['contract expires'] || '';

    // League
    data.leagueNames = cleanText($('.data-header__league a').first().text());

    // Market value
    data.marketValue     = parseMarketValue($);
    data.marketValueDate = cleanText($('.data-header__last-update').text()).replace(/last update[:\s]*/i, '');

    // ---- Facts & data info-table ----
    const infoRows: Array<[string, string]> = [];
    $('.info-table__content--regular').each((_, el) => {
        const label = cleanText($(el).text()).toLowerCase();
        const value = cleanText($(el).next('.info-table__content--bold').text());
        infoRows.push([label, value]);
    });

    for (const [label, value] of infoRows) {
        if (label.includes('date of birth'))   data.dateOfBirth  = value.replace(/\(\d+\)/, '').trim();
        if (label.includes('place of birth'))  data.placeOfBirth = value;
        if (label.includes('height'))          data.height       = value;
        if (label.includes('citizenship'))     data.citizenships = value;
        if (label.includes('position'))        data.position     = value;
        if (label.includes('foot'))            data.foot         = value;
        if (label.includes('joined'))          {} // not a column — skip
        if (label.includes('outfitter'))       {} // not a column — skip
        if (label.includes('agent')) {
            data.agentName = value;
            // Agent link
            const agentHref = $('.info-table__content--bold a[href*="/berater/"]').first().attr('href') || '';
            data.agentLink  = agentHref ? `https://www.transfermarkt.com${agentHref}` : '';
        }
    }

    // ---- Social media ----
    // Transfermarkt puts socials in a specific section of the info table
    $('.info-table__content--bold a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const lower = href.toLowerCase();
        if (lower.includes('instagram.com'))   data.instagram = href;
        else if (lower.includes('twitter.com') || lower.includes('x.com')) data.twitter = href;
        else if (lower.includes('facebook.com')) data.facebook = href;
        else if (lower.includes('tiktok.com')) data.tiktok = href;
    });

    // Also check ext links in "further information" / additional data boxes
    $('a[href]').each((_, el) => {
        const href = ($(el).attr('href') || '').toLowerCase();
        if (!data.instagram && href.includes('instagram.com'))   data.instagram = $(el).attr('href')!;
        if (!data.twitter   && (href.includes('twitter.com') || href.includes('x.com'))) data.twitter = $(el).attr('href')!;
        if (!data.facebook  && href.includes('facebook.com'))   data.facebook  = $(el).attr('href')!;
        if (!data.tiktok    && href.includes('tiktok.com'))     data.tiktok    = $(el).attr('href')!;
        if (!data.website   && href.includes('official') && !href.includes('transfermarkt')) data.website = $(el).attr('href')!;
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
    // Main profile photo — usually in .data-header__profile-image or similar
    const headshotEl = $('img.data-header__profile-image, .data-header__box--big img').first();
    data.headshot = headshotEl.attr('src') || headshotEl.attr('data-src') || '';

    // ---- Gallery images ----
    // Gallery section uses data-src on lazy-loaded <img> tags inside the gallery box
    const galleryUrls: string[] = [];
    $('div.gallery img, .gallery-slider img, section.gallery img').each((_, el) => {
        const src = $(el).attr('data-src') || $(el).attr('src') || '';
        if (src && src.includes('http') && !src.includes('placeholder')) {
            galleryUrls.push(src);
        }
    });
    // Fallback: look for the gallery section by heading text
    if (galleryUrls.length === 0) {
        $('h2').each((_, el) => {
            if (cleanText($(el).text()).toLowerCase() === 'gallery') {
                $(el).parent().find('img').each((_, img) => {
                    const src = $(img).attr('data-src') || $(img).attr('src') || '';
                    if (src && src.includes('http')) galleryUrls.push(src);
                });
            }
        });
    }
    data.images = galleryUrls.join('\n');

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
    await sleep(FETCH_DELAY_MS);
    try {
        const url = `https://www.transfermarkt.com/ceapi/player/performance/${playerId}`;
        const res = await axios.get(url, { headers: TM_JSON_HEADERS, timeout: 10000 });
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
        console.warn(`  [PERF ERR] player ${playerId}:`, e.response?.status || e.message);
        return { entries: [], totals: { apps: 0, goals: 0, assists: 0 } };
    }
}

// ---------------------------------------------------------------------------
// Step 3 — Fetch gallery images from the /galerie page (separate URL)
// ---------------------------------------------------------------------------

async function fetchGalleryImages(slug: string, playerId: string): Promise<string[]> {
    await sleep(FETCH_DELAY_MS);
    try {
        const url = `https://www.transfermarkt.com/${slug}/galerie/spieler/${playerId}`;
        const res = await axios.get(url, { headers: TM_HEADERS, timeout: 10000 });
        const $ = cheerio.load(res.data as string);

        const urls: string[] = [];

        // Gallery thumbnails — full-size links are in the <a href> wrapping each img
        $('a[href*="/galerie/"], a[data-fancybox]').each((_, el) => {
            const href = $(el).attr('href') || '';
            // Try to get the full-res image from the anchor href (sometimes direct image link)
            if (href.match(/\.(jpg|jpeg|png|webp)/i)) {
                urls.push(href.startsWith('http') ? href : `https://www.transfermarkt.com${href}`);
            }
        });

        // Fallback: lazy-loaded img data-src inside gallery
        if (urls.length === 0) {
            $('img[data-src], img[src]').each((_, el) => {
                const src = $(el).attr('data-src') || $(el).attr('src') || '';
                // Filter out tiny thumbnails/icons — only keep actual photos
                if (src && src.includes('images.transfermarkt') && !src.includes('tiny') && !src.includes('icon')) {
                    // Try to convert thumb URL to full-size
                    const fullSize = src
                        .replace('/thumb/', '/medium/')
                        .replace('/small/', '/big/')
                        .replace('_thumbnail', '');
                    urls.push(fullSize);
                }
            });
        }

        return [...new Set(urls)]; // deduplicate
    } catch (e: any) {
        console.warn(`  [GALLERY ERR] ${slug}/${playerId}:`, e.response?.status || e.message);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Airtable — fetch records from the view
// ---------------------------------------------------------------------------

async function fetchAirtableRecords(): Promise<AirtableRecord[]> {
    console.log(`📡 Fetching Airtable records from "Transfermarket Get" view...`);
    const allRecords: AirtableRecord[] = [];
    let offset: string | undefined;

    do {
        const params: Record<string, any> = { view: AIRTABLE_VIEW, maxRecords: 100 };
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
        // Fetch profile HTML + performance data in parallel
        const profileUrl = `https://www.transfermarkt.com/${slug}/profil/spieler/${playerId}`;
        const [html, perfData] = await Promise.all([
            fetchProfileHtml(profileUrl),
            fetchPerformance(playerId),
        ]);

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

        // Fetch gallery separately if profile parse didn't find images
        let galleryImages = profile.images || '';
        if (!galleryImages) {
            const imgs = await fetchGalleryImages(slug, playerId);
            galleryImages = imgs.join('\n');
            if (imgs.length > 0) console.log(`  🖼  Gallery: ${imgs.length} images`);
        }

        // Build appearances summary string (most recent seasons first, top 10)
        let appearancesByComp = '';
        if (perfData.entries.length > 0) {
            const lines = perfData.entries
                .slice(0, 10)
                .map(e => `${e.season} ${e.competition}: ${e.apps} apps, ${e.goals}G, ${e.assists}A`);
            appearancesByComp = lines.join('\n');
        }

        console.log(`  ✅ ${profile.displayName} | ${profile.clubName} | ${profile.marketValue} | ${perfData.totals.apps} apps`);

        // Map to Airtable fields
        const fields: Record<string, any> = {
            'TM Full Name':               profile.fullName         || '',
            'TM Display Name':            profile.displayName      || '',
            'TM Shirt Numbers':           profile.shirtNumber      || '',
            'TM Date of Birth':           profile.dateOfBirth      || '',
            'TM Place of Birth':          profile.placeOfBirth     || '',
            'TM Citizenships':            profile.citizenships     || '',
            'TM Height':                  profile.height           || '',
            'TM Position':                profile.position         || '',
            'TM Preferred Foot':          profile.foot             || '',
            'TM Club Name':               profile.clubName         || '',
            'TM Club Link':               profile.clubLink         || '',
            'TM League Names':            profile.leagueNames      || '',
            'TM Contract Expiry Date':    profile.contractExpiry   || '',
            'TM Current Market Value':    profile.marketValue      || '',
            'TM Last Updated Date':       profile.marketValueDate  || '',
            'RM Current International Team': profile.currentIntlTeam || '',
            'TM National Caps':           profile.nationalCaps     || '',
            'TM National Goals':          profile.nationalGoals    || '',
            'TM Trophies won':            profile.trophies         || '',
            'TM Player Agent/Agency Name': profile.agentName       || '',
            'TM Player Agent/Agency Link': profile.agentLink       || '',
            'TM Headshot':                profile.headshot         || '',
            'TM Images (gallery)':         galleryImages            || '',
            'TM Instagram':               profile.instagram        || '',
            'TM Facebook':                profile.facebook         || '',
            'TM Tiktok':                  profile.tiktok           || '',
            'TM Twitter':                 profile.twitter          || '',
            'TM Website':                 profile.website          || '',
            'TM Appearances by comp':     appearancesByComp        || '',
            'TM Total Appearances':       perfData.totals.apps     || 0,
            'TM Goals':                   perfData.totals.goals    || 0,
            'TM Assists':                 perfData.totals.assists  || 0,
            'TM Data Status':             'Complete',
            'TM Last Check':              new Date().toISOString().split('T')[0],
            'TM Update':                  new Date().toISOString(),
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

async function run(): Promise<void> {
    const startTime = Date.now();
    console.log('==========================================');
    console.log('⚽ Transfermarkt Enrichment Scraper');
    console.log(`   Concurrency: ${CONCURRENCY} | Limit: ${PROFILE_LIMIT > 0 ? PROFILE_LIMIT : 'all'} | Delay: ${FETCH_DELAY_MS}ms`);
    console.log('==========================================\n');

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
