import * as THREE from 'three';
import { analyzeGeometry, inferObjectType } from './geometryAnalysis.js';

const GEMINI_API_KEY = import.meta.env.VITE_GEMINI_API_KEY;

// Lazy geometry-analysis cache: filename → analysis result
// Cleared whenever the scene is reloaded.
const _geoCache = new Map();

/**
 * Run geometry analysis for a single loaded file (cached).
 * @param {string} filename
 * @param {object} fileData
 */
function getGeoAnalysis(filename, fileData) {
    if (!fileData || !fileData.geometry) return null;
    if (_geoCache.has(filename)) return _geoCache.get(filename);
    try {
        const result = analyzeGeometry(fileData.geometry, filename);
        _geoCache.set(filename, result);
        return result;
    } catch (e) {
        console.warn('[GeoAnalysis] Failed for', filename, e);
        return null;
    }
}

/** Invalidate analysis cache (call when scene reloads). */
export function clearGeoAnalysisCache() { _geoCache.clear(); }

export function createQueryHandler(app, sceneManager, ui) {
    const queryCache = new Map();
    let isQueryInFlight = false;
    let currentAbortController = null;
    const conversationHistory = [];
    let lastReferencedFilename = null;

    const SEND_ICON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z" /></svg>`;
    const PAUSE_ICON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 5v14M16 5v14" /></svg>`;

    const handler = {
        getSceneMetadata,
        localQueryHandler,
        handleQuerySend,
        normalizeQuestion,
        generateCSVFromScene
    };

    return handler;

    function normalizeQuestion(q) {
        return q.trim().toLowerCase();
    }

    function addHistory(role, text) {
        conversationHistory.push({ role, text: String(text || '') });
        if (conversationHistory.length > 20) {
            conversationHistory.shift();
        }
    }

    function detectFilenameMention(text) {
        if (!text) return null;
        const lower = String(text).toLowerCase();
        for (const [filename] of app.loadedFiles.entries()) {
            const fLower = filename.toLowerCase();
            const noExt = fLower.replace(/\.ply$/i, '');
            if (lower.includes(fLower) || lower.includes(noExt)) {
                return filename;
            }
        }
        return null;
    }

    function enrichQuestionWithSessionReference(question) {
        const hasPronounRef = /\b(it|that|this|that one|this one|them|those)\b/i.test(question);
        if (!hasPronounRef || !lastReferencedFilename) return question;
        return `${question}\n\nContext: In this session, pronouns like "it/that/this/them" refer to '${lastReferencedFilename}'.`;
    }

    function getSceneMetadata() {
        const files = [];
        app.loadedFiles.forEach((fileData, filename) => {
            if (!fileData.geometry) return;
            const geometry = fileData.geometry;
            geometry.computeBoundingBox();
            const bbox   = geometry.boundingBox;
            const center = new THREE.Vector3();
            bbox.getCenter(center);

            // Run geometry analysis (cached)
            const geo = getGeoAnalysis(filename, fileData);

            files.push({
                filename,
                visible:      !!fileData.visible,
                vertex_count: geometry.attributes.position.count,
                object_type:  geo?.objectType  ?? inferObjectType(filename),
                height:       geo?.height       ?? null,
                diameter:     geo?.diameter     ?? null,
                volume:       geo?.volume       ?? null,
                surface_area: geo?.surface_area ?? null,
                bbox: {
                    min:    bbox.min.toArray(),
                    max:    bbox.max.toArray(),
                    size:   bbox.getSize(new THREE.Vector3()).toArray(),
                    center: center.toArray(),
                },
            });
        });
        return files;
    }

    function localQueryHandler(question) {
        const q = normalizeQuestion(question);
        if (queryCache.has(q)) return { handled: true, data: queryCache.get(q) };
        const sceneFiles = getSceneMetadata(); const filenamesLower = sceneFiles.map(f => f.filename.toLowerCase());
        const responseData = { success: true, question, sql: null, results: [], columns: [], row_count: 0 };
        let match = q.match(/how many (?:of )?([\w\s-]+)s?\b/);
        if (!match) match = q.match(/count (?:the )?(\w+)s?\b/);
        if (match) {
            let object = match[1]; object = object.trim().toLowerCase(); if (object.endsWith('s')) object = object.slice(0, -1);
            const count = filenamesLower.filter(f => f.includes(object)).length; responseData.results = [{ object, count }]; responseData.columns = ['object', 'count']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData };
        }
        match = q.match(/is there (?:a|an|the )?([\w\s-]+)/);
        if (match) {
            let object = match[1]; object = object.trim().toLowerCase(); if (object.endsWith('s')) object = object.slice(0, -1);
            let exists = filenamesLower.some(f => f.includes(object));
            if (app.sceneInfo && app.sceneInfo._map) {
                const lower = object.toLowerCase(); const entry = app.sceneInfo._map.get(lower); exists = Boolean(entry) || sceneFiles.some(f => f.filename.toLowerCase().includes(lower));
                if (entry) {
                    responseData.results = [{ object, exists: true, filename: entry.filename }]; responseData.columns = ['object', 'exists', 'filename']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData };
                }
            }
            responseData.results = [{ object, exists }]; responseData.columns = ['object', 'exists']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData };
        }
        match = q.match(/where is (?:a|an|the )?([\w\s-]+)/);
        if (match) {
            let object = match[1]; object = object.trim().toLowerCase(); if (object.endsWith('s')) object = object.slice(0, -1);
            if (app.sceneInfo && app.sceneInfo._map) {
                const lower = object.toLowerCase(); let entry = app.sceneInfo._map.get(lower);
                if (!entry) { for (const [k, v] of app.sceneInfo._map.entries()) { if (k.includes(lower) || lower.includes(k)) { entry = v; break; } } }
                if (entry) {
                    let center = [0, 0, 0]; let size = [1, 1, 1];
                    if (entry.filename) {
                        const f = sceneFiles.find(ff => ff.filename.toLowerCase().includes(String(entry.filename).toLowerCase())); if (f) { center = f.bbox.center; size = f.bbox.size; }
                    }
                    const bboxInfo = app.sceneInfo.bounding_box && app.sceneInfo.bounding_box[entry.key]; if (bboxInfo) size = [bboxInfo.x || size[0], bboxInfo.y || size[1], bboxInfo.z || size[2]];
                    responseData.results = [{ object: entry.key, center, size, filename: entry.filename }]; responseData.columns = ['object', 'center', 'size']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData };
                } else { responseData.results = [{ object, exists: false }]; responseData.columns = ['object', 'exists']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData }; }
            } else { let file = sceneFiles.find(f => f.filename.toLowerCase().includes(object)); if (file) { responseData.results = [{ object, center: file.bbox.center, size: file.bbox.size, filename: file.filename }]; responseData.columns = ['object', 'center', 'size']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData }; } }
        }
        match = q.match(/vertex count (?:of )?(?:the )?([\w\s-]+)|how many vertices (?:in|for) ([\w\s-]+)/);
        if (match) {
            const object = (match[1] || match[2] || '').trim().toLowerCase(); const file = sceneFiles.find(f => f.filename.toLowerCase().includes(object)); if (file) { responseData.results = [{ object: file.filename, vertex_count: file.vertex_count }]; responseData.columns = ['object', 'vertex_count']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData }; }
        }
        return { handled: false };
    }

    /**
     * Generate CSV content from scene metadata.
     *
     * Supported columns:
     *   object, object_type, position_x, position_y, position_z,
     *   size_x, size_y, size_z, height, diameter, volume, surface_area,
     *   vertex_count, visible
     *
     * @param {string[]} requestedColumns
     * @param {string}   [filterType]  – optional object type filter ('tank'|'pipe'|'pump')
     * @returns {{ csvContent: string, filename: string, previewRows: string[][], columns: string[], totalRows: number }}
     */
    function generateCSVFromScene(requestedColumns, filterType) {
        const sceneFiles = getSceneMetadata();

        const ALL_COLS = [
            'object', 'object_type',
            'position_x', 'position_y', 'position_z',
            'size_x', 'size_y', 'size_z',
            'height', 'diameter', 'volume', 'surface_area',
            'vertex_count', 'visible',
        ];

        let cols = (requestedColumns && requestedColumns.length > 0)
            ? requestedColumns.filter(c => ALL_COLS.includes(c))
            : ALL_COLS;
        if (cols.length === 0) cols = ALL_COLS;

        // Apply type filter if requested
        let filteredFiles = sceneFiles;
        if (filterType) {
            filteredFiles = sceneFiles.filter(f =>
                (f.object_type || '').toLowerCase() === filterType.toLowerCase()
            );
        }

        const rows = filteredFiles.map(f => {
            const name = f.filename.replace(/\.(?:ply|drc)$/i, '');
            const row = {};
            row['object']       = name;
            row['object_type']  = f.object_type  ?? '';
            row['position_x']   = f.bbox.center[0]?.toFixed(4) ?? '';
            row['position_y']   = f.bbox.center[1]?.toFixed(4) ?? '';
            row['position_z']   = f.bbox.center[2]?.toFixed(4) ?? '';
            row['size_x']       = f.bbox.size[0]?.toFixed(4)   ?? '';
            row['size_y']       = f.bbox.size[1]?.toFixed(4)   ?? '';
            row['size_z']       = f.bbox.size[2]?.toFixed(4)   ?? '';
            row['height']       = f.height       != null ? f.height.toFixed(4)       : '';
            row['diameter']     = f.diameter     != null ? f.diameter.toFixed(4)     : '';
            row['volume']       = f.volume       != null ? f.volume.toFixed(4)       : '';
            row['surface_area'] = f.surface_area != null ? f.surface_area.toFixed(4) : '';
            row['vertex_count'] = f.vertex_count ?? '';
            row['visible']      = f.visible ? 'yes' : 'no';
            return row;
        });

        const header   = cols.join(',');
        const csvRows  = rows.map(r => cols.map(c => {
            const val = String(r[c] ?? '');
            return val.includes(',') || val.includes('"') ? `"${val.replace(/"/g, '""')}"` : val;
        }).join(','));
        const csvContent = [header, ...csvRows].join('\n');
        const previewRows = rows.slice(0, 5).map(r => cols.map(c => String(r[c] ?? '')));
        const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const filename = filterType ? `${filterType}_data_${ts}.csv` : `scene_data_${ts}.csv`;

        return { csvContent, filename, previewRows, columns: cols, totalRows: rows.length };
    }

    async function geminiQueryHandler(question, sceneFiles, abortSignal) {
        if (!GEMINI_API_KEY) {
            return { handled: false, error: 'Gemini API Key not configured' };
        }

        try {
            // ── Build context ────────────────────────────────────────────────────
            // Group files by object type for concise summary
            const typeGroups = {};
            sceneFiles.forEach(f => {
                const t = f.object_type || 'unknown';
                if (!typeGroups[t]) typeGroups[t] = [];
                typeGroups[t].push(f.filename);
            });

            // Per-type aggregate stats (total surface area etc.)
            const typeStats = {};
            for (const [type, names] of Object.entries(typeGroups)) {
                const members = sceneFiles.filter(f => f.object_type === type);
                const totalSA  = members.reduce((s, f) => s + (f.surface_area ?? 0), 0);
                const totalVol = members.reduce((s, f) => s + (f.volume ?? 0), 0);
                typeStats[type] = {
                    count: members.length,
                    total_surface_area: parseFloat(totalSA.toFixed(4)),
                    total_volume:       parseFloat(totalVol.toFixed(4)),
                };
            }

            const context = {
                fileCount: sceneFiles.length,
                typeGroups,
                typeStats,
                files: sceneFiles.map(f => ({
                    filename:     f.filename,
                    object_type:  f.object_type,
                    visible:      f.visible,
                    vertices:     f.vertex_count,
                    position:     f.bbox.center,
                    size:         f.bbox.size,
                    height:       f.height     != null ? parseFloat(f.height.toFixed(4))       : null,
                    diameter:     f.diameter   != null ? parseFloat(f.diameter.toFixed(4))     : null,
                    volume:       f.volume     != null ? parseFloat(f.volume.toFixed(4))       : null,
                    surface_area: f.surface_area != null ? parseFloat(f.surface_area.toFixed(4)) : null,
                })),
                sceneMapping:         app.sceneInfo ? 'Available' : 'Not available',
                selectedObject:       app.selectedFile || null,
                lastReferencedObject: lastReferencedFilename,
                recentConversation:   conversationHistory.slice(-8),
                calculated_distances: [],
            };

            // If a specific object is selected, add its full analysis to context
            if (app.selectedFile) {
                const fd = app.loadedFiles.get(app.selectedFile);
                const geo = getGeoAnalysis(app.selectedFile, fd);
                if (geo) {
                    context.selectedObjectAnalysis = {
                        filename:     app.selectedFile,
                        object_type:  geo.objectType,
                        height:       parseFloat(geo.height.toFixed(4)),
                        diameter:     parseFloat(geo.diameter.toFixed(4)),
                        radius:       parseFloat(geo.radius.toFixed(4)),
                        volume:       geo.volume     != null ? parseFloat(geo.volume.toFixed(4))       : null,
                        surface_area: geo.surface_area != null ? parseFloat(geo.surface_area.toFixed(4)) : null,
                    };
                }
            }

            // Dynamic distance calculation
            const qLower  = question.toLowerCase().replace(/[.,/#!$%^&*;:{}=\-_`~()]/g, ' ');
            const qTokens = new Set(qLower.split(/\s+/));

            const getMentionedFiles = () => {
                const mentioned = new Set();
                sceneFiles.forEach(f => {
                    const fname     = f.filename.toLowerCase();
                    const noExt     = fname.replace(/\.(ply|drc)$/i, '');
                    const relaxed   = noExt.replace(/[_\-\s]+/g, ' ');
                    if (qLower.includes(fname) || qLower.includes(noExt) || qLower.includes(relaxed)) {
                        mentioned.add(f); return;
                    }
                    if (app.sceneInfo?._map) {
                        for (const [key, val] of app.sceneInfo._map.entries()) {
                            if ((qTokens.has(key) || qLower.includes(key)) && val.filename === f.filename) {
                                mentioned.add(f); return;
                            }
                        }
                    }
                });
                return Array.from(mentioned);
            };

            const targetFiles = getMentionedFiles();
            const distKW = ['distance','dist','far','close','near','between'];
            if (distKW.some(k => qLower.includes(k)) && targetFiles.length >= 2) {
                for (let i = 0; i < targetFiles.length; i++) {
                    for (let j = i+1; j < targetFiles.length; j++) {
                        const p1 = new THREE.Vector3().fromArray(targetFiles[i].bbox.center);
                        const p2 = new THREE.Vector3().fromArray(targetFiles[j].bbox.center);
                        context.calculated_distances.push({
                            pair: `${targetFiles[i].filename} <-> ${targetFiles[j].filename}`,
                            distance: parseFloat(p1.distanceTo(p2).toFixed(3)),
                        });
                    }
                }
            }

            /*************************************************************/
            const systemPrompt = `You are an intelligent 3D Point Cloud Viewer assistant specialising in industrial facility inspection.

Current Scene Data (JSON):
${JSON.stringify(context, null, 2)}

User Question: "${question}"

Answer concisely and precisely. When giving numeric answers, round to 2–4 decimal places and include units (metres / m³ / m²).

AVAILABLE ACTIONS — include in your response exactly as shown:

[ACTION:ZOOM_IN]              — zoom in
[ACTION:ZOOM_OUT]             — zoom out
[ACTION:SHOW:'filename']      — make object visible
[ACTION:HIDE:'filename']      — hide object
[ACTION:MOVE:'filename':'direction':amount]   — relative move (left/right/up/down/forward/back)
[ACTION:POSITION:'filename':x:y:z]            — absolute position
[ACTION:ROTATE:'filename':'axis':degrees]     — rotate (axis: x/y/z)

[ACTION:GENERATE_CSV:columns:col1,col2,col3]  — export CSV
  Available columns: object, object_type, position_x, position_y, position_z,
                     size_x, size_y, size_z, height, diameter, volume, surface_area,
                     vertex_count, visible
  Optional suffix: :filter:tank  (or :filter:pipe  :filter:pump) — only include that type.
  Example (tanks only): [ACTION:GENERATE_CSV:columns:object,position_x,position_y,position_z,height,diameter:filter:tank]

[ACTION:HIGHLIGHT_TYPE:'typeName']            — highlight / zoom all objects of a given type
  Example: [ACTION:HIGHLIGHT_TYPE:'pump']

[ACTION:HIGHLIGHT_CONNECTED:'tankFilename']   — highlight pipes spatially near the given tank
  Example: [ACTION:HIGHLIGHT_CONNECTED:'tank_OSCG_4']

RULES:
- For "how many X" questions, count filenames matching the type in context.typeGroups or context.files.
  e.g. "How many pumps?" → look at context.typeGroups.pump.length or typeStats.pump.count.
- For volume / surface area of a selected object, use context.selectedObjectAnalysis (if available).
  If the user says "this tank" or "it", resolve to context.selectedObject or context.lastReferencedObject.
- For total surface area of all tanks → sum f.surface_area for all files where object_type === 'tank'
  (typeStats.tank.total_surface_area is pre-calculated for you).
- For CSV export of location + height + diameter of tanks:
  [ACTION:GENERATE_CSV:columns:object,position_x,position_y,position_z,height,diameter:filter:tank]
- Pipes connected to a tank: use [ACTION:HIGHLIGHT_CONNECTED:'tank_filename']; also list the pipe
  filenames that are spatially nearby (bbox overlap or small distance).
- If you mention a filename, put it in single quotes like 'tank_OSCG_4'.
- IMPORTANT: pronouns like "it","this","that" → resolve to context.selectedObject then context.lastReferencedObject.
- ALWAYS use GENERATE_CSV when the user asks to create / export / make / generate a CSV, table, or spreadsheet.`;
            /*************************************************************/

            const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${GEMINI_API_KEY}`;

            const response = await fetch(url, {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                signal:  abortSignal,
                body:    JSON.stringify({ contents: [{ parts: [{ text: systemPrompt }] }] })
            });

            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.error?.message || 'Gemini API Error');
            }

            const data   = await response.json();
            const answer = data.candidates?.[0]?.content?.parts?.[0]?.text || "I couldn't generate an answer.";
            return { handled: true, answer };

        } catch (error) {
            if (error?.name === 'AbortError') return { handled: false, aborted: true, error: 'Request aborted by user' };
            return { handled: false, error: error.message };
        }
    }

    function findMatchingFilename(targetFilename) {
        // Try exact match first
        if (app.loadedFiles.has(targetFilename)) {
            return targetFilename;
        }

        // Try case-insensitive match
        const targetLower = targetFilename.toLowerCase();
        for (const [filename, fileData] of app.loadedFiles.entries()) {
            if (filename.toLowerCase() === targetLower) {
                return filename;
            }
        }

        // Try partial match (includes)
        for (const [filename, fileData] of app.loadedFiles.entries()) {
            if (filename.toLowerCase().includes(targetLower) || targetLower.includes(filename.toLowerCase())) {
                return filename;
            }
        }

        // Try matching without extension
        const targetNoExt = targetFilename.replace(/\.ply$/i, '');
        for (const [filename, fileData] of app.loadedFiles.entries()) {
            const filenameNoExt = filename.replace(/\.ply$/i, '');
            if (filenameNoExt.toLowerCase() === targetNoExt.toLowerCase()) {
                return filename;
            }
        }

        return null;
    }

    async function handleQuerySend() {
        const queryInput = document.getElementById('query-input');
        if (!queryInput) return;
        const querySendBtn = document.getElementById('query-send-btn');

        // While thinking, the same button acts as a pause/stop control.
        if (isQueryInFlight) {
            if (currentAbortController) {
                currentAbortController.abort();
            }
            return;
        }

        const query = queryInput.value.trim();
        if (query === '') return;
        if (ui) ui.showInlineQueryMessage(query, 'user');
        addHistory('user', query);
        const directMention = detectFilenameMention(query);
        if (directMention) lastReferencedFilename = directMention;

        // Fast-path for follow-up pronoun commands in the same session.
        const hasPronounRef = /\b(it|that|this|that one|this one|them|those)\b/i.test(query);
        const wantsHide = /\b(remove|hide|delete)\b/i.test(query);
        const wantsShow = /\b(show|add|bring\s+back|unhide)\b/i.test(query);
        if (hasPronounRef && lastReferencedFilename && (wantsHide || wantsShow)) {
            const makeVisible = Boolean(wantsShow && !wantsHide);
            if (sceneManager && sceneManager.toggleFileVisibility) {
                sceneManager.toggleFileVisibility(lastReferencedFilename, makeVisible);
            }
            if (ui && ui.createFileCheckboxes) {
                ui.createFileCheckboxes();
            }
            const actionWord = makeVisible ? 'shown' : 'hidden';
            const msg = `Done. '${lastReferencedFilename}' is now ${actionWord}.`;
            if (ui) ui.showInlineQueryMessage(msg, 'assistant');
            addHistory('assistant', msg);
            queryInput.value = '';
            return;
        }
        queryInput.value = '';

        // UX: Show "Thinking..." state
        isQueryInFlight = true;
        if (querySendBtn) {
            querySendBtn.innerHTML = PAUSE_ICON;
            querySendBtn.classList.add('pause-state');
            querySendBtn.disabled = false;
        }
        if (ui) ui.showInlineQueryMessage('Thinking...', 'info');

        try {
            currentAbortController = new AbortController();
            // Gemini AI Only Mode
            const sceneFiles = getSceneMetadata();
            const contextualQuery = enrichQuestionWithSessionReference(query);
            const aiResponse = await geminiQueryHandler(contextualQuery, sceneFiles, currentAbortController.signal);

            if (aiResponse.handled && !aiResponse.error && aiResponse.answer && aiResponse.answer !== "I couldn't generate an answer.") {
                const rawAnswer = aiResponse.answer;

                // Parse Actions
                let userDisplayMessage = rawAnswer;

                // Check for ZOOM_IN
                if (rawAnswer.includes('[ACTION:ZOOM_IN]')) {
                    if (ui && ui.zoomIn) ui.zoomIn();
                    userDisplayMessage = userDisplayMessage.replace(/\[ACTION:ZOOM_IN\]/g, '');
                }

                // Check for ZOOM_OUT
                if (rawAnswer.includes('[ACTION:ZOOM_OUT]')) {
                    if (ui && ui.zoomOut) ui.zoomOut();
                    userDisplayMessage = userDisplayMessage.replace(/\[ACTION:ZOOM_OUT\]/g, '');
                }

                const hideMatches = [...rawAnswer.matchAll(/\[ACTION:HIDE:'(.*?)'\]/g)];
                for (const match of hideMatches) {
                    const targetFilename = match[1];
                    const actualFilename = findMatchingFilename(targetFilename);
                    if (actualFilename && sceneManager && sceneManager.toggleFileVisibility) {
                        sceneManager.toggleFileVisibility(actualFilename, false);
                        lastReferencedFilename = actualFilename;
                    }
                    userDisplayMessage = userDisplayMessage.replace(match[0], '');
                }


                const showMatches = [...rawAnswer.matchAll(/\[ACTION:SHOW:'(.*?)'\]/g)];
                for (const match of showMatches) {
                    const targetFilename = match[1];
                    const actualFilename = findMatchingFilename(targetFilename);
                    if (actualFilename && sceneManager && sceneManager.toggleFileVisibility) {
                        sceneManager.toggleFileVisibility(actualFilename, true);
                        lastReferencedFilename = actualFilename;
                    }
                    userDisplayMessage = userDisplayMessage.replace(match[0], '');
                }

                // Update UI checkboxes after all show/hide operations
                if ((hideMatches.length > 0 || showMatches.length > 0) && ui && ui.createFileCheckboxes) {
                    ui.createFileCheckboxes();
                }

                // Check for MOVE object (relative)
                const moveMatch = rawAnswer.match(/\[ACTION:MOVE:'(.*?)':'(.*?)':([-\d.]+)\]/);
                if (moveMatch && moveMatch[1] && moveMatch[2] && moveMatch[3]) {
                    const filename = moveMatch[1];
                    const direction = moveMatch[2].toLowerCase();
                    const amount = parseFloat(moveMatch[3]);

                    const fileData = app.loadedFiles.get(filename);
                    if (fileData && fileData.object) {
                        lastReferencedFilename = filename;
                        const obj = fileData.object;
                        switch (direction) {
                            case 'left':
                                obj.position.x -= amount;
                                break;
                            case 'right':
                                obj.position.x += amount;
                                break;
                            case 'up':
                                obj.position.y += amount;
                                break;
                            case 'down':
                                obj.position.y -= amount;
                                break;
                            case 'forward':
                                obj.position.z -= amount;
                                break;
                            case 'back':
                                obj.position.z += amount;
                                break;
                        }
                        obj.updateMatrixWorld(true);
                        // Invalidate bbox cache
                        fileData._cachedBBox = null;
                    }
                    userDisplayMessage = userDisplayMessage.replace(moveMatch[0], '');
                }

                // Check for POSITION object (absolute)
                const posMatch = rawAnswer.match(/\[ACTION:POSITION:'(.*?)':([-\d.]+):([-\d.]+):([-\d.]+)\]/);
                if (posMatch && posMatch[1]) {
                    const filename = posMatch[1];
                    const x = parseFloat(posMatch[2]);
                    const y = parseFloat(posMatch[3]);
                    const z = parseFloat(posMatch[4]);

                    const fileData = app.loadedFiles.get(filename);
                    if (fileData && fileData.object) {
                        lastReferencedFilename = filename;
                        fileData.object.position.set(x, y, z);
                        fileData.object.updateMatrixWorld(true);
                        // Invalidate bbox cache
                        fileData._cachedBBox = null;
                    }
                    userDisplayMessage = userDisplayMessage.replace(posMatch[0], '');
                }

                // Check for ROTATE object
                const rotateMatch = rawAnswer.match(/\[ACTION:ROTATE:'(.*?)':'(.*?)':([-\d.]+)\]/);
                if (rotateMatch && rotateMatch[1] && rotateMatch[2] && rotateMatch[3]) {
                    const filename = rotateMatch[1];
                    const axis = rotateMatch[2].toLowerCase();
                    const degrees = parseFloat(rotateMatch[3]);
                    const radians = (degrees * Math.PI) / 180;

                    const fileData = app.loadedFiles.get(filename);
                    if (fileData && fileData.object) {
                        lastReferencedFilename = filename;
                        const obj = fileData.object;
                        switch (axis) {
                            case 'x':
                                obj.rotation.x += radians;
                                break;
                            case 'y':
                                obj.rotation.y += radians;
                                break;
                            case 'z':
                                obj.rotation.z += radians;
                                break;
                        }
                        obj.updateMatrixWorld(true);
                        // Invalidate bbox cache
                        fileData._cachedBBox = null;
                    }
                    userDisplayMessage = userDisplayMessage.replace(rotateMatch[0], '');
                }

                // ── GENERATE_CSV (with optional :filter:type suffix) ───────────
                const csvMatch = rawAnswer.match(/\[ACTION:GENERATE_CSV:columns:([\w,]+)(?::filter:(\w+))?\]/);
                if (csvMatch) {
                    const requestedCols = csvMatch[1].split(',').map(c => c.trim()).filter(Boolean);
                    const filterType    = csvMatch[2] || null;
                    const csvData = generateCSVFromScene(requestedCols, filterType);
                    userDisplayMessage = userDisplayMessage.replace(csvMatch[0], '');
                    if (ui) {
                        ui.showInlineQueryMessage(userDisplayMessage.trim(), 'assistant');
                        ui.showFileAttachment(csvData.filename, csvData.csvContent, csvData.previewRows, csvData.columns, csvData.totalRows);
                    }
                    addHistory('assistant', userDisplayMessage.trim() + `\n[Generated ${csvData.filename}]`);
                }

                // ── HIGHLIGHT_TYPE ────────────────────────────────────────────
                const hlTypeMatch = rawAnswer.match(/\[ACTION:HIGHLIGHT_TYPE:'(.*?)'\]/);
                if (hlTypeMatch) {
                    const typeName = hlTypeMatch[1].toLowerCase();
                    const matches  = sceneFiles.filter(f => (f.object_type || '').toLowerCase() === typeName && f.visible);
                    matches.forEach(f => {
                        const fd = app.loadedFiles.get(f.filename);
                        if (fd && fd.geometry) {
                            fd.geometry.computeBoundingBox();
                            const center = new THREE.Vector3();
                            fd.geometry.boundingBox.getCenter(center);
                            if (fd.object) fd.object.localToWorld(center);
                            const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                            sceneManager.createHighlightBox({ name: f.filename, filename: f.filename, center: center.toArray(), size });
                        }
                    });
                    userDisplayMessage = userDisplayMessage.replace(hlTypeMatch[0], '');
                }

                // ── HIGHLIGHT_CONNECTED ───────────────────────────────────────
                const hlConnMatch = rawAnswer.match(/\[ACTION:HIGHLIGHT_CONNECTED:'(.*?)'\]/);
                if (hlConnMatch) {
                    const tankName = hlConnMatch[1];
                    const tankActual = findMatchingFilename(tankName);
                    if (tankActual) {
                        lastReferencedFilename = tankActual;
                        const tankFD = app.loadedFiles.get(tankActual);
                        if (tankFD && tankFD.geometry) {
                            // Compute world-space bounding box of the tank
                            tankFD.geometry.computeBoundingBox();
                            const tankBoxWorld = new THREE.Box3().setFromObject(tankFD.object || new THREE.Object3D());

                            // Expand by a proximity margin (e.g. 2 m) to catch nearby pipes
                            const PROXIMITY = 2.0;
                            const expanded = tankBoxWorld.clone().expandByScalar(PROXIMITY);

                            // Find all pipe-type files whose bbox intersects the expanded tank box
                            const connectedPipes = [];
                            app.loadedFiles.forEach((fd, fname) => {
                                if (!fd.geometry || !fd.visible) return;
                                const t = inferObjectType(fname);
                                if (t !== 'pipe') return;
                                fd.geometry.computeBoundingBox();
                                const pipeBox = new THREE.Box3().setFromObject(fd.object || new THREE.Object3D());
                                if (expanded.intersectsBox(pipeBox)) {
                                    connectedPipes.push(fname);
                                    const center = pipeBox.getCenter(new THREE.Vector3());
                                    const size   = pipeBox.getSize(new THREE.Vector3()).toArray();
                                    sceneManager.createHighlightBox({ name: fname, filename: fname, center: center.toArray(), size });
                                }
                            });

                            if (connectedPipes.length === 0) {
                                if (ui) ui.showInlineQueryMessage(`No nearby pipes found for '${tankActual}'.`, 'info');
                            }
                        }
                    }
                    userDisplayMessage = userDisplayMessage.replace(hlConnMatch[0], '');
                }

                // Show final message (if not already shown by CSV branch)
                if (!csvMatch) {
                    if (ui) ui.showInlineQueryMessage(userDisplayMessage.trim(), 'assistant');
                    addHistory('assistant', userDisplayMessage.trim());
                }

                const filenameFromAnswer = detectFilenameMention(userDisplayMessage);
                if (filenameFromAnswer) lastReferencedFilename = filenameFromAnswer;

                // Auto-highlight first filename mentioned in AI answer (world-space aware)
                if (!hideMatches.length && !hlTypeMatch && !hlConnMatch) {
                    for (const f of sceneFiles) {
                        if (userDisplayMessage.includes(f.filename)) {
                            const fd = app.loadedFiles.get(f.filename);
                            if (fd && fd.geometry && fd.visible) {
                                fd.geometry.computeBoundingBox();
                                const center = new THREE.Vector3();
                                fd.geometry.boundingBox.getCenter(center);
                                if (fd.object) fd.object.localToWorld(center);
                                const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                                sceneManager.createHighlightBox({ name: f.filename, filename: f.filename, center: center.toArray(), size });
                            }
                            break; // only highlight first match
                        }
                    }
                }

            } else if (aiResponse.aborted) {
                if (ui) ui.showInlineQueryMessage('Response paused. You can send another message.', 'info');
                addHistory('assistant', 'Response paused by user.');
            } else if (aiResponse.error === 'Gemini API Key not configured') {
                if (ui) ui.showInlineQueryMessage('Query not understood. Configure Gemini API Key to enable AI.', 'error');
                addHistory('assistant', 'Gemini API Key not configured.');
            } else {
                if (ui) ui.showInlineQueryMessage('No answer found.', 'error');
                addHistory('assistant', 'No answer found.');
            }
            /*
            }
            */
        } finally {
            isQueryInFlight = false;
            currentAbortController = null;
            if (querySendBtn) {
                querySendBtn.innerHTML = SEND_ICON;
                querySendBtn.classList.remove('pause-state');
                querySendBtn.disabled = false;
            }
        }
    }
}