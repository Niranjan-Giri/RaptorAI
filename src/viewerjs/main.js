// Deprecated entrypoint; the refactored app entry is in src/app.entry.js
console.warn('Note: src/main.js is deprecated. The app now uses src/app.entry.js');

/**
 * Load the scene info JSON which maps object labels to filenames and bounding box metadata.
 */
async function loadSceneInfo() {
    try {
        const resp = await fetch('/info.json');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json = await resp.json();
        sceneInfo = json;
        // Build a map for faster search: map normalized tokens to object keys
        sceneInfo._map = new Map();
        sceneInfo.displayNames = new Map(); // map filename -> array of friendly labels
        if (json.name) {
            for (const key of Object.keys(json.name)) {
                const val = json.name[key];
                const filenameLower = String(val).toLowerCase();
                const keyLower = String(key).toLowerCase();
                sceneInfo._map.set(keyLower, { key, filename: val });
                sceneInfo._map.set(filenameLower, { key, filename: val });
                // Also store filename without extension for matching searches that omit .ply
                const basename = filenameLower.replace(/\.ply$/i, '');
                sceneInfo._map.set(basename, { key, filename: val });

                // Tokenize filename and key words into map for fuzzy matching
                const tokens = new Set([...basename.split(/[^a-z0-9]+/), ...keyLower.split(/[^a-z0-9]+/)]);
                for (const t of tokens) {
                    if (t && t.length > 0) {
                        sceneInfo._map.set(t, { key, filename: val });
                    }
                }
                // Track the display names (we store primary label if available as key or filename)
                const existing = sceneInfo.displayNames.get(val) || [];
                if (!existing.includes(key)) existing.push(key);
                sceneInfo.displayNames.set(val, existing);
            }
        }
        // Also map bounding_box keys
        if (json.bounding_box) {
            for (const key of Object.keys(json.bounding_box)) {
                sceneInfo._map.set(key.toLowerCase(), { key, filename: json.name?.[key] });
            }
        }
        // If labels/aliases are defined explicitly, map them too
        if (json.labels) {
            for (const [fn, labels] of Object.entries(json.labels)) {
                const filenameLower = String(fn).toLowerCase();
                for (const lab of labels) {
                    const labLower = String(lab).toLowerCase();
                    sceneInfo._map.set(labLower, { key: Object.keys(json.name).find(k => json.name[k] === fn) || labLower, filename: fn });
                }
                // store display names
                const existing = sceneInfo.displayNames.get(fn) || [];
                for (const lab of labels) if (!existing.includes(lab)) existing.push(lab);
                sceneInfo.displayNames.set(fn, existing);
            }
        }

        console.log('[Main] Scene info.json loaded', sceneInfo);
        if (sceneInfo.displayNames) {
            for (const [fn, labs] of sceneInfo.displayNames.entries()) {
                console.log(`[Main] displayNames: ${fn} -> ${labs.join(', ')}`);
            }
        }
    } catch (err) {
        console.warn('[Main] Could not load info.json (not present or failed to parse). Will generate from loaded PLY files.', err);
        sceneInfo = { name: {}, bounding_box: {}, labels: {}, _map: new Map(), displayNames: new Map() };
    }
}

/**
 * Ensure an entry for a loaded file exists in `sceneInfo`; add bounding box and basic labels if missing.
 */
function ensureSceneInfoForFile(filename) {
    if (!sceneInfo) sceneInfo = { name: {}, bounding_box: {}, labels: {}, _map: new Map(), displayNames: new Map() };
    const fileData = loadedFiles.get(filename);
    if (!fileData || !fileData.geometry) return;

    // If there's already a mapping for this filename, don't overwrite
    const existingEntryKey = Object.keys(sceneInfo.name || {}).find(k => String(sceneInfo.name[k]).toLowerCase() === filename.toLowerCase());
    if (!existingEntryKey) {
        // Use basename without extension as key
        const basename = filename.replace(/\.ply$/i, '');
        let key = basename;
        // If key already used, add numeric suffix
        let suffix = 1;
        while (sceneInfo.name && sceneInfo.name[key]) {
            key = `${basename}_${suffix++}`;
        }
        sceneInfo.name = sceneInfo.name || {};
        sceneInfo.name[key] = filename;
    }

    // Compute bounding box for geometry
    const geometry = fileData.geometry;
    geometry.computeBoundingBox();
    const bbox = geometry.boundingBox;
    const size = new THREE.Vector3();
    bbox.getSize(size);
    const centerLocal = new THREE.Vector3();
    bbox.getCenter(centerLocal);

    // Convert center to world coordinates using associated object if available
    let centerWorld = centerLocal.toArray();
    if (fileData.object) {
        const cw = centerLocal.clone();
        fileData.object.localToWorld(cw);
        centerWorld = cw.toArray();
    }

    sceneInfo.bounding_box = sceneInfo.bounding_box || {};
    const keyForFile = Object.keys(sceneInfo.name).find(k => sceneInfo.name[k] === filename);
    // Only set bounding box if not already present (don't override explicit info.json)
    if (!sceneInfo.bounding_box[keyForFile]) {
        sceneInfo.bounding_box[keyForFile] = { x: size.x, y: size.y, z: size.z, center: centerWorld };
    }

    // Default label: basename
    sceneInfo.labels = sceneInfo.labels || {};
    sceneInfo.labels[filename] = sceneInfo.labels[filename] || [filename.replace(/\.ply$/i, '')];

    // Update _map and displayNames
    sceneInfo._map = sceneInfo._map || new Map();
    const basenameLower = filename.replace(/\.ply$/i, '').toLowerCase();
    sceneInfo._map.set(basenameLower, { key: Object.keys(sceneInfo.name).find(k => sceneInfo.name[k] === filename), filename });
    if (sceneInfo.labels[filename]) {
        for (const lab of sceneInfo.labels[filename]) {
            const labLower = String(lab).toLowerCase();
            sceneInfo._map.set(labLower, { key: Object.keys(sceneInfo.name).find(k => sceneInfo.name[k] === filename), filename });
        }
        sceneInfo.displayNames = sceneInfo.displayNames || new Map();
        sceneInfo.displayNames.set(filename, sceneInfo.labels[filename]);
    }

    console.log('[Main] Generated sceneInfo entry for', filename, sceneInfo.name, sceneInfo.bounding_box[Object.keys(sceneInfo.name).find(k => sceneInfo.name[k] === filename)]);
}

/**
 * Export the generated or combined sceneInfo as a downloadable JSON file.
 */
function exportSceneInfo() {
    // Build minimal scene info object
    const out = { name: {}, bounding_box: {}, labels: {} };
    if (sceneInfo && sceneInfo.name) out.name = { ...sceneInfo.name };
    if (sceneInfo && sceneInfo.bounding_box) {
        // Copy bounding_box but exclude internal center object (keep size values)
        for (const [k, v] of Object.entries(sceneInfo.bounding_box)) {
            out.bounding_box[k] = { x: v.x, y: v.y, z: v.z };
        }
    }
    if (sceneInfo && sceneInfo.labels) out.labels = { ...sceneInfo.labels };

    const blob = new Blob([JSON.stringify(out, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'scene-info.generated.json';
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

function setMode(mode) 
{
    currentMode = mode;
    
    // Update button states - remove active from both control-btn and icon-btn
    document.querySelectorAll('.control-btn, .icon-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    if (mode === 'orbit') 
    {
        document.getElementById('btn-orbit').classList.add('active');
        controls.enableRotate = true;
        controls.enablePan = false;
        controls.enabled = true;
        // Deselect and disable transform when leaving select mode
        deselectFile();
        renderer.domElement.style.cursor = 'grab';  
    } 
    else if (mode === 'pan') 
    {
        document.getElementById('btn-pan').classList.add('active');
        controls.enableRotate = false;
        controls.enablePan = true;
        controls.enabled = true;
        // Deselect and disable transform when leaving select mode
        deselectFile();
        renderer.domElement.style.cursor = 'move';
    } 
    else if (mode === 'select') 
    {
        document.getElementById('btn-select').classList.add('active');
        controls.enableRotate = false;
        controls.enablePan = false;
        
        controls.enabled = true;
       
        renderer.domElement.style.cursor = 'crosshair';
    }
}

function setColorMode(mode) {
    colorMode = mode;
    
    // Update button states
    if (mode === 'original') {
        document.getElementById('btn-original-color').classList.add('active');
        document.getElementById('btn-coded-color').classList.remove('active');
    } else {
        document.getElementById('btn-original-color').classList.remove('active');
        document.getElementById('btn-coded-color').classList.add('active');
    }
    
    // Re-apply colors to all loaded files
    loadedFiles.forEach((fileData, filename) => {
        applyColorMode(fileData.geometry, filename);
        updateFileRender(filename);
    });
}

function setQualityMode(mode) {
    qualityMode = mode;
    
    // Update button states
    if (mode === 'downsampled') {
        document.getElementById('btn-downsampled').classList.add('active');
        document.getElementById('btn-original-quality').classList.remove('active');
    } else {
        document.getElementById('btn-downsampled').classList.remove('active');
        document.getElementById('btn-original-quality').classList.add('active');
    }
    
    // Update loader manager quality mode
    loaderManager.setQualityMode(mode);
    
    // Cancel any in-progress loads
    console.log(`Switching to ${mode} quality mode...`);
    loaderManager.cancelAll();
    
    // Store current file data (keep objects visible during reload)
    const currentFiles = new Map();
    loadedFiles.forEach((fileData, filename) => {
        currentFiles.set(filename, {
            object: fileData.object,
            visible: fileData.visible,
            filepath: fileData.filepath
        });
    });
    
    // Mark all files as loading but keep existing geometry/objects
    loadedFiles.forEach((fileData, filename) => {
        fileData.loading = true;
        fileData.isPreview = true;
        fileData.loadingMessage = `Switching to ${mode} mode...`;
        fileData.loadingProgress = 0;
    });
    
    // Update UI to show loading state
    createFileCheckboxes();
    
    // Reload all files with new quality (objects stay visible)
    plyFiles.forEach((filepath) => {
        const filename = filepath.split('/').pop();
        loaderManager.loadPLY(filepath, filename);
    });
}

function setRenderMode(mode) 
{
    renderMode = mode;
    
    // Update button states
    if (mode === 'points') 
    {
        document.getElementById('btn-point-cloud').classList.add('active');
        document.getElementById('btn-3d-mesh').classList.remove('active');
    } 
    else 
    {
        document.getElementById('btn-point-cloud').classList.remove('active');
        document.getElementById('btn-3d-mesh').classList.add('active');
    }
    
    // Re-render all files with new mode
    loadedFiles.forEach((fileData, filename) => {
        updateFileRender(filename);
    });
}

function zoomIn() 
{
    camera.position.multiplyScalar(0.8);
    controls.update();
}

function zoomOut() 
{
    camera.position.multiplyScalar(1.2);
    controls.update();
}

function resetView() 
{
    camera.position.set(0, 0, 2);
    controls.target.set(0, 0, 0);
    controls.update();
}

async function handleQuerySend() {
    const queryInput = document.getElementById('query-input');
    const query = queryInput.value.trim();
    
    if (query === '') {
        console.log('Empty query');
        return;
    }
    
    console.log('Query submitted:', query);
    
    // Show loading indicator
    const querySendBtn = document.getElementById('query-send-btn');
    const originalBtnHTML = querySendBtn.innerHTML;
    querySendBtn.innerHTML = '<div class="spinner"></div>';
    querySendBtn.disabled = true;
    
    try {
        // Use only local info.json mapping for all queries
        const localResponse = localQueryHandler(query);
        if (localResponse && localResponse.handled) {
            // If local query returns location info, highlight the object and animate camera; do not show modal.
            const first = localResponse.data.results && localResponse.data.results[0];
            if (first && (first.center || first.size || first.filename)) {
                // If filename present and geometry loaded, prefer geometry bbox
                let filename = first.filename;
                if (filename) {
                    const f = Array.from(loadedFiles.entries()).find(([name, fd]) => name.toLowerCase() === String(filename).toLowerCase() || fd.filepath.toLowerCase().endsWith(String(filename).toLowerCase()));
                    if (f) {
                        const [name, fd] = f;
                        if (fd.geometry) {
                            fd.geometry.computeBoundingBox();
                            const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray();
                            const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                            createHighlightBox({ name, filename: name, center, size });
                        } else {
                            createHighlightBox({ name: filename, filename, center: first.center || [0,0,0], size: first.size || [1,1,1] });
                        }
                    } else {
                        // geometry isn't loaded, use info.json bbox
                        createHighlightBox({ name: filename, filename, center: first.center || [0,0,0], size: first.size || [1,1,1] });
                    }
                } else {
                    // No filename, use provided center/size if any
                    createHighlightBox({ name: first.object, filename: first.object, center: first.center || [0,0,0], size: first.size || [1,1,1] });
                }
            } else if (first && first.exists !== undefined) {
                // Existence or count result: show a small inline message in the query area instead of modal
                const existsMsg = first.exists ? `Yes, ${first.object} is present.` : `No, ${first.object} is not present.`;
                showInlineQueryMessage(existsMsg, first.exists ? 'success' : 'error');
            } else if (localResponse.data && localResponse.data.results && localResponse.data.results.length > 0) {
                // Generic results (like counts)
                const row = localResponse.data.results[0];
                const keys = Object.keys(row || {});
                if (keys.length > 0) {
                    const msg = keys.map(k => `${k}: ${row[k]}`).join(', ');
                    showInlineQueryMessage(msg, 'info');
                }
            } else {
                // Nothing found
                showInlineQueryMessage('No results found.', 'error');
            }
        } else {
            showInlineQueryMessage('No results found.', 'error');
        }
    } finally {
        // Restore button state
        querySendBtn.innerHTML = originalBtnHTML;
        querySendBtn.disabled = false;
        
        // Clear input after sending
        queryInput.value = '';
        // No server signature to clear
    }
}

// In-memory frontend cache to avoid requesting backend repeatedly for the same question
const queryCache = new Map();

function normalizeQuestion(q) {
    return q.trim().toLowerCase();
}

function getSceneMetadata() {
    const files = [];
    loadedFiles.forEach((fileData, filename) => {
        if (!fileData.geometry) return;
        const geometry = fileData.geometry;
        geometry.computeBoundingBox();
        const bbox = geometry.boundingBox;
        const center = new THREE.Vector3();
        bbox.getCenter(center);
        files.push({
            filename,
            visible: !!fileData.visible,
            vertex_count: geometry.attributes.position.count,
            bbox: {
                min: bbox.min.toArray(),
                max: bbox.max.toArray(),
                size: bbox.getSize(new THREE.Vector3()).toArray(),
                center: center.toArray()
            }
        });
    });
    return files;
}

function localQueryHandler(question) {
    const q = normalizeQuestion(question);

    // Fast path: use frontend cache
    if (queryCache.has(q)) {
        return { handled: true, data: queryCache.get(q) };
    }

    // Get scene metadata
    const sceneFiles = getSceneMetadata();
    const filenamesLower = sceneFiles.map(f => f.filename.toLowerCase());

    const responseData = { success: true, question, sql: null, results: [], columns: [], row_count: 0 };

    // 1. Count objects: e.g., "how many chairs"
    let match = q.match(/how many (?:of )?([\w\s-]+)s?\b/);
    if (!match) match = q.match(/count (?:the )?(\w+)s?\b/);
    if (match) {
        let object = match[1];
        // Normalize token: trim and remove common trailing s for plural forms
        object = object.trim().toLowerCase();
        if (object.endsWith('s')) object = object.slice(0, -1);
        const count = filenamesLower.filter(f => f.includes(object)).length;
        responseData.results = [{ object, count }];
        responseData.columns = ['object', 'count'];
        responseData.row_count = 1;
        queryCache.set(q, responseData);
        return { handled: true, data: responseData };
    }

    // 2. Is there an object: e.g., "is there a chair"
    match = q.match(/is there (?:a|an|the )?([\w\s-]+)/);
    if (match) {
        let object = match[1];
        object = object.trim().toLowerCase();
        if (object.endsWith('s')) object = object.slice(0, -1);
        // Prefer explicit info.json names if available
        let exists = filenamesLower.some(f => f.includes(object));
        if (sceneInfo && sceneInfo._map) {
            // match tokens and basenames too
            const lower = object.toLowerCase();
            const entry = sceneInfo._map.get(lower);
            exists = Boolean(entry) || sceneFiles.some(f => f.filename.toLowerCase().includes(lower));
            // If we have an entry for the label, include the filename in the response
            if (entry) {
                responseData.results = [{ object, exists: true, filename: entry.filename }];
                responseData.columns = ['object', 'exists', 'filename'];
                responseData.row_count = 1;
                queryCache.set(q, responseData);
                return { handled: true, data: responseData };
            }
        }
        responseData.results = [{ object, exists }];
        responseData.columns = ['object', 'exists'];
        responseData.row_count = 1;
        queryCache.set(q, responseData);
        return { handled: true, data: responseData };
    }

    // 3. Where is object e.g., "where is the chair"
    match = q.match(/where is (?:a|an|the )?([\w\s-]+)/);
    if (match) {
        let object = match[1];
        object = object.trim().toLowerCase();
        if (object.endsWith('s')) object = object.slice(0, -1);
        // If info.json is present, treat it as authoritative for "where is" queries
        if (sceneInfo && sceneInfo._map) {
            const lower = object.toLowerCase();
            let entry = sceneInfo._map.get(lower);
            if (!entry) {
                // try tokens and substrings
                for (const [k, v] of sceneInfo._map.entries()) {
                    if (k.includes(lower) || lower.includes(k)) {
                        entry = v; break;
                    }
                }
            }
            if (entry) {
                // try to compute center/size from loaded geometry if available
                let center = [0,0,0];
                let size = [1,1,1];
                if (entry.filename) {
                    const f = sceneFiles.find(ff => ff.filename.toLowerCase().includes(String(entry.filename).toLowerCase()));
                    if (f) {
                        center = f.bbox.center;
                        size = f.bbox.size;
                    }
                }
                // Otherwise, read size from the info json bounding_box if present
                const bboxInfo = sceneInfo.bounding_box && sceneInfo.bounding_box[entry.key];
                if (bboxInfo) {
                    size = [bboxInfo.x || size[0], bboxInfo.y || size[1], bboxInfo.z || size[2]];
                }
                responseData.results = [{ object: entry.key, center, size, filename: entry.filename }];
                responseData.columns = ['object', 'center', 'size'];
                responseData.row_count = 1;
                queryCache.set(q, responseData);
                return { handled: true, data: responseData };
            } else {
                // Info.json is authoritative and the object wasn't found
                responseData.results = [{ object, exists: false }];
                responseData.columns = ['object', 'exists'];
                responseData.row_count = 1;
                queryCache.set(q, responseData);
                return { handled: true, data: responseData };
            }
        } else {
            // Fallback to previous behavior when info.json is not present
            let file = sceneFiles.find(f => f.filename.toLowerCase().includes(object));
            if (file) {
                responseData.results = [{ object, center: file.bbox.center, size: file.bbox.size, filename: file.filename }];
                responseData.columns = ['object', 'center', 'size'];
                responseData.row_count = 1;
                queryCache.set(q, responseData);
                return { handled: true, data: responseData };
            }
        }
        // Not found locally - don't claim handled, fall back to backend
    }

    // 4. Vertex count of a file
    match = q.match(/vertex count (?:of )?(?:the )?([\w\s-]+)|how many vertices (?:in|for) ([\w\s-]+)/);
    if (match) {
        const object = (match[1] || match[2] || '').trim().toLowerCase();
        const file = sceneFiles.find(f => f.filename.toLowerCase().includes(object));
        if (file) {
            responseData.results = [{ object: file.filename, vertex_count: file.vertex_count }];
            responseData.columns = ['object', 'vertex_count'];
            responseData.row_count = 1;
            queryCache.set(q, responseData);
            return { handled: true, data: responseData };
        }
    }

    // Not handled locally
    return { handled: false };
}

// The app does not use server-side SQL execution anymore;
// This function removed as we now only use local info.json mapping.

function displayQueryResults(data) {
    // Create or update results modal
    let modal = document.getElementById('query-results-modal');
    
    if (!modal) {
        modal = createQueryResultsModal();
    }
    
    const contentDiv = document.getElementById('query-results-content');
    
    const source = data.source || 'local';
    console.log(`[Query] Displaying results from: ${source}`);
    let html = `
        <div style="display:flex; align-items:center; justify-content:space-between;">
            <h3 style="color: #4CAF50; margin-top: 0;">Query Results</h3>
            <div class="query-source-badge" style="font-size: 12px; color: #fff; padding: 6px 8px; border-radius: 8px; margin-left: 8px; background: rgba(255, 193, 7, 0.12); border: 1px solid rgba(255,193,7,0.2)">Local Preview</div>
        </div>
        
        <div style="margin-bottom: 15px;">
            <strong style="color: #2196F3;">Question:</strong>
            <p style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 5px; margin: 5px 0;">
                ${data.question}
            </p>
        </div>
    `;
    
    // The app is local-only, there's no SQL generation displayed.
    
    if (data.results && data.results.length > 0) {
        html += `
            <div style="margin-bottom: 15px;">
                <strong style="color: #2196F3;">Results (${data.row_count} rows):</strong>
                <div style="overflow-x: auto; margin-top: 10px;">
                    <table style="width: 100%; border-collapse: collapse; background: rgba(255,255,255,0.05);">
                        <thead>
                            <tr style="background: rgba(76, 175, 80, 0.3);">
                                ${data.columns.map(col => `<th style="padding: 10px; text-align: left; border: 1px solid rgba(255,255,255,0.1);">${col}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            ${data.results.slice(0, 100).map(row => `
                                <tr>
                                    ${data.columns.map(col => `<td style="padding: 8px; border: 1px solid rgba(255,255,255,0.1);">${row[col] ?? ''}</td>`).join('')}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                    ${data.results.length > 100 ? '<p style="margin-top: 10px; color: #ffa500;">Showing first 100 rows of ' + data.results.length + '</p>' : ''}
                </div>
            </div>
        `;
    }
    
    contentDiv.innerHTML = html;
    modal.style.display = 'flex';
    
    // No SQL copy functionality used in local-only mode.

    // If the query provides a bounding box/center/size, highlight it
    if (data.results && data.results.length > 0) {
        const first = data.results[0];
        // If response says object exists: highlight its bounding box
        if (first.center && first.size) {
            const name = first.object || first.filename || Object.keys(sceneInfo?.name || {})[0];
            createHighlightBox({ name, filename: first.filename, center: first.center, size: first.size });
        } else if (first.exists !== undefined) {
            if (!first.exists) {
                // Clear any existing highlights
                clearHighlights();
                // Add a small 'not found' message to the modal
                const notFoundDiv = document.createElement('div');
                notFoundDiv.style.background = 'rgba(255, 0, 0, 0.12)';
                notFoundDiv.style.borderLeft = '4px solid #f44336';
                notFoundDiv.style.padding = '10px';
                notFoundDiv.style.borderRadius = '4px';
                notFoundDiv.style.marginTop = '10px';
                notFoundDiv.textContent = `Object '${first.object}' not found in scene (as per info.json).`;
                contentDiv.appendChild(notFoundDiv);
            } else {
                // Found: if filename specified, try to highlight it
                if (first.filename) {
                    // If the geometry is loaded, use the true bounding box center/size
                    const f = Array.from(loadedFiles.entries()).find(([name, fd]) => name.toLowerCase() === String(first.filename).toLowerCase() || fd.filepath.toLowerCase().endsWith(String(first.filename).toLowerCase()));
                    if (f) {
                        const [name, fd] = f;
                        if (fd.geometry) {
                            fd.geometry.computeBoundingBox();
                            const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray();
                            const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                            createHighlightBox({ name, filename: name, center, size });
                        } else {
                            // Not loaded geometry - fall back to info.json bounding box
                            const bboxInfo = sceneInfo?.bounding_box?.[Object.keys(sceneInfo.name).find(k => sceneInfo.name[k] === first.filename)];
                            if (bboxInfo) {
                                const center = [0, 0, 0];
                                const size = [bboxInfo.x || 1, bboxInfo.y || 1, bboxInfo.z || 1];
                                createHighlightBox({ name: first.filename, filename: first.filename, center, size });
                            }
                        }
                    }
                }
            }
        }
    }
}

/**
 * Create and display a translucent green highlight box around an object.
 * If file geometry is available we center the box on its mesh; otherwise use center data if provided.
 */
function clearHighlights() {
    highlightBoxes.forEach((mesh, name) => {
        scene.remove(mesh);
        if (mesh.geometry) mesh.geometry.dispose();
        if (mesh.material) mesh.material.dispose();
    });
    highlightBoxes.clear();
    // No labels to remove (3D label system disabled)
}

function createHighlightBox({ name, filename, center = [0,0,0], size = [1,1,1] }) {
    // Remove any current highlight(s) to focus on the new one
    clearHighlights();

    const boxSize = new THREE.Vector3(size[0] || 1, size[1] || 1, size[2] || 1);
    const geometry = new THREE.BoxGeometry(boxSize.x, boxSize.y, boxSize.z);
    const material = new THREE.MeshBasicMaterial({ color: 0x00ff00, transparent: true, opacity: 0.12, depthTest: false });
    const boxMesh = new THREE.Mesh(geometry, material);
    const centerVec = new THREE.Vector3(center[0] || 0, center[1] || 0, center[2] || 0);

    // If a loaded file exists for this filename, use its object to convert center to world space
    if (filename) {
        const fileEntry = Array.from(loadedFiles.values()).find(f => f.filepath && (f.filepath.endsWith(filename) || f.filepath.includes(filename)));
        if (fileEntry && fileEntry.object) {
            // Compute bounding box center in local coordinates and convert to world
            fileEntry.geometry.computeBoundingBox();
            const bb = fileEntry.geometry.boundingBox;
            const localCenter = new THREE.Vector3();
            bb.getCenter(localCenter);
            fileEntry.object.localToWorld(localCenter);
            boxMesh.position.copy(localCenter);
        } else {
            boxMesh.position.copy(centerVec);
        }
    } else {
        boxMesh.position.copy(centerVec);
    }

    // Add a colored outline using EdgesGeometry
    const edges = new THREE.EdgesGeometry(geometry);
    const line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({ color: 0x00ff00, linewidth: 2 }));
    line.position.copy(boxMesh.position);

    scene.add(boxMesh);
    scene.add(line);
    const key = name || filename || 'highlight';
    highlightBoxes.set(key, boxMesh);
    highlightBoxes.set(key + ':outline', line);

    // Optionally, move camera target to center to focus on it
    // Smoothly animate camera to focus on the box
    animateCameraTo(boxMesh.position, { size: boxSize }, 700);
}

// 3D label functionality removed - keeping highlight-only experience

function animateCameraTo(targetCenter, options = {}, duration = 700) {
    if (!camera) return;
    if (cameraAnim) {
        // cancel existing animation
        cancelAnimationFrame(cameraAnim.raf);
    }

    const startPos = camera.position.clone();
    const startTarget = controls.target.clone();
    const endTarget = targetCenter.clone();

    // compute a desired distance based on bounding box size and camera FOV
    const size = options.size || new THREE.Vector3(1,1,1);
    const maxSize = Math.max(size.x, size.y, size.z);
    const fov = (camera.fov * Math.PI) / 180.0; // in radians
    // ensure some minimum distance factor
    const distance = Math.max(1.0, maxSize * 1.8 / Math.tan(fov / 2));

    // direction from center to camera
    const dir = camera.position.clone().sub(controls.target).normalize();
    const endPos = endTarget.clone().add(dir.multiplyScalar(distance));

    const startTime = performance.now();
    function tick(now) {
        const t = Math.min(1, (now - startTime) / duration);
        // smoothstep easing
        const s = t * t * (3 - 2 * t);
        camera.position.lerpVectors(startPos, endPos, s);
        controls.target.lerpVectors(startTarget, endTarget, s);
        controls.update();
        // label orientation is handled in updateFrameDependentUI

        if (t < 1) {
            cameraAnim.raf = requestAnimationFrame(tick);
        } else {
            cameraAnim = null;
        }
    }

    cameraAnim = { raf: requestAnimationFrame(tick) };
}

function createQueryResultsModal() {
    const modal = document.createElement('div');
    modal.id = 'query-results-modal';
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100%';
    modal.style.height = '100%';
    modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    modal.style.display = 'none';
    modal.style.justifyContent = 'center';
    modal.style.alignItems = 'center';
    modal.style.zIndex = '10000';
    modal.style.backdropFilter = 'blur(5px)';
    
    const modalContent = document.createElement('div');
    modalContent.style.backgroundColor = '#2a2a2a';
    modalContent.style.padding = '30px';
    modalContent.style.borderRadius = '12px';
    modalContent.style.maxWidth = '900px';
    modalContent.style.width = '90%';
    modalContent.style.maxHeight = '80vh';
    modalContent.style.overflow = 'auto';
    modalContent.style.boxShadow = '0 8px 32px rgba(0, 0, 0, 0.5)';
    modalContent.style.border = '1px solid rgba(255, 255, 255, 0.1)';
    
    const closeBtn = document.createElement('span');
    closeBtn.innerHTML = '&times;';
    closeBtn.style.float = 'right';
    closeBtn.style.fontSize = '32px';
    closeBtn.style.fontWeight = 'bold';
    closeBtn.style.color = '#aaa';
    closeBtn.style.cursor = 'pointer';
    closeBtn.style.lineHeight = '20px';
    closeBtn.style.transition = 'color 0.2s';
    
    closeBtn.addEventListener('mouseenter', () => {
        closeBtn.style.color = '#fff';
    });
    
    closeBtn.addEventListener('mouseleave', () => {
        closeBtn.style.color = '#aaa';
    });
    
    closeBtn.addEventListener('click', () => {
        modal.style.display = 'none';
    });
    
    const contentDiv = document.createElement('div');
    contentDiv.id = 'query-results-content';
    contentDiv.style.color = '#fff';
    contentDiv.style.marginTop = '20px';
    
    modalContent.appendChild(closeBtn);
    modalContent.appendChild(contentDiv);
    modal.appendChild(modalContent);
    
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            modal.style.display = 'none';
        }
    });
    
    document.body.appendChild(modal);
    return modal;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function onCanvasClick(event) 
{
    if (currentMode !== 'select') return;
    
    mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
    mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;
    
    raycaster.setFromCamera(mouse, camera);
    
    // Only check loaded file objects, not transform controls
    const objectsToCheck = [];
    loadedFiles.forEach((fileData) => {
        if (fileData.object && fileData.visible) {
            objectsToCheck.push(fileData.object);
        }
    });
    
    const intersects = raycaster.intersectObjects(objectsToCheck, false);
    
    if (intersects.length > 0) {
        // Find which file was clicked
        const clickedObject = intersects[0].object;
        
        // Deselect previous selection
        if (selectedFile) {
            const prevData = loadedFiles.get(selectedFile);
            if (prevData && prevData.object) {
                // Reset material color
                if (renderMode === 'points') {
                    prevData.object.material.vertexColors = true;
                    prevData.object.material.color.set(0xffffff);
                    prevData.object.material.needsUpdate = true;
                }
            }
        }
        
        // Find the file that owns this object
        for (const [filename, fileData] of loadedFiles.entries()) {
            if (fileData.object === clickedObject) {
                selectedFile = filename;
                console.log('Selected file:', filename);
                console.log('Selected point:', intersects[0].point);
                
                // Highlight selected object
                if (renderMode === 'points') {
                    clickedObject.material.vertexColors = false;
                    clickedObject.material.color.set(0xffffff);
                    clickedObject.material.needsUpdate = true;
                }
                
                // Attach transform controls to selected object
                transformControl.attach(clickedObject);
                transformControl.enabled = true;
                transformControl.visible = true;
                
                // Update UI to show selection
                updateObjectLabelsUI();
                // Create highlight box for selected object
                const fd = loadedFiles.get(filename);
                if (fd && fd.geometry) {
                    fd.geometry.computeBoundingBox();
                    const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray();
                    const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                    createHighlightBox({ name: filename, filename: filename, center, size });
                }
                break;
            }
        }
    } else {
        // Clicked on empty space - deselect
        deselectFile();
    }
}

function updateFileRender(filename) 
{
    const fileData = loadedFiles.get(filename);
    if (!fileData) return;
    
    // Remove current object if exists
    if (fileData.object) 
    {
        scene.remove(fileData.object);
    }
    
    // Don't render if not visible
    if (!fileData.visible) return;
    
    if (renderMode === 'points') 
    {
        const material = new THREE.PointsMaterial({
            size: 0.005,
            vertexColors: true,
            color: 0xffffff
        });
        fileData.object = new THREE.Points(fileData.geometry, material);  
        fileData.object.castShadow = true;
        fileData.object.receiveShadow = true;    
    } 
    else 
    {
        const material = new THREE.MeshStandardMaterial({
            vertexColors: true,
            flatShading: false,
            side: THREE.DoubleSide,
            roughness: 0.7,
            metalness: 0.0,
            envMapIntensity: 1.0
        });
        fileData.object = new THREE.Mesh(fileData.geometry, material);
        fileData.object.castShadow = true;
        fileData.object.receiveShadow = true;
        
        // Add lights for mesh rendering
        if (!ambientLight) 
        {
            ambientLight = new THREE.AmbientLight(0xffffff, 0.5);
            scene.add(ambientLight);
            
            directionalLight = new THREE.DirectionalLight(0xffffff, 0.8);
            directionalLight.position.set(5, 5, 5);
            directionalLight.castShadow = true;
            directionalLight.shadow.mapSize.width = 2048;
            directionalLight.shadow.mapSize.height = 2048;
            scene.add(directionalLight);

            const fillLight = new THREE.DirectionalLight(0xffffff, 0.3);
            fillLight.position.set(-5, 0, -5);
            scene.add(fillLight);
        }
    }
    
    scene.add(fileData.object);
}

function loadAllPLYFiles() 
{
    console.log('[Main] Starting to load PLY files with Web Workers...');
    
    plyFiles.forEach((filepath) => {
        const filename = filepath.split('/').pop();
        
        // Initialize file entry with loading state
        loadedFiles.set(filename, {
            geometry: null,
            object: null,
            visible: true,
            originalColors: null,
            codedColors: null,
            filepath: filepath,
            isPreview: true,
            loading: true
        });

        // Start loading with worker
        loaderManager.loadPLY(filepath, filename);
    });
    
    // Update UI to show loading state
    createFileCheckboxes();
}

/**
 * Callback when file data is loaded (called for previews and final data)
 */
function handleFileLoaded(filename, geometry, metadata) {
    const { isPreview, totalExpectedPoints, wasDownsampled, isIdleUpdate } = metadata;
    
    const fileData = loadedFiles.get(filename);
    if (!fileData) {
        console.warn(`File ${filename} not found in loadedFiles`);
        return;
    }

    // Ensure geometry has all required attributes
    ensureGeometryHasNormals(geometry);
    if (!geometry.attributes.color) {
        const defaultColors = createDefaultColors(geometry.attributes.position.count);
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(defaultColors, 3));
    }

    // For incremental updates, update geometry in-place to avoid recreating objects
    const isIncrementalUpdate = isPreview && fileData.geometry && fileData.object;
    
    if (isIncrementalUpdate && isIdleUpdate) {
        // Update existing geometry in-place (non-blocking)
        updateGeometryInPlace(fileData.geometry, geometry);
        
        // Update metadata
        fileData.isPreview = isPreview;
        fileData.loading = isPreview;
        fileData.wasDownsampled = wasDownsampled;
        
        // Update checkboxes (lightweight)
        createFileCheckboxes();
        // Ensure sceneInfo gets updated for previewed geometry too
        ensureSceneInfoForFile(filename);
    } else {
        // First load or final load - do full update
        
        // Store colors
        const originalColors = geometry.attributes.color.array.slice();
        const codedColors = createCodedColors(geometry);

        // Keep track of old object to remove it after new one is added
        const oldObject = fileData.object;

        // Update file data
        loadedFiles.set(filename, {
            ...fileData,
            geometry: geometry,
            originalColors: originalColors,
            codedColors: codedColors,
            isPreview: isPreview,
            loading: isPreview,
            wasDownsampled: wasDownsampled
        });

        // Apply current color mode
        applyColorMode(geometry, filename);
        
        // Render the new file (this creates new object)
        updateFileRender(filename);
        
        // Remove old object after new one is added (seamless transition)
        if (oldObject && oldObject !== loadedFiles.get(filename).object) {
            scene.remove(oldObject);
            if (oldObject.geometry) {
                oldObject.geometry.dispose();
            }
            if (oldObject.material) {
                oldObject.material.dispose();
            }
        }

        // Update UI
        createFileCheckboxes();
        updateObjectLabelsUI();

        const pointCount = geometry.attributes.position.count.toLocaleString();
        const status = isPreview ? `Preview (${pointCount} points)` : `Complete (${pointCount} points)`;
        console.log(`[${filename}] ${status}${wasDownsampled ? ' - downsampled' : ''}`);

        // If this was the selected file and we upgraded it, reattach transform controls
        if (!isPreview && selectedFile === filename) {
            const upgradedData = loadedFiles.get(filename);
            if (upgradedData && upgradedData.object) {
                transformControl.attach(upgradedData.object);
                transformControl.enabled = true;
                transformControl.visible = true;
                updateInfoIconPosition();
            }
        }
        // Generate/ensure sceneInfo entry for this file if info.json wasn't present
        ensureSceneInfoForFile(filename);
    }
}

/**
 * Update geometry attributes in-place without recreating the object
 */
function updateGeometryInPlace(targetGeometry, sourceGeometry) {
    // Update position attribute
    if (sourceGeometry.attributes.position) {
        targetGeometry.setAttribute('position', sourceGeometry.attributes.position);
    }
    
    // Update color attribute
    if (sourceGeometry.attributes.color) {
        targetGeometry.setAttribute('color', sourceGeometry.attributes.color);
    }
    
    // Update normal attribute
    if (sourceGeometry.attributes.normal) {
        targetGeometry.setAttribute('normal', sourceGeometry.attributes.normal);
    }
    
    // Update bounding box
    targetGeometry.computeBoundingBox();
    
    // Mark as needing update
    targetGeometry.attributes.position.needsUpdate = true;
    if (targetGeometry.attributes.color) {
        targetGeometry.attributes.color.needsUpdate = true;
    }
    if (targetGeometry.attributes.normal) {
        targetGeometry.attributes.normal.needsUpdate = true;
    }
}

/**
 * Callback for file loading progress
 */
function handleFileProgress(filename, message, progress) {
    console.log(`[${filename}] ${message} (${progress.toFixed(1)}%)`);
    
    // You can update UI here with progress bar if desired
    // For now, just update the checkbox label
    const fileData = loadedFiles.get(filename);
    if (fileData) {
        fileData.loadingMessage = message;
        fileData.loadingProgress = progress;
        createFileCheckboxes();
    }
}

/**
 * Callback for file loading errors
 */
function handleFileError(filename, error) {
    console.error(`[${filename}] Load error:`, error);
    
    const fileData = loadedFiles.get(filename);
    if (fileData) {
        fileData.loading = false;
        fileData.error = error;
        createFileCheckboxes();
    }
}

function ensureGeometryHasNormals(geometry) {
    if (!geometry) {
        return;
    }
    geometry.computeVertexNormals();
}

function createDefaultColors(pointCount) {
    const colors = new Float32Array(pointCount * 3);
    for (let i = 0; i < pointCount * 3; i++) {
        colors[i] = 1.0; // Default to white
    }
    return colors;
}

function createCodedColors(geometry) {
    geometry.computeBoundingBox();
    const bbox = geometry.boundingBox;
    const center = new THREE.Vector3();
    bbox.getCenter(center);

    const positions = geometry.attributes.position;
    const colors = [];
    let minDist = Infinity;
    let maxDist = -Infinity;

    for (let i = 0; i < positions.count; i++)
    {
        const x = positions.getX(i);
        const y = positions.getY(i);
        const z = positions.getZ(i);

        const dist = Math.sqrt(x*x + y*y + z*z);
        minDist = Math.min(minDist, dist);
        maxDist = Math.max(maxDist, dist);
    }

    for (let i = 0; i < positions.count; i++) 
    {
        const x = positions.getX(i);
        const y = positions.getY(i);
        const z = positions.getZ(i);
        const dist = Math.sqrt(x * x + y * y + z * z);
        
        const normalizedDist = (dist - minDist) / (maxDist - minDist);
        
        let r, g, b;
        if (normalizedDist < 0.5) 
        {   
            const t = normalizedDist * 2;
            r = 1 - t;
            g = 1;
            b = 0;
        } 
        else 
        {
            const t = (normalizedDist - 0.5) * 2; 
            r = t;
            g = 1 - t;
            b = 0;
        }
        
        colors.push(r, g, b);
    }

    return new Float32Array(colors);
}

function applyColorMode(geometry, filename) {
    const fileData = loadedFiles.get(filename);
    if (!fileData) return;

    const colorsToUse = colorMode === 'original' ? fileData.originalColors : fileData.codedColors;
    
    geometry.setAttribute(
        'color',
        new THREE.Float32BufferAttribute(colorsToUse, 3)
    );
}

function createFileCheckboxes() {
    const container = document.getElementById('object-labels-section');
    if (!container) {
        return;
    }
    const contentDiv = container.querySelector('.section-content');
    if (!contentDiv) {
        return;
    }

    contentDiv.innerHTML = '';

    if (loadedFiles.size === 0) {
        contentDiv.textContent = 'Loading objects...';
        return;
    }

    loadedFiles.forEach((fileData, filename) => {
        const label = document.createElement('label');
        label.dataset.filename = filename;
        label.style.display = 'flex';
        label.style.alignItems = 'center';
        label.style.marginBottom = '8px';
        label.style.cursor = 'pointer';
        label.style.flexDirection = 'column';
        label.style.alignItems = 'flex-start';

        const topRow = document.createElement('div');
        topRow.style.display = 'flex';
        topRow.style.alignItems = 'center';
        topRow.style.width = '100%';

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = fileData.visible;
        checkbox.style.marginRight = '8px';
        checkbox.addEventListener('change', (e) => {
            toggleFileVisibility(filename, e.target.checked);
        });

        const nameSpan = document.createElement('span');
        
        // Show status based on loading state
        let statusText = '';
        if (fileData.error) {
            statusText = ` (error: ${fileData.error})`;
            nameSpan.style.color = '#ff6b6b';
        } else if (fileData.loading) {
            statusText = fileData.isPreview ? ' (loading...)' : ' (processing...)';
            nameSpan.style.color = '#ffa500';
        } else if (fileData.isPreview) {
            statusText = ' (preview)';
            nameSpan.style.color = '#ffd700';
        } else if (fileData.wasDownsampled) {
            statusText = ' (downsampled)';
        }
        
        // Add friendly label(s) from sceneInfo if available
        let displayName = filename;
        if (sceneInfo && sceneInfo.displayNames && sceneInfo.displayNames.has(filename)) {
            const labs = sceneInfo.displayNames.get(filename);
            if (labs && labs.length > 0) {
                // If there is a human-friendly name, show it before filename
                displayName = `${labs.join(', ')} (${filename})`;
            }
        }
        nameSpan.textContent = displayName + statusText;
        nameSpan.style.cursor = 'pointer';
        nameSpan.title = 'Click to highlight this object';
        nameSpan.addEventListener('click', (e) => {
            e.stopPropagation();
            const fd = loadedFiles.get(filename);
            if (!fd || !fd.geometry) return;
            fd.geometry.computeBoundingBox();
            const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray();
            const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
            createHighlightBox({ name: filename, filename: filename, center, size });
        });

        topRow.appendChild(checkbox);
        topRow.appendChild(nameSpan);
        label.appendChild(topRow);

        // Show progress bar if loading
        if (fileData.loading && fileData.loadingProgress !== undefined) {
            const progressBar = document.createElement('div');
            progressBar.style.width = '100%';
            progressBar.style.height = '4px';
            progressBar.style.backgroundColor = 'rgba(255, 255, 255, 0.2)';
            progressBar.style.marginTop = '4px';
            progressBar.style.borderRadius = '2px';
            progressBar.style.overflow = 'hidden';

            const progressFill = document.createElement('div');
            progressFill.style.width = `${fileData.loadingProgress}%`;
            progressFill.style.height = '100%';
            progressFill.style.backgroundColor = '#4CAF50';
            progressFill.style.transition = 'width 0.3s ease';

            progressBar.appendChild(progressFill);
            label.appendChild(progressBar);

            if (fileData.loadingMessage) {
                const messageSpan = document.createElement('span');
                messageSpan.textContent = fileData.loadingMessage;
                messageSpan.style.fontSize = '10px';
                messageSpan.style.color = '#aaa';
                messageSpan.style.marginTop = '2px';
                label.appendChild(messageSpan);
            }
        }

        contentDiv.appendChild(label);
    });

    // Add export button for generated scene JSON (only once)
    let exportBtn = document.getElementById('export-scene-json-btn');
    if (!exportBtn) {
        exportBtn = document.createElement('button');
        exportBtn.id = 'export-scene-json-btn';
        exportBtn.addEventListener('click', exportSceneInfo);
    }
    exportBtn.textContent = 'Export Scene JSON';
    exportBtn.style.marginTop = '10px';
    exportBtn.style.padding = '6px 10px';
    exportBtn.style.fontSize = '12px';
    exportBtn.style.borderRadius = '6px';
    exportBtn.style.border = '1px solid rgba(255,255,255,0.08)';
    exportBtn.style.background = '#333';
    exportBtn.style.cursor = 'pointer';
    exportBtn.addEventListener('click', exportSceneInfo);
    // Remove existing and add button at end
    const existing = contentDiv.querySelector('#export-scene-json-btn');
    if (existing) existing.remove();
    contentDiv.appendChild(exportBtn);
}

function toggleFileVisibility(filename, visible) {
    const fileData = loadedFiles.get(filename);
    if (!fileData) return;

    fileData.visible = visible;

    if (visible) {
        // Render the file
        updateFileRender(filename);
    } else {
        // Remove from scene
        if (fileData.object) {
            scene.remove(fileData.object);
        }
        // Deselect if this was selected
        if (selectedFile === filename) {
            deselectFile();
        }
        // Clear any highlights that reference this file
        clearHighlights();
    }
}

function deselectFile() {
    // Reset color of previously selected file
    if (selectedFile) {
        const prevData = loadedFiles.get(selectedFile);
        if (prevData && prevData.object) {
            if (renderMode === 'points') {
                prevData.object.material.vertexColors = true;
                prevData.object.material.color.set(0xffffff);
                prevData.object.material.needsUpdate = true;
            }
        }
    }
    
    selectedFile = null;
    if (transformControl) {
        transformControl.detach();
        transformControl.enabled = false;
        transformControl.visible = false;
    }
    
    // Hide info icon
    if (infoIcon) {
        infoIcon.style.display = 'none';
    }

    // Clear any highlights when deselecting
    clearHighlights();
    
    updateObjectLabelsUI();
}

function updateObjectLabelsUI() {
    const container = document.getElementById('object-labels-section');
    if (!container) {
        return;
    }
    const contentDiv = container.querySelector('.section-content');
    if (!contentDiv) {
        return;
    }
    
    // Update checkboxes to highlight selected file
    const labels = contentDiv.querySelectorAll('label');
    labels.forEach(label => {
        const filename = label.dataset.filename || label.textContent.trim();
        if (filename === selectedFile) {
            label.style.background = 'rgba(76, 175, 80, 0.3)';
            label.style.fontWeight = 'bold';
        } else {
            label.style.background = '';
            label.style.fontWeight = 'normal';
        }
    });
    
    if (selectedFile) {
        console.log('Currently selected:', selectedFile);
    }
}

function onWindowResize() 
{
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
}

function onKeyDown(event) {
    if (!selectedFile || currentMode !== 'select') return;
    
    switch (event.key.toLowerCase()) {
        case 'g': // Translate (Grab in Blender)
        case 't':
            transformControl.setMode('translate');
            console.log('Transform mode: Translate');
            break;
        case 'r': // Rotate
            transformControl.setMode('rotate');
            console.log('Transform mode: Rotate');
            break;
        case 's': // Scale
            transformControl.setMode('scale');
            console.log('Transform mode: Scale');
            break;
        case 'escape': // Deselect
            deselectFile();
            break;
    }
}

function animate() 
{
    requestAnimationFrame(animate);
    controls.update();
    renderer.render(scene, camera);
    
    // Update info icon position every frame
    if (selectedFile) {
        updateInfoIconPosition();
    }
    // Update highlight label positions/frame dependent UI
    updateFrameDependentUI();
}