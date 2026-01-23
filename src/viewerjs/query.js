import * as THREE from 'three';

const GEMINI_API_KEY = import.meta.env.VITE_GEMINI_API_KEY;

export function createQueryHandler(app, sceneManager, ui) {
    const queryCache = new Map();

    const handler = {
        getSceneMetadata,
        localQueryHandler,
        handleQuerySend,
        normalizeQuestion
    };

    return handler;

    function normalizeQuestion(q) {
        return q.trim().toLowerCase();
    }

    function getSceneMetadata() {
        const files = [];
        app.loadedFiles.forEach((fileData, filename) => {
            if (!fileData.geometry) return;
            const geometry = fileData.geometry; geometry.computeBoundingBox(); const bbox = geometry.boundingBox;
            const center = new THREE.Vector3(); bbox.getCenter(center);
            files.push({ filename, visible: !!fileData.visible, vertex_count: geometry.attributes.position.count, bbox: { min: bbox.min.toArray(), max: bbox.max.toArray(), size: bbox.getSize(new THREE.Vector3()).toArray(), center: center.toArray() } });
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
            const count = filenamesLower.filter(f => f.includes(object)).length; responseData.results=[{object, count}]; responseData.columns=['object','count']; responseData.row_count=1; queryCache.set(q, responseData); return { handled: true, data: responseData };
        }
        match = q.match(/is there (?:a|an|the )?([\w\s-]+)/);
        if (match) {
            let object = match[1]; object = object.trim().toLowerCase(); if (object.endsWith('s')) object = object.slice(0, -1);
            let exists = filenamesLower.some(f => f.includes(object));
            if (app.sceneInfo && app.sceneInfo._map) {
                const lower = object.toLowerCase(); const entry = app.sceneInfo._map.get(lower); exists = Boolean(entry) || sceneFiles.some(f => f.filename.toLowerCase().includes(lower));
                if (entry) {
                    responseData.results = [{ object, exists: true, filename: entry.filename }]; responseData.columns=['object','exists','filename']; responseData.row_count=1; queryCache.set(q, responseData); return { handled: true, data: responseData };
                }
            }
            responseData.results = [{ object, exists }]; responseData.columns = ['object','exists']; responseData.row_count = 1; queryCache.set(q, responseData); return { handled: true, data: responseData };
        }
        match = q.match(/where is (?:a|an|the )?([\w\s-]+)/);
        if (match) {
            let object = match[1]; object = object.trim().toLowerCase(); if (object.endsWith('s')) object = object.slice(0, -1);
            if (app.sceneInfo && app.sceneInfo._map) {
                const lower = object.toLowerCase(); let entry = app.sceneInfo._map.get(lower);
                if (!entry) { for (const [k, v] of app.sceneInfo._map.entries()) { if (k.includes(lower) || lower.includes(k)) { entry = v; break; } } }
                if (entry) {
                    let center = [0,0,0]; let size = [1,1,1];
                    if (entry.filename) {
                        const f = sceneFiles.find(ff => ff.filename.toLowerCase().includes(String(entry.filename).toLowerCase())); if (f) { center = f.bbox.center; size = f.bbox.size; }
                    }
                    const bboxInfo = app.sceneInfo.bounding_box && app.sceneInfo.bounding_box[entry.key]; if (bboxInfo) size = [bboxInfo.x || size[0], bboxInfo.y || size[1], bboxInfo.z || size[2]];
                    responseData.results=[{ object: entry.key, center, size, filename: entry.filename }]; responseData.columns = ['object','center','size']; responseData.row_count=1; queryCache.set(q, responseData); return { handled: true, data: responseData };
                } else { responseData.results=[{ object, exists: false }]; responseData.columns=['object','exists']; responseData.row_count=1; queryCache.set(q, responseData); return { handled: true, data: responseData }; }
            } else { let file = sceneFiles.find(f => f.filename.toLowerCase().includes(object)); if (file) { responseData.results=[{ object, center: file.bbox.center, size: file.bbox.size, filename: file.filename }]; responseData.columns=['object','center','size']; responseData.row_count=1; queryCache.set(q, responseData); return { handled: true, data: responseData }; } }
        }
        match = q.match(/vertex count (?:of )?(?:the )?([\w\s-]+)|how many vertices (?:in|for) ([\w\s-]+)/);
        if (match) {
            const object = (match[1] || match[2] || '').trim().toLowerCase(); const file = sceneFiles.find(f => f.filename.toLowerCase().includes(object)); if (file) { responseData.results=[{ object: file.filename, vertex_count: file.vertex_count }]; responseData.columns=['object','vertex_count']; responseData.row_count=1; queryCache.set(q, responseData); return { handled: true, data: responseData }; }
        }
        return { handled: false };
    }

    async function geminiQueryHandler(question, sceneFiles) {
        if (!GEMINI_API_KEY) {
            return { handled: false, error: 'Gemini API Key not configured' };
        }

        try {
            // Prepare context about the scene for the LLM
            const context = {
                fileCount: sceneFiles.length,
                files: sceneFiles.map(f => ({
                    filename: f.filename,
                    visible: f.visible,
                    vertices: f.vertex_count,
                    position: f.bbox.center,
                    size: f.bbox.size
                })),
                sceneMapping: app.sceneInfo ? 'Available' : 'Not available'
            };
            // Dynamic Distance Calculation (On-Demand)
            context.calculated_distances = [];
            const qLower = question.toLowerCase().replace(/[.,/#!$%^&*;:{}=\-_`~()]/g, " "); // Replace punctuation with space
            const qTokens = new Set(qLower.split(/\s+/));
            
            console.log('[Gemini] Analyzing query for objects:', qLower);

            // Helper to check if file is mentioned
            const getMentionedFiles = () => {
                const mentioned = new Set();
                sceneFiles.forEach(f => {
                    const fname = f.filename.toLowerCase(); // e.g. "b3_s4.ply"
                    const fnameNoExt = fname.replace('.ply', ''); // "b3_s4"
                    const fnameParts = fnameNoExt.split(/[_-\s]+/); // ["b3", "s4"]

                    // Strategy 1: Exact match of filename or no-ext (relaxed spaces)
                    // "B3_S4" -> matches "b3_s4" or "b3 s4"
                    const relaxedName = fnameNoExt.replace(/[_-\s]+/g, ' ');
                    
                    if (qLower.includes(fname) || qLower.includes(fnameNoExt) || qLower.includes(relaxedName)) {
                        mentioned.add(f);
                        return;
                    }

                    // Strategy 2: Check aliases
                    if (app.sceneInfo && app.sceneInfo._map) {
                        for (const [key, val] of app.sceneInfo._map.entries()) {
                            // key is already lowercased token from map
                            if (qTokens.has(key) || qLower.includes(key)) {
                                if (val.filename === f.filename) {
                                    mentioned.add(f);
                                    return;
                                }
                            }
                        }
                    }
                });
                return Array.from(mentioned).filter(Boolean);
            };

            const targetFiles = getMentionedFiles();
            console.log('[Gemini] Targets found:', targetFiles.map(f => f.filename));
            
            // If the query specifically asks for distance/far/close AND targets found
            const distKeywords = ['distance', 'dist', 'far', 'close', 'near', 'between'];
            const isDistanceQuery = distKeywords.some(k => qLower.includes(k));

            if (isDistanceQuery && targetFiles.length >= 2) {
                console.log('[Gemini] Calculating distances for targets...');
                for (let i = 0; i < targetFiles.length; i++) {
                    for (let j = i + 1; j < targetFiles.length; j++) {
                        const f1 = targetFiles[i];
                        const f2 = targetFiles[j];
                        const p1 = new THREE.Vector3().fromArray(f1.bbox.center);
                        const p2 = new THREE.Vector3().fromArray(f2.bbox.center);
                        const dist = p1.distanceTo(p2);
                        const entry = {
                            pair: `${f1.filename} <-> ${f2.filename}`,
                            distance: parseFloat(dist.toFixed(3))
                        };
                        context.calculated_distances.push(entry);
                    }
                }
                console.log('[Gemini] Calculated:', context.calculated_distances);
            }


            /*************************************************************/
            const systemPrompt = `You are an intelligent assistant for a 3D Point Cloud Viewer. 
            You answer questions about the current scene.
            
            Current Scene Data:
            ${JSON.stringify(context, null, 2)}
            
            User Question: "${question}"
            
            Answer concisely. 
            
            ACTIONS:
            If the user asks to perform an action, include one of the following codes in your response:
            - To zoom in on the scene: [ACTION:ZOOM_IN]
            - To zoom out of the scene: [ACTION:ZOOM_OUT]
            - To show/add an object: [ACTION:SHOW:'filename.ply']
            - To hide/remove an object: [ACTION:HIDE:'filename.ply']

            RULES:
            - If the question asks to highlight or find an object, provide the exact filename in your response so the user knows.
            - If the question asks for distance, look at the 'calculated_distances' array.
            - If you mention a filename, put it in single quotes like 'filename.ply'.`;
            /****************************************************************** */
            const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`;
            console.log('[Gemini] Request URL:', url.replace(GEMINI_API_KEY, 'HIDDEN'));

            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    contents: [{ parts: [{ text: systemPrompt }] }]
                })
            });

            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.error?.message || 'Gemini API Error');
            }

            const data = await response.json();
            const answer = data.candidates?.[0]?.content?.parts?.[0]?.text || "I couldn't generate an answer.";
            
            return { handled: true, answer: answer };

        } catch (error) {
            console.error('Gemini API Error:', error);
            return { handled: false, error: error.message };
        }
    }

    async function handleQuerySend() {
        const queryInput = document.getElementById('query-input'); if (!queryInput) return; const query = queryInput.value.trim(); if (query === '') return;
        console.log('Query submitted:', query);
        const querySendBtn = document.getElementById('query-send-btn'); const originalBtnHTML = querySendBtn ? querySendBtn.innerHTML : null; 
        
        // UX: Show "Thinking..." state
        if (querySendBtn) { querySendBtn.innerHTML = '<div class="spinner"></div>'; querySendBtn.disabled = true; }
        if (ui) ui.showInlineQueryMessage('Thinking...', 'info');

        try {
            //I have commented out this block, so that the fall back
            //will be from gemini, and not from regex
            //we can uncomment later



            /*
            // 1. Try Local Regex Match first
            const localResponse = localQueryHandler(query);
            
            if (localResponse && localResponse.handled) {
                const first = localResponse.data.results && localResponse.data.results[0];
                if (first && (first.center || first.size || first.filename)) {
                    let filename = first.filename;
                    if (filename) {
                        const f = Array.from(app.loadedFiles.entries()).find(([name, fd]) => name.toLowerCase() === String(filename).toLowerCase() || fd.filepath.toLowerCase().endsWith(String(filename).toLowerCase()));
                        if (f) {
                            const [name, fd] = f;
                            if (fd.geometry) {
                                fd.geometry.computeBoundingBox(); const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray(); const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray(); sceneManager.createHighlightBox({ name, filename: name, center, size });
                            } else {
                                sceneManager.createHighlightBox({ name: filename, filename, center: first.center || [0,0,0], size: first.size || [1,1,1] });
                            }
                        } else {
                            sceneManager.createHighlightBox({ name: filename, filename, center: first.center || [0,0,0], size: first.size || [1,1,1] });
                        }
                    } else {
                        sceneManager.createHighlightBox({ name: first.object, filename: first.object, center: first.center || [0,0,0], size: first.size || [1,1,1] });
                    }
                } else if (first && first.exists !== undefined) {
                    const existsMsg = first.exists ? `Yes, ${first.object} is present.` : `No, ${first.object} is not present.`; if (ui) ui.showInlineQueryMessage(existsMsg, first.exists ? 'success' : 'error');
                } else if (localResponse.data && localResponse.data.results && localResponse.data.results.length > 0) {
                    const row = localResponse.data.results[0]; const keys = Object.keys(row || {}); if (keys.length > 0) { const msg = keys.map(k => `${k}: ${row[k]}`).join(', '); if (ui) ui.showInlineQueryMessage(msg, 'info'); }
                } else {
                    if (ui) ui.showInlineQueryMessage('No results found.', 'error');
                }
            } else {
            */

                // Gemini AI Only Mode
                const sceneFiles = getSceneMetadata();
                const aiResponse = await geminiQueryHandler(query, sceneFiles);
                
                if (aiResponse.handled && !aiResponse.error && aiResponse.answer && aiResponse.answer !== "I couldn't generate an answer.") {
                    const rawAnswer = aiResponse.answer;
                    
                    // Parse Actions
                    let userDisplayMessage = rawAnswer;
                    
                    // Check for ZOOM_IN
                    if (rawAnswer.includes('[ACTION:ZOOM_IN]')) {
                        console.log('[Gemini Action] Zoom In');
                        if (ui && ui.zoomIn) ui.zoomIn();
                        userDisplayMessage = userDisplayMessage.replace(/\[ACTION:ZOOM_IN\]/g, '');
                    }
                    
                    // Check for ZOOM_OUT
                    if (rawAnswer.includes('[ACTION:ZOOM_OUT]')) {
                        console.log('[Gemini Action] Zoom Out');
                        if (ui && ui.zoomOut) ui.zoomOut();
                        userDisplayMessage = userDisplayMessage.replace(/\[ACTION:ZOOM_OUT\]/g, '');
                    }

                    // Check for HIDE object
                    const hideMatch = rawAnswer.match(/\[ACTION:HIDE:'(.*?)'\]/);
                    if (hideMatch && hideMatch[1]) {
                        const filename = hideMatch[1];
                        console.log('[Gemini Action] Hide:', filename);
                        if (sceneManager && sceneManager.toggleFileVisibility) {
                            sceneManager.toggleFileVisibility(filename, false);
                            // Also update checkbox UI if possible
                            if (ui && ui.createFileCheckboxes) ui.createFileCheckboxes();
                        }
                        userDisplayMessage = userDisplayMessage.replace(hideMatch[0], '');
                    }

                    // Check for SHOW object
                    const showMatch = rawAnswer.match(/\[ACTION:SHOW:'(.*?)'\]/);
                    if (showMatch && showMatch[1]) {
                        const filename = showMatch[1];
                        console.log('[Gemini Action] Show:', filename);
                        if (sceneManager && sceneManager.toggleFileVisibility) {
                            sceneManager.toggleFileVisibility(filename, true);
                             // Also update checkbox UI if possible
                            if (ui && ui.createFileCheckboxes) ui.createFileCheckboxes();
                        }
                        userDisplayMessage = userDisplayMessage.replace(showMatch[0], '');
                    }
                    
                    // Display cleaned message
                    if (ui) ui.showInlineQueryMessage(userDisplayMessage.trim(), 'success');
                    
                    // Optional: Try to detect filename in AI response to highlight only if NOT hiding
                    if (!hideMatch) {
                        const potentialFiles = sceneFiles.map(f => f.filename);
                        for (const file of potentialFiles) {
                            // Simple check if filename appears in the answer
                            if (userDisplayMessage.includes(file)) {
                                // trigger highlight if exact match found
                                const f = app.loadedFiles.get(file);
                                if (f && f.geometry && f.visible) { // Only highlight if visible
                                    f.geometry.computeBoundingBox();
                                    const center = f.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray();
                                    const size = f.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                                    sceneManager.createHighlightBox({ name: file, filename: file, center, size });
                                }
                            }
                        }
                    }

                } else if (aiResponse.error === 'Gemini API Key not configured') {
                     if (ui) ui.showInlineQueryMessage('Query not understood. Configure Gemini API Key to enable AI.', 'error');
                } else {
                     if (ui) ui.showInlineQueryMessage('No answer found.', 'error');
                }
            /*
            }
            */
        } finally {
            if (querySendBtn) { querySendBtn.innerHTML = originalBtnHTML; querySendBtn.disabled = false; }
            queryInput.value = '';
        }
    }
}