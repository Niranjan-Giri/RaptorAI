import * as THREE from 'three';

const GEMINI_API_KEY = import.meta.env.VITE_GEMINI_API_KEY;

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
        normalizeQuestion
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

    async function geminiQueryHandler(question, sceneFiles, abortSignal) {
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
                sceneMapping: app.sceneInfo ? 'Available' : 'Not available',
                lastReferencedObject: lastReferencedFilename,
                recentConversation: conversationHistory.slice(-8)
            };
            // Dynamic Distance Calculation (On-Demand)
            context.calculated_distances = [];
            const qLower = question.toLowerCase().replace(/[.,/#!$%^&*;:{}=\-_`~()]/g, " "); // Replace punctuation with space
            const qTokens = new Set(qLower.split(/\s+/));
            
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
            
            // If the query specifically asks for distance/far/close AND targets found
            const distKeywords = ['distance', 'dist', 'far', 'close', 'near', 'between'];
            const isDistanceQuery = distKeywords.some(k => qLower.includes(k));

            if (isDistanceQuery && targetFiles.length >= 2) {
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
            - To move an object relatively: [ACTION:MOVE:'filename.ply':'direction':amount]
              Directions: left, right, up, down, forward, back
              Amount: numeric value (e.g., 1.5, 2, 0.5)
              Example: [ACTION:MOVE:'B3_S4.ply':'left':2]
            - To move an object to absolute position: [ACTION:POSITION:'filename.ply':x:y:z]
              Example: [ACTION:POSITION:'B3_S4.ply':1.5:0:0]
            - To rotate an object: [ACTION:ROTATE:'filename.ply':'axis':degrees]
              Axis: x, y, z
              Example: [ACTION:ROTATE:'B3_S4.ply':'y':90]

            RULES:
            - If the question asks to highlight or find an object, provide the exact filename in your response so the user knows.
            - If the question asks for distance, look at the 'calculated_distances' array.
            - If you mention a filename, put it in single quotes like 'filename.ply'.
            - When asked to move objects, use the appropriate ACTION code.
            - For relative movements (left, right, etc.), use reasonable default amounts like 1 or 2 units if not specified.
            - IMPORTANT MEMORY RULE: if user says pronouns like "it", "that", "this", "them", resolve to context.lastReferencedObject when available.`;
            /****************************************************************** */
            const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`;

            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                signal: abortSignal,
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
            if (error?.name === 'AbortError') {
                return { handled: false, aborted: true, error: 'Request aborted by user' };
            }
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
                    
                    // Display cleaned message
                    if (ui) ui.showInlineQueryMessage(userDisplayMessage.trim(), 'assistant');
                    addHistory('assistant', userDisplayMessage.trim());

                    const filenameFromAnswer = detectFilenameMention(userDisplayMessage);
                    if (filenameFromAnswer) lastReferencedFilename = filenameFromAnswer;
                    
                    // Optional: Try to detect filename in AI response to highlight only if NOT hiding
                    if (hideMatches.length === 0) {
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