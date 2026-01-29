import * as THREE from 'three';

export function createUIManager(app, sceneManager, queryHandler) {
    // Setup initial UI elements and return public small API
    const ui = {
        createInfoIcon,
        createInfoModal,
        showInlineQueryMessage,
        createFileCheckboxes,
        updateObjectLabelsUI,
        updateInfoIconPosition,
        showInfoModal,
        createQueryResultsModal,
        ensureSceneInfoForFile,
        setupMenuControls,
        zoomIn,
        zoomOut
    };

    // Create initial UI elements
    createInfoIcon();
    createInfoModal();
    createQueryResultsModal();
    // Don't call setupMenuControls here - wait for React to mount buttons
    // setupMenuControls();

    return ui;

    // -------------------- Implementation -----------------------
    function createInfoIcon() {
        app.infoIcon = document.createElement('div');
        app.infoIcon.id = 'info-icon';
        app.infoIcon.innerHTML = '&#9432;';
        Object.assign(app.infoIcon.style, {
            position: 'absolute', width: '24px', height: '24px', borderRadius: '50%',
            backgroundColor: 'rgba(33, 150, 243, 0.9)', color: 'white', display: 'none', justifyContent: 'center', alignItems: 'center',
            cursor: 'pointer', fontWeight: 'bold', fontSize: '16px', fontFamily: 'Arial, sans-serif', border: '2px solid white', zIndex: '1000', pointerEvents: 'auto', transition: 'all 0.2s ease'
        });
        app.infoIcon.addEventListener('mouseenter', () => {
            app.infoIcon.style.transform = 'scale(1.1)'; app.infoIcon.style.backgroundColor = 'rgba(33, 150, 243, 1)';
        });
        app.infoIcon.addEventListener('mouseleave', () => {
            app.infoIcon.style.transform = 'scale(1)'; app.infoIcon.style.backgroundColor = 'rgba(33, 150, 243, 0.9)';
        });
        app.infoIcon.addEventListener('click', (e) => { e.stopPropagation(); showInfoModal(); });
        document.body.appendChild(app.infoIcon);
    }

    function createInfoModal() {
        const modal = document.createElement('div');
        modal.id = 'info-modal';
        Object.assign(modal.style, { position: 'fixed', top: '0', left: '0', width: '100%', height: '100%', backgroundColor: 'rgba(0,0,0,0.7)', display: 'none', justifyContent: 'center', alignItems: 'center', zIndex: '10000', backdropFilter: 'blur(5px)'});
        const modalContent = document.createElement('div');
        Object.assign(modalContent.style, { backgroundColor: '#2a2a2a', padding: '30px', borderRadius: '12px', maxWidth: '500px', width: '90%', maxHeight: '80vh', overflow: 'auto', boxShadow: '0 8px 32px rgba(0, 0, 0, 0.5)', border: '1px solid rgba(255,255,255,0.1)' });
        const closeBtn = document.createElement('span');
        closeBtn.innerHTML = '&times;';
        Object.assign(closeBtn.style, { float: 'right', fontSize: '32px', fontWeight: 'bold', color: '#aaa', cursor: 'pointer', lineHeight: '20px', transition: 'color 0.2s' });
        closeBtn.addEventListener('mouseenter', () => closeBtn.style.color = '#fff');
        closeBtn.addEventListener('mouseleave', () => closeBtn.style.color = '#aaa');
        closeBtn.addEventListener('click', () => modal.style.display = 'none');
        const contentDiv = document.createElement('div');
        contentDiv.id = 'modal-content-info'; contentDiv.style.color = '#fff'; contentDiv.style.marginTop = '20px';
        modalContent.appendChild(closeBtn); modalContent.appendChild(contentDiv); modal.appendChild(modalContent);
        modal.addEventListener('click', (e) => { if (e.target === modal) modal.style.display = 'none'; });
        document.body.appendChild(modal);
    }

    function showInfoModal() {
        if (!app.selectedFile) return;
        const fileData = app.loadedFiles.get(app.selectedFile);
        if (!fileData) return;
        const modal = document.getElementById('info-modal');
        const contentDiv = document.getElementById('modal-content-info');
        const geometry = fileData.geometry; const object = fileData.object;
        const vertexCount = geometry.attributes.position.count;
        const position = object.position; const rotation = object.rotation; const scale = object.scale;
        geometry.computeBoundingBox(); const bbox = geometry.boundingBox; const size = new THREE.Vector3(); bbox.getSize(size);
        let html = `...`; // fill minimal
        html = `
        <h2 style="margin-top:0; color: #4CAF50; border-bottom: 2px solid #4CAF50; padding-bottom: 10px;">Object Information</h2>
        <div style="line-height: 1.8; color: #fff;">
            <p><strong style="color: #2196F3;">File Name:</strong> ${app.selectedFile}</p>
            <!-- <p><strong style="color: #2196F3;">File Path:</strong> ${fileData.filepath}</p> -->
            <p><strong style="color: #2196F3;">Render Mode:</strong> ${app.renderMode === 'points' ? 'Point Cloud' : '3D Mesh'}</p>
            <hr style="border: none; border-top: 1px solid rgba(255,255,255,0.1); margin: 15px 0;">
            <h3 style="color: #FF9800; margin-bottom: 10px;">Geometry</h3>
            <p><strong>Vertex Count:</strong> ${vertexCount.toLocaleString()}</p>
            <p><strong>Bounding Box Size:</strong></p>
            <ul style="margin-left: 20px;">
                <li>Width (X): ${size.x.toFixed(4)}</li>
                <li>Height (Y): ${size.y.toFixed(4)}</li>
                <li>Depth (Z): ${size.z.toFixed(4)}</li>
            </ul>
            <hr style="border: none; border-top: 1px solid rgba(255,255,255,0.1); margin: 15px 0;">
            <h3 style="color: #FF9800; margin-bottom: 10px;">Transform</h3>
            <p><strong>Position:</strong></p>
            <ul style="margin-left: 20px;">
                <li>X: ${position.x.toFixed(4)}</li>
                <li>Y: ${position.y.toFixed(4)}</li>
                <li>Z: ${position.z.toFixed(4)}</li>
            </ul>
            <p><strong>Rotation (radians):</strong></p>
            <ul style="margin-left: 20px;">
                <li>X: ${rotation.x.toFixed(4)}</li>
                <li>Y: ${rotation.y.toFixed(4)}</li>
                <li>Z: ${rotation.z.toFixed(4)}</li>
            </ul>
            <p><strong>Scale:</strong></p>
            <ul style="margin-left: 20px;">
                <li>X: ${scale.x.toFixed(4)}</li>
                <li>Y: ${scale.y.toFixed(4)}</li>
                <li>Z: ${scale.z.toFixed(4)}</li>
            </ul>
        </div>
        `;
        contentDiv.innerHTML = html;
        modal.style.display = 'flex';
    }

    function showInlineQueryMessage(text, type = 'info', duration = 0) {
        // Preferred anchor: the input container inside the query section
        const inputContainer = document.querySelector('#query-section .section-content .query-input-container');
        const queryContainer = inputContainer ? inputContainer.parentElement : document.querySelector('#query-section .section-content');
        if (!queryContainer) { console.log('[Query] ' + text); return; }

        // Remove any existing message
        const existing = document.getElementById('query-inline-msg');
        if (existing) existing.remove();

        const msg = document.createElement('div');
        msg.id = 'query-inline-msg';
        
        // Simple Markdown Parser for Gemini responses
        // 1. **bold** -> <strong>bold</strong>
        // 2. 'code' -> <code>code</code>
        // 3. `code` -> <code>code</code>
        let formattedText = text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/'(.*?)'/g, '<code style="background:rgba(255,255,255,0.15); padding:2px 4px; borderRadius:3px; font-family:monospace;">$1</code>')
            .replace(/`(.*?)`/g, '<code style="background:rgba(255,255,255,0.15); padding:2px 4px; borderRadius:3px; font-family:monospace;">$1</code>')
            .replace(/\n/g, '<br>');

        msg.innerHTML = formattedText;

        // More visible styles for multiline error/info messages below the input
        Object.assign(msg.style, {
            marginTop: '8px',
            padding: '8px 10px',
            borderRadius: '6px',
            fontSize: '13px',
            width: '100%',
            boxSizing: 'border-box',
            whiteSpace: 'normal',
            overflow: 'visible'
        });
        if (type === 'success') {
            msg.style.background = 'rgba(76,175,80,0.12)';
            msg.style.border = '1px solid rgba(76,175,80,0.25)';
            msg.style.color = '#c8ffd1';
        } else if (type === 'error') {
            msg.style.background = 'rgba(244,67,54,0.12)';
            msg.style.border = '1px solid rgba(244,67,54,0.25)';
            msg.style.color = '#ffd6d1';
        } else {
            msg.style.background = 'rgba(255,255,255,0.04)';
            msg.style.border = '1px solid rgba(255,255,255,0.06)';
            msg.style.color = '#fff';
        }

        // insert it after the input container so it appears below; fallback to appendChild
        if (inputContainer && inputContainer.parentElement) {
            inputContainer.insertAdjacentElement('afterend', msg);
        } else {
            queryContainer.appendChild(msg);
        }

        if (duration > 0) {
            setTimeout(() => {
                if (msg.parentElement) msg.remove();
            }, duration);
        }
    }

    function createFileCheckboxes() {
        const container = document.getElementById('object-labels-section'); if (!container) return; const contentDiv = container.querySelector('.section-content'); if (!contentDiv) return;
        console.log('[UI] createFileCheckboxes, loadedFiles.size=', app.loadedFiles.size);
        contentDiv.innerHTML = '';
        if (app.loadedFiles.size === 0) { contentDiv.textContent = 'Loading objects...'; return; }
        app.loadedFiles.forEach((fileData, filename) => {
            console.log(`[UI] file ${filename} status: visible=${fileData.visible} loading=${fileData.loading} isPreview=${fileData.isPreview}`);
            const label = document.createElement('label'); label.dataset.filename = filename; label.style.display = 'flex'; label.style.alignItems = 'center'; label.style.marginBottom = '8px'; label.style.cursor = 'pointer'; label.style.flexDirection='column'; label.style.alignItems='flex-start';
            const topRow = document.createElement('div'); topRow.style.display='flex'; topRow.style.alignItems='center'; topRow.style.width='100%';
            const checkbox = document.createElement('input'); checkbox.type='checkbox'; checkbox.checked = fileData.visible; checkbox.style.marginRight='8px'; checkbox.addEventListener('change',(e)=> { sceneManager.toggleFileVisibility(filename, e.target.checked); });
            const nameSpan = document.createElement('span');
            let statusText = '';
            if (fileData.error) { statusText = ` (error: ${fileData.error})`; nameSpan.style.color='#ff6b6b'; }
            else if (fileData.loading) { statusText = fileData.isPreview ? ' (loading...)' : ' (processing...)'; nameSpan.style.color='#ffa500'; }
            else if (fileData.isPreview) { statusText = ' (preview)'; nameSpan.style.color='#ffd700'; }
            else if (fileData.wasDownsampled) { statusText = ' (downsampled)'; }
            let displayName = filename;
            if (app.sceneInfo && app.sceneInfo.displayNames && app.sceneInfo.displayNames.has(filename)) {
                const labs = app.sceneInfo.displayNames.get(filename); if (labs && labs.length>0) displayName = `${labs.join(', ')} (${filename})`;
            }
            nameSpan.textContent = displayName + statusText; nameSpan.style.cursor='pointer'; nameSpan.title='Click to highlight this object';
            nameSpan.addEventListener('click', (e)=>{ e.stopPropagation(); const fd = app.loadedFiles.get(filename); if (!fd || !fd.geometry) return; fd.geometry.computeBoundingBox(); const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray(); const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray(); sceneManager.createHighlightBox({ name: filename, filename: filename, center, size }); });
            topRow.appendChild(checkbox); topRow.appendChild(nameSpan); label.appendChild(topRow);
            if (fileData.loading && fileData.loadingProgress !== undefined) {
                const progressBar = document.createElement('div'); Object.assign(progressBar.style,{ width:'100%', height:'4px', backgroundColor:'rgba(255,255,255,0.2)', marginTop:'4px', borderRadius:'2px', overflow:'hidden'});
                const progressFill = document.createElement('div'); progressFill.style.width = `${fileData.loadingProgress}%`; progressFill.style.height='100%'; progressFill.style.backgroundColor='#4CAF50'; progressFill.style.transition='width 0.3s ease';
                progressBar.appendChild(progressFill); label.appendChild(progressBar);
                if (fileData.loadingMessage) { const messageSpan = document.createElement('span'); messageSpan.textContent = fileData.loadingMessage; messageSpan.style.fontSize='10px'; messageSpan.style.color='#aaa'; messageSpan.style.marginTop='2px'; label.appendChild(messageSpan);} }
            contentDiv.appendChild(label);
        });
    }

    function exportSceneInfo() {
        const out = { name: {}, bounding_box: {}, labels: {} };
        if (app.sceneInfo && app.sceneInfo.name) out.name = { ...app.sceneInfo.name };
        if (app.sceneInfo && app.sceneInfo.bounding_box) {
            for (const [k,v] of Object.entries(app.sceneInfo.bounding_box)) {
                out.bounding_box[k] = { x: v.x, y: v.y, z: v.z };
            }
        }
        if (app.sceneInfo && app.sceneInfo.labels) out.labels = { ...app.sceneInfo.labels };
        
        const blob = new Blob([JSON.stringify(out, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'info.json';
        a.style.display = 'none';
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        
        showInlineQueryMessage('Scene info exported as info.json. You can use this file with your PLY files for faster loading next time!', 'success');
        console.log('[SceneInfo] Exported generated scene info:', out);
    }

    function setModeButtons() {
        // helper used by setupMenuControls - left intentionally as a small function to keep code organized
    }

    function setupMenuControls() {
        const safe = (id, fn) => { const el = document.getElementById(id); if (el && fn) el.addEventListener('click', fn); };
        safe('btn-orbit', () => setMode('orbit'));
        safe('btn-pan', () => setMode('pan'));
        safe('btn-select', () => setMode('select'));
        safe('btn-zoom-in', () => zoomIn());
        safe('btn-zoom-out', () => zoomOut());
        safe('btn-reset', () => resetView());
        safe('btn-point-cloud', () => setRenderMode('points'));
        safe('btn-3d-mesh', () => setRenderMode('mesh'));
        safe('btn-downsampled', () => setQualityMode('downsampled'));
        safe('btn-original-quality', () => setQualityMode('original'));
        safe('btn-original-color', () => setColorMode('original'));
        safe('btn-coded-color', () => setColorMode('coded'));
        safe('btn-instancing-on', () => setInstancingMode(true));
        safe('btn-instancing-off', () => setInstancingMode(false));
        safe('btn-optimize-now', () => optimizeInstances());
        const queryInput = document.getElementById('query-input'); const querySendBtn = document.getElementById('query-send-btn'); if (querySendBtn) querySendBtn.addEventListener('click', () => { const qh = queryHandler || app.query; if (qh && qh.handleQuerySend) qh.handleQuerySend(); }); if (queryInput) queryInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') { const qh = queryHandler || app.query; if (qh && qh.handleQuerySend) qh.handleQuerySend(); } });
    }

    function setMode(mode) {
        app.currentMode = mode;
        // Only remove active from icon-btn (tool mode buttons), not control-btn (other control groups)
        document.querySelectorAll('.icon-btn').forEach(btn => btn.classList.remove('active'));
        
        // Hide gizmo when switching away from pan mode
        if (mode !== 'pan' && app.transformControl) {
            app.transformControl.detach();
            app.transformControl.enabled = false;
            app.transformControl.visible = false;
        }
        
        if (mode === 'orbit') {
            document.getElementById('btn-orbit')?.classList.add('active'); app.controls.enableRotate = true; app.controls.enablePan = false; app.controls.enabled = true; app.deselectFile && app.deselectFile(); app.renderer.domElement.style.cursor = 'grab';
        } else if (mode === 'pan') {
            document.getElementById('btn-pan')?.classList.add('active'); app.controls.enableRotate = false; app.controls.enablePan = true; app.controls.enabled = true; app.deselectFile && app.deselectFile(); app.renderer.domElement.style.cursor = 'move';
        } else if (mode === 'select') {
            document.getElementById('btn-select')?.classList.add('active'); app.controls.enableRotate = false; app.controls.enablePan = false; app.controls.enabled = true; app.renderer.domElement.style.cursor = 'crosshair';
        }
    }

    function setColorMode(mode) {
        app.colorMode = mode;
        if (mode === 'original') { document.getElementById('btn-original-color')?.classList.add('active'); document.getElementById('btn-coded-color')?.classList.remove('active'); }
        else { document.getElementById('btn-original-color')?.classList.remove('active'); document.getElementById('btn-coded-color')?.classList.add('active'); }
        app.loadedFiles.forEach((fileData, filename) => app.sceneManager.applyColorMode && app.sceneManager.applyColorMode(fileData.geometry, filename));
        app.loadedFiles.forEach((fd, fn) => app.sceneManager.updateFileRender(fn));
    }

    function setQualityMode(mode) { app.qualityMode = mode; document.getElementById('btn-downsampled')?.classList.toggle('active', mode === 'downsampled'); document.getElementById('btn-original-quality')?.classList.toggle('active', mode === 'original'); app.sceneManager.setQualityMode(mode); }

    function setRenderMode(mode) { app.renderMode = mode; document.getElementById('btn-point-cloud')?.classList.toggle('active', mode === 'points'); document.getElementById('btn-3d-mesh')?.classList.toggle('active', mode === 'mesh'); app.sceneManager.setRenderMode(mode); }
    
    function setInstancingMode(enabled) {
        app.instancingEnabled = enabled;
        document.getElementById('btn-instancing-on')?.classList.toggle('active', enabled);
        document.getElementById('btn-instancing-off')?.classList.toggle('active', !enabled);
        
        if (app.sceneManager && app.sceneManager.toggleInstancing) {
            app.sceneManager.toggleInstancing(enabled);
            updateInstanceStats();
        }
        
        const status = enabled ? 'enabled' : 'disabled';
        console.log(`[Instancing] ${status}`);
    }
    
    function optimizeInstances() {
        if (app.sceneManager && app.sceneManager.optimizeInstances) {
            const count = app.sceneManager.optimizeInstances();
            updateInstanceStats();
            
            if (count > 0) {
                showInlineQueryMessage(`Optimized ${count} objects with instancing`, 'success', 2000);
            } else {
                showInlineQueryMessage('No repeated objects found to optimize', 'info', 2000);
            }
        }
    }
    
    function updateInstanceStats() {
        const statsEl = document.getElementById('instance-stats');
        if (!statsEl) return;
        
        if (app.sceneManager && app.sceneManager.getInstanceStats) {
            const stats = app.sceneManager.getInstanceStats();
            if (stats && stats.totalInstancedObjects > 0) {
                statsEl.innerHTML = `
                    ${stats.instancedMeshCount} instanced meshes<br>
                    ${stats.totalInstancedObjects} objects<br>
                    -${stats.drawCallReduction} draw calls
                `;
                statsEl.style.color = '#4CAF50';
            } else {
                statsEl.innerHTML = 'No instances active';
                statsEl.style.color = '#888';
            }
        }
    }

    function zoomIn() { app.camera.position.multiplyScalar(0.8); app.controls.update(); }
    function zoomOut() { app.camera.position.multiplyScalar(1.2); app.controls.update(); }
    function resetView() { app.camera.position.set(0,0,2); app.controls.target.set(0,0,0); app.controls.update(); }

    function updateInfoIconPosition() {
        if (!app.selectedFile || !app.infoIcon) return;
        const fileData = app.loadedFiles.get(app.selectedFile);
        if (!fileData || !fileData.object || !fileData.visible) { app.infoIcon.style.display='none'; return; }
        
        // Cache bounding box to avoid recomputation every frame
        if (!fileData._cachedBBox) {
            const geometry = fileData.geometry;
            if (!geometry.boundingBox) geometry.computeBoundingBox();
            fileData._cachedBBox = geometry.boundingBox.clone();
        }
        const bbox = fileData._cachedBBox;
        
        const cornerPosition = new THREE.Vector3(bbox.max.x, bbox.max.y, bbox.max.z);
        fileData.object.localToWorld(cornerPosition);
        const screenPosition = cornerPosition.clone().project(app.camera);
        const x = (screenPosition.x * 0.5 + 0.5) * window.innerWidth; const y = (screenPosition.y * -0.5 + 0.5) * window.innerHeight;
        if (screenPosition.z < 1) { app.infoIcon.style.display = 'flex'; app.infoIcon.style.left = `${x + 10}px`; app.infoIcon.style.top = `${y - 10}px`; } else app.infoIcon.style.display = 'none';
    }

    function updateFrameDependentUI() {
        // placeholder for later; currently no extra UI updates
    }

    function createQueryResultsModal() {
        const modal = document.createElement('div'); modal.id='query-results-modal'; Object.assign(modal.style, { position: 'fixed', top: '0', left:'0', width: '100%', height:'100%', backgroundColor:'rgba(0,0,0,0.7)', display:'none', justifyContent:'center', alignItems:'center', zIndex:'10000', backdropFilter:'blur(5px)'});
        const modalContent = document.createElement('div'); Object.assign(modalContent.style,{ backgroundColor:'#2a2a2a', padding:'30px', borderRadius:'12px', maxWidth:'900px', width:'90%', maxHeight:'80vh', overflow:'auto', boxShadow:'0 8px 32px rgba(0,0,0,0.5)', border:'1px solid rgba(255,255,255,0.1)'});
        const closeBtn = document.createElement('span'); closeBtn.innerHTML='&times;'; Object.assign(closeBtn.style, {float:'right', fontSize:'32px', fontWeight:'bold', color:'#aaa', cursor:'pointer', lineHeight:'20px', transition:'color 0.2s'});
        closeBtn.addEventListener('mouseenter',()=> closeBtn.style.color = '#fff'); closeBtn.addEventListener('mouseleave', ()=> closeBtn.style.color = '#aaa'); closeBtn.addEventListener('click', ()=> modal.style.display='none');
        const contentDiv = document.createElement('div'); contentDiv.id = 'query-results-content'; contentDiv.style.color = '#fff'; contentDiv.style.marginTop = '20px';
        modalContent.appendChild(closeBtn); modalContent.appendChild(contentDiv); modal.appendChild(modalContent); modal.addEventListener('click', (e)=> { if (e.target === modal) modal.style.display = 'none';}); document.body.appendChild(modal);
        return modal;
    }

    function displayQueryResults(data) {
        let modal = document.getElementById('query-results-modal'); if (!modal) modal = createQueryResultsModal(); const contentDiv = document.getElementById('query-results-content');
        const source = data.source || 'local'; console.log(`[Query] Displaying results from: ${source}`);
        let html = `...`;
        html = `<div style="display:flex; align-items:center; justify-content:space-between;"><h3 style="color: #4CAF50; margin-top: 0;">Query Results</h3><div class="query-source-badge" style="font-size: 12px; color: #fff; padding: 6px 8px; border-radius: 8px; margin-left: 8px; background: rgba(255, 193, 7, 0.12); border: 1px solid rgba(255,193,7,0.2)">Local Preview</div></div>`;
        html += `<div style="margin-bottom: 15px;"><strong style="color: #2196F3;">Question:</strong><p style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 5px; margin: 5px 0;">${data.question}</p></div>`;
        if (data.results && data.results.length > 0) {
            html += `<div style="margin-bottom: 15px;"><strong style="color: #2196F3;">Results (${data.row_count} rows):</strong><div style="overflow-x: auto; margin-top: 10px;"><table style="width: 100%; border-collapse: collapse; background: rgba(255,255,255,0.05);"><thead><tr style="background: rgba(76, 175, 80, 0.3);">${data.columns.map(col => `<th style="padding: 10px; text-align: left; border: 1px solid rgba(255,255,255,0.1);">${col}</th>`).join('')}</tr></thead><tbody>${data.results.slice(0,100).map(row=>`<tr>${data.columns.map(col=>`<td style="padding: 8px; border: 1px solid rgba(255,255,255,0.1);">${row[col] ?? ''}</td>`).join('')}</tr>`).join('')}</tbody></table>${data.results.length > 100 ? `<p style="margin-top: 10px; color: #ffa500;">Showing first 100 rows of ${data.results.length}</p>`: ''}</div></div>`;
        }
        contentDiv.innerHTML = html; modal.style.display = 'flex';
        if (data.results && data.results.length > 0) {
            const first = data.results[0];
            if (first.center && first.size) {
                const name = first.object || first.filename || Object.keys(app.sceneInfo?.name || {})[0];
                sceneManager.createHighlightBox({ name, filename: first.filename, center: first.center, size: first.size });
            } else if (first.exists !== undefined) {
                if (!first.exists) {
                    sceneManager.clearHighlights();
                    const notFoundDiv = document.createElement('div'); notFoundDiv.style.background='rgba(255,0,0,0.12)'; notFoundDiv.style.borderLeft='4px solid #f44336'; notFoundDiv.style.padding='10px'; notFoundDiv.style.borderRadius='4px'; notFoundDiv.style.marginTop='10px'; notFoundDiv.textContent = `Object '${first.object}' not found in scene (as per info.json).`;
                    contentDiv.appendChild(notFoundDiv);
                } else {
                    if (first.filename) {
                        const f = Array.from(app.loadedFiles.entries()).find(([name, fd]) => name.toLowerCase() === String(first.filename).toLowerCase() || fd.filepath.toLowerCase().endsWith(String(first.filename).toLowerCase()));
                        if (f) {
                            const [name, fd] = f; if (fd.geometry) { fd.geometry.computeBoundingBox(); const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray(); const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray(); sceneManager.createHighlightBox({ name, filename: name, center, size }); }
                            else { const bboxInfo = app.sceneInfo?.bounding_box?.[Object.keys(app.sceneInfo.name).find(k => app.sceneInfo.name[k] === first.filename)]; if (bboxInfo) { const center = [0,0,0]; const size = [bboxInfo.x || 1, bboxInfo.y || 1, bboxInfo.z || 1]; sceneManager.createHighlightBox({ name: first.filename, filename: first.filename, center, size }); } }
                        }
                    }
                }
            }
        }
    }

    function ensureSceneInfoForFile(filename) {
        if (!app.sceneInfo) app.sceneInfo = { name: {}, bounding_box: {}, labels: {}, _map: new Map(), displayNames: new Map(), _autoGenerated: true };
        const fileData = app.loadedFiles.get(filename);
        if (!fileData || !fileData.geometry) return;
        
        // Skip if we already have explicit info from info.json (not auto-generated)
        if (!app.sceneInfo._autoGenerated) return;
        
        const existingEntryKey = Object.keys(app.sceneInfo.name || {}).find(k => String(app.sceneInfo.name[k]).toLowerCase() === filename.toLowerCase());
        if (!existingEntryKey) {
            const basename = filename.replace(/\.ply$/i, '');
            let key = basename;
            let suffix = 1;
            while (app.sceneInfo.name && app.sceneInfo.name[key]) key = `${basename}_${suffix++}`;
            app.sceneInfo.name = app.sceneInfo.name || {};
            app.sceneInfo.name[key] = filename;
            console.log(`[SceneInfo] Auto-generated entry for ${filename} -> ${key}`);
        }
        
        const geometry = fileData.geometry;
        geometry.computeBoundingBox();
        const bbox = geometry.boundingBox;
        const size = new THREE.Vector3();
        bbox.getSize(size);
        const centerLocal = new THREE.Vector3();
        bbox.getCenter(centerLocal);
        let centerWorld = centerLocal.toArray();
        if (fileData.object) {
            const cw = centerLocal.clone();
            fileData.object.localToWorld(cw);
            centerWorld = cw.toArray();
        }
        
        app.sceneInfo.bounding_box = app.sceneInfo.bounding_box || {};
        const keyForFile = Object.keys(app.sceneInfo.name).find(k => app.sceneInfo.name[k] === filename);
        if (!app.sceneInfo.bounding_box[keyForFile]) {
            app.sceneInfo.bounding_box[keyForFile] = { x: size.x, y: size.y, z: size.z, center: centerWorld };
        }
        
        app.sceneInfo.labels = app.sceneInfo.labels || {};
        app.sceneInfo.labels[filename] = app.sceneInfo.labels[filename] || [filename.replace(/\.ply$/i, '')];
        
        // Rebuild maps for all entries to ensure consistency
        rebuildSceneInfoMaps();
    }
    
    function rebuildSceneInfoMaps() {
        // Rebuild the lookup maps from current sceneInfo data
        app.sceneInfo._map = new Map();
        app.sceneInfo.displayNames = new Map();
        
        if (app.sceneInfo.name) {
            for (const key of Object.keys(app.sceneInfo.name)) {
                const val = app.sceneInfo.name[key];
                const filenameLower = String(val).toLowerCase();
                const keyLower = String(key).toLowerCase();
                app.sceneInfo._map.set(keyLower, { key, filename: val });
                app.sceneInfo._map.set(filenameLower, { key, filename: val });
                const basename = filenameLower.replace(/\.ply$/i, '');
                app.sceneInfo._map.set(basename, { key, filename: val });
                const tokens = new Set([...basename.split(/[^a-z0-9]+/), ...keyLower.split(/[^a-z0-9]+/)]);
                for (const t of tokens) {
                    if (t && t.length > 0) app.sceneInfo._map.set(t, { key, filename: val });
                }
            }
        }
        
        if (app.sceneInfo.labels) {
            for (const [fn, labels] of Object.entries(app.sceneInfo.labels)) {
                for (const lab of labels) {
                    const labLower = String(lab).toLowerCase();
                    app.sceneInfo._map.set(labLower, {
                        key: Object.keys(app.sceneInfo.name).find(k => app.sceneInfo.name[k] === fn) || labLower,
                        filename: fn
                    });
                }
                const existing = app.sceneInfo.displayNames.get(fn) || [];
                for (const lab of labels) if (!existing.includes(lab)) existing.push(lab);
                app.sceneInfo.displayNames.set(fn, existing);
            }
        }
    }

    function updateObjectLabelsUI() {
        const container = document.getElementById('object-labels-section'); if (!container) return; const contentDiv = container.querySelector('.section-content'); if (!contentDiv) return; const labels = contentDiv.querySelectorAll('label'); labels.forEach(label => { const filename = label.dataset.filename || label.textContent.trim(); if (filename === app.selectedFile) { label.style.background = 'rgba(76, 175, 80, 0.3)'; label.style.fontWeight = 'bold'; } else { label.style.background = ''; label.style.fontWeight = 'normal'; } }); if (app.selectedFile) console.log('Currently selected:', app.selectedFile);
    }
}
