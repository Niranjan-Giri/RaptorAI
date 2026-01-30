import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { TransformControls } from 'three/examples/jsm/controls/TransformControls.js';
import { LoaderManager } from './loaderManager.js';
import { InstanceManager } from './instanceManager.js';

export function createSceneManager(app, ui) {
    // app is a shared state object

    // Create loaders and scene components
    app.scene = new THREE.Scene();
    app.scene.background = new THREE.Color(0x202020);

    app.camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 1000);
    app.camera.position.set(0, 0, 2);

    app.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, powerPreference: 'high-performance' });
    app.renderer.outputColorSpace = THREE.SRGBColorSpace;
    app.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    app.renderer.toneMappingExposure = 1.0;
    app.renderer.shadowMap.enabled = true;                    
    app.renderer.shadowMap.type = THREE.PCFSoftShadowMap;   
    app.renderer.setPixelRatio(window.devicePixelRatio);
    app.renderer.setSize(window.innerWidth, window.innerHeight);
    const canvasContainer = document.getElementById('canvas-container');
    if (canvasContainer) {
      canvasContainer.appendChild(app.renderer.domElement);
    }

    app.controls = new OrbitControls(app.camera, app.renderer.domElement);

    /*This is the main transform control logic.... we can modify stuffs below*/

    // Initialize TransformControls
    app.transformControl = new TransformControls(app.camera, app.renderer.domElement);
    
    app.transformControl.setMode('translate'); // Default to translate mode
    app.transformControl.addEventListener('dragging-changed', function (event) {
        // Disable orbit controls while dragging the gizmo
        app.controls.enabled = !event.value;
        
        // Set transform flag when dragging starts/stops
        app._isTransforming = event.value;
        
        // Invalidate bbox cache when object is transformed
        if (!event.value && app.selectedFile) {
            const fileData = app.loadedFiles.get(app.selectedFile);
            if (fileData) fileData._cachedBBox = null;
        }
    });
    app.transformControl.addEventListener('objectChange', function () {
        // Object is being transformed - this fires continuously during drag
        // The highlight box will be updated in the animate loop
    });
    app.transformControl.setSize(0.5);
    //Note: we can change this to world for global gizmo
    app.transformControl.setSpace('local'); 
    
    app.scene.add(app.transformControl.getHelper());
    /************************************************************* */

    app.raycaster = new THREE.Raycaster();
    app.raycaster.params.Points.threshold = 0.05; // Increase threshold for easier point cloud selection
    app.mouse = new THREE.Vector2();

    window.addEventListener('resize', onWindowResize);
    
    // Track mouse down position to detect actual clicks vs drags
    let mouseDownPos = null;
    app.renderer.domElement.addEventListener('mousedown', (e) => {
        mouseDownPos = { x: e.clientX, y: e.clientY };
    });
    app.renderer.domElement.addEventListener('mouseup', (e) => {
        if (mouseDownPos) {
            const dx = e.clientX - mouseDownPos.x;
            const dy = e.clientY - mouseDownPos.y;
            const distance = Math.sqrt(dx * dx + dy * dy);
            // Only treat as click if mouse moved less than 5 pixels
            if (distance < 5) {
                onCanvasClick(e);
            }
        }
        mouseDownPos = null;
    });
    app.renderer.domElement.addEventListener('mousemove', () => { if (ui) ui.updateInfoIconPosition(); });
    window.addEventListener('keydown', onKeyDown);

    // Lights will be added only when mesh render mode is used to save performance
    app.ambientLight = null;
    app.directionalLight = null;

    // Create the LoaderManager and wire callbacks
    app.loaderManager = new LoaderManager(handleFileLoaded, handleFileProgress, handleFileError);
    
    // Create InstanceManager for optimizing repeated objects
    app.instanceManager = new InstanceManager(app.scene);

    return {
        init: () => {
            animate();
        },
        loadAllPLYFiles: loadAllPLYFiles,
        updateFileRender: updateFileRender,
        toggleFileVisibility: toggleFileVisibility,
        deselectFile: deselectFile,
        animateCameraTo: animateCameraTo,
        frameAllObjects: frameAllObjects,
        createHighlightBox: createHighlightBox,
        clearHighlights: clearHighlights,
        setUI: (newUI) => { ui = newUI; },
        ensureGeometryHasNormals: ensureGeometryHasNormals,
        applyColorMode: applyColorMode,
        setQualityMode: setQualityMode,
        setRenderMode: setRenderMode,
        onCanvasClick: onCanvasClick,
        optimizeInstances: optimizeInstances,
        toggleInstancing: toggleInstancing,
        getInstanceStats: getInstanceStats
    };

    // ----------------- Implementation ------------------
    
    /**
     * Calculate optimal point size for consistent visual density across all screens
     * Simplified formula for better consistency
     */
    function calculatePointSize() {
        // Use dynamic base size from app state (default 0.015)
        //Taken from ui.js 
        const baseSize = app.pointBaseSize || 0.015;
        
        // DPI compensation - directly scale with device pixel ratio
        // High DPI screens need proportionally larger points
        const dpiCompensation = window.devicePixelRatio;
        
        // Viewport scaling - normalize based on standard 1080p height
        // This ensures points look similar on different screen sizes
        const viewportScale = window.innerHeight / 1080;
        
        // Final size calculation: base * DPI * viewport scale
        // This provides consistent point density across different displays
        return baseSize * dpiCompensation * viewportScale;
    }
    function onWindowResize() {
        app.camera.aspect = window.innerWidth / window.innerHeight;
        app.camera.updateProjectionMatrix();
        app.renderer.setPixelRatio(window.devicePixelRatio);
        app.renderer.setSize(window.innerWidth, window.innerHeight);

        // Update point sizes for all loaded files to maintain consistent density across screens
        if (app.renderMode === 'points') {
            const optimalSize = calculatePointSize();
            app.loadedFiles.forEach((fileData) => {
                if (fileData.object && fileData.object.isPoints) {
                    fileData.object.material.size = optimalSize;
                    fileData.object.material.needsUpdate = true;
                }
            });
        }
    }

    function onKeyDown(event) {
        if (!app.selectedFile) return;
        switch (event.key.toLowerCase()) {
            case 'g': // Translate
            case 't':
                app.transformControl.setMode('translate');
                break;
            case 'r':
                app.transformControl.setMode('rotate');
                break;
            case 's':
                app.transformControl.setMode('scale');
                break;
            case 'escape':
                deselectFile();
                break;
        }
    }

    function animate() {
        requestAnimationFrame(animate);
        app.controls.update();
        app.renderer.render(app.scene, app.camera);

        // Throttle info icon updates - only update every 2 frames (30fps max)
        if (!app._frameCounter) app._frameCounter = 0;
        app._frameCounter++;
        
        // Update both Info Icon AND Highlight Box position if an object is selected
        if (app.selectedFile) {
            const fileData = app.loadedFiles.get(app.selectedFile);
            
            // Sync Highlight Box position with Object position
            if (fileData && fileData.object && app.highlightBoxes) {
                // Get the main highlight box mesh
                const boxMesh = app.highlightBoxes.get(app.selectedFile) || app.highlightBoxes.get(app.selectedFile + ':outline');
                
                if (boxMesh) {
                    // Recompute world center of the object
                    if (!fileData.geometry.boundingBox) fileData.geometry.computeBoundingBox();
                    
                    // We need to apply the object's current World Matrix to the center of its local bounding box
                    // to find where the box should be in World Space.
                    const localCenter = new THREE.Vector3();
                    fileData.geometry.boundingBox.getCenter(localCenter);
                    
                    // Transform local center to world space
                    const worldCenter = localCenter.clone().applyMatrix4(fileData.object.matrixWorld);
                    
                    // Apply rotation and scale if needed, but for an AABB highlight we usually just follow position
                    // Ideally, the highlight box should match the object's transform.
                    // Simple approach: Set box position to object's world center
                    
                    // Note: This assumes the HighlightBox was created axis-aligned in World Space. 
                    // If we rotate the object, the AABB naturally changes, but our BoxGeometry is static.
                    // For a true OBB (Oriented Bounding Box), we should copy position/quaternion.
                    // For now, let's copy position to strictly follow the drag.
                    
                    const currentBox = app.highlightBoxes.get(app.selectedFile);
                    const currentOutline = app.highlightBoxes.get(app.selectedFile + ':outline');

                    if (currentBox) currentBox.position.copy(worldCenter);
                    if (currentOutline) currentOutline.position.copy(worldCenter);
                }
            }

            if (app._frameCounter % 2 === 0 && ui) {
                ui.updateInfoIconPosition();
            }
        }
        
        // Update highlight label positions/frame dependent UI
        if (ui && ui.updateFrameDependentUI) ui.updateFrameDependentUI();
    }

    function loadAllPLYFiles() {
        // starts loading files and initial UI updates
        // Clear duplicates map for this load session to handle unrelated files with same name
        const usedNames = new Set();
        
        app.plyFiles.forEach((filepath, index) => {
            let filename = filepath.split('/').pop();
            
            // Use friendly name if available (passed from Viewer/index.jsx)
            if (app.plyFileNames && app.plyFileNames[index]) {
                filename = app.plyFileNames[index];
            }
            
            // Ensure unique filenames in the map
            let uniqueName = filename;
            let counter = 1;
            while (usedNames.has(uniqueName) || app.loadedFiles.has(uniqueName)) {
                uniqueName = `${filename} (${counter++})`;
            }
            filename = uniqueName;
            usedNames.add(filename);

            app.loadedFiles.set(filename, {
                geometry: null,
                object: null,
                visible: true,
                originalColors: null,
                codedColors: null,
                filepath: filepath,
                isPreview: true,
                loading: true
            });
            app.loaderManager.loadPLY(filepath, filename);
        });
        if (ui) ui.createFileCheckboxes();
    }

    function setQualityMode(mode) {
        app.qualityMode = mode;
        app.loaderManager.setQualityMode(mode);
        app.loaderManager.cancelAll();

        const currentFiles = new Map();
        app.loadedFiles.forEach((fileData, filename) => {
            currentFiles.set(filename, {
                object: fileData.object,
                visible: fileData.visible,
                filepath: fileData.filepath
            });
        });

        app.loadedFiles.forEach((fileData, filename) => {
            fileData.loading = true;
            fileData.isPreview = true;
            fileData.loadingMessage = `Switching to ${mode} mode...`;
            fileData.loadingProgress = 0;
        });
        if (ui) ui.createFileCheckboxes();
        
        // Iterate through loadedFiles instead of plyFiles array to ensure we use the correct keys (filenames)
        // that were established in loadAllPLYFiles (checking for friendly names/deduplication)
        app.loadedFiles.forEach((fileData, filename) => {
            if (fileData.filepath) {
                app.loaderManager.loadPLY(fileData.filepath, filename);
            }
        });
    }

    function setRenderMode(mode) {
        app.renderMode = mode;
        app.loadedFiles.forEach((fileData, filename) => updateFileRender(filename));
    }

    function ensureGeometryHasNormals(geometry) {
        if (!geometry) return;
        geometry.computeVertexNormals();
    }

    function createDefaultColors(pointCount) {
        const colors = new Float32Array(pointCount * 3);
        for (let i = 0; i < pointCount * 3; i++) {
            colors[i] = 1.0; // white
        }
        return colors;
    }

    function createCodedColors(geometry) {
        geometry.computeBoundingBox();
        const positions = geometry.attributes.position;
        const colors = [];
        let minDist = Infinity;
        let maxDist = -Infinity;
        for (let i = 0; i < positions.count; i++) {
            const x = positions.getX(i);
            const y = positions.getY(i);
            const z = positions.getZ(i);
            const dist = Math.sqrt(x * x + y * y + z * z);
            minDist = Math.min(minDist, dist);
            maxDist = Math.max(maxDist, dist);
        }
        for (let i = 0; i < positions.count; i++) {
            const x = positions.getX(i);
            const y = positions.getY(i);
            const z = positions.getZ(i);
            const dist = Math.sqrt(x * x + y * y + z * z);
            const normalizedDist = (dist - minDist) / (maxDist - minDist);
            let r, g, b;
            if (normalizedDist < 0.5) {
                const t = normalizedDist * 2;
                r = 1 - t; g = 1; b = 0;
            } else {
                const t = (normalizedDist - 0.5) * 2;
                r = t; g = 1 - t; b = 0;
            }
            colors.push(r, g, b);
        }
        return new Float32Array(colors);
    }

    function applyColorMode(geometry, filename) {
        const fileData = app.loadedFiles.get(filename);
        if (!fileData) return;
        const colorsToUse = app.colorMode === 'original' ? fileData.originalColors : fileData.codedColors;
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(colorsToUse, 3));
    }

    function updateFileRender(filename) {
        const fileData = app.loadedFiles.get(filename);
        if (!fileData) return;

        // Store transform from current object if exists
        let oldPosition, oldRotation, oldScale;
        if (fileData.object) {
            oldPosition = fileData.object.position.clone();
            oldRotation = fileData.object.rotation.clone();
            oldScale = fileData.object.scale.clone();
            app.scene.remove(fileData.object);
        }

        if (!fileData.visible) return;

        if (app.renderMode === 'points') {
            const optimalSize = calculatePointSize();
            const material = new THREE.PointsMaterial({ size: optimalSize, vertexColors: true, color: 0xffffff });
            fileData.object = new THREE.Points(fileData.geometry, material);
            fileData.object.castShadow = true;
            fileData.object.receiveShadow = true;
        } else {
            const material = new THREE.MeshStandardMaterial({ vertexColors: true, flatShading: false, side: THREE.DoubleSide, roughness: 0.7, metalness: 0.0, envMapIntensity: 1.0 });
            fileData.object = new THREE.Mesh(fileData.geometry, material);
            fileData.object.castShadow = true;
            fileData.object.receiveShadow = true;
            if (!app.ambientLight) {
                app.ambientLight = new THREE.AmbientLight(0xffffff, 0.5);
                app.scene.add(app.ambientLight);
                app.directionalLight = new THREE.DirectionalLight(0xffffff, 0.8);
                app.directionalLight.position.set(5, 5, 5);
                app.directionalLight.castShadow = true;
                app.directionalLight.shadow.mapSize.width = 2048;
                app.directionalLight.shadow.mapSize.height = 2048;
                app.scene.add(app.directionalLight);
                const fillLight = new THREE.DirectionalLight(0xffffff, 0.3);
                fillLight.position.set(-5, 0, -5);
                app.scene.add(fillLight);
            }
        }

        // Restore transform from old object
        if (oldPosition) fileData.object.position.copy(oldPosition);
        if (oldRotation) fileData.object.rotation.copy(oldRotation);
        if (oldScale) fileData.object.scale.copy(oldScale);

        app.scene.add(fileData.object);
    }

    function updateGeometryInPlace(targetGeometry, sourceGeometry) {
        if (sourceGeometry.attributes.position) targetGeometry.setAttribute('position', sourceGeometry.attributes.position);
        if (sourceGeometry.attributes.color) targetGeometry.setAttribute('color', sourceGeometry.attributes.color);
        if (sourceGeometry.attributes.normal) targetGeometry.setAttribute('normal', sourceGeometry.attributes.normal);
        targetGeometry.computeBoundingBox();
        targetGeometry.attributes.position.needsUpdate = true;
        if (targetGeometry.attributes.color) targetGeometry.attributes.color.needsUpdate = true;
        if (targetGeometry.attributes.normal) targetGeometry.attributes.normal.needsUpdate = true;
    }

    function handleFileLoaded(filename, geometry, metadata) {
        const { isPreview, totalExpectedPoints, wasDownsampled, isIdleUpdate, isIncremental } = metadata;
        const fileData = app.loadedFiles.get(filename);
        if (!fileData) return;
        ensureGeometryHasNormals(geometry);
        if (!geometry.attributes.color) geometry.setAttribute('color', new THREE.Float32BufferAttribute(createDefaultColors(geometry.attributes.position.count), 3));
        
        // Handle incremental updates more efficiently
        const isIncrementalUpdate = isPreview && fileData.geometry && fileData.object;
        if (isIncrementalUpdate && (isIdleUpdate || isIncremental)) {
            updateGeometryInPlace(fileData.geometry, geometry);
            fileData.isPreview = isPreview;
            fileData.loading = isPreview;
            fileData.wasDownsampled = wasDownsampled;
            if (ui) ui.createFileCheckboxes();
            if (ui) ui.ensureSceneInfoForFile(filename);
        } else {
            const originalColors = geometry.attributes.color.array.slice();
            const codedColors = createCodedColors(geometry);
            const oldObject = fileData.object;
            app.loadedFiles.set(filename, { ...fileData, geometry, originalColors, codedColors, isPreview, loading: isPreview, wasDownsampled });
            applyColorMode(geometry, filename);
            updateFileRender(filename);
            if (oldObject && oldObject !== app.loadedFiles.get(filename).object) {
                app.scene.remove(oldObject);
                if (oldObject.geometry) oldObject.geometry.dispose();
                if (oldObject.material) oldObject.material.dispose();
            }
            if (ui) ui.createFileCheckboxes();
            if (ui) ui.updateObjectLabelsUI();
            const pointCount = geometry.attributes.position.count.toLocaleString();
            if (!isPreview && app.selectedFile === filename && app.currentMode === 'pan') {
                const upgradedData = app.loadedFiles.get(filename);
                if (upgradedData && upgradedData.object) {
                    //Remove this line if we want to make a replaceable object
                    app.transformControl.detach();
                    //--------------------------------------------
                    app.transformControl.attach(upgradedData.object);
                    app.transformControl.enabled = true;
                    app.transformControl.visible = true;
                    if (ui && ui.updateInfoIconPosition) ui.updateInfoIconPosition();
                }
            }
            if (ui) ui.ensureSceneInfoForFile(filename);
            
            // If this is final load (not preview), optimize instances
            if (!isPreview) {
                // Defer instance optimization slightly to avoid blocking
                setTimeout(() => {
                    const optimizedCount = optimizeInstances();
                    if (optimizedCount > 0) {
                        const stats = getInstanceStats();
                    }
                }, 100);
            }
        }
    }

    function handleFileProgress(filename, message, progress) {
        const fileData = app.loadedFiles.get(filename);
        if (fileData) {
            fileData.loadingMessage = message;
            fileData.loadingProgress = progress;
            if (ui) ui.createFileCheckboxes();
        }
    }

    function handleFileError(filename, error) {
        const fileData = app.loadedFiles.get(filename);
        if (fileData) {
            fileData.loading = false;
            fileData.error = error;
            if (ui) ui.createFileCheckboxes();
        }
    }

    function onCanvasClick(event) {
        // Don't select if we're actively dragging the gizmo
        if (app._isTransforming) {
            return;
        }

        app.mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
        app.mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;
        app.raycaster.setFromCamera(app.mouse, app.camera);
        
        const objectsToCheck = [];
        app.loadedFiles.forEach((fileData) => {
            if (fileData.object && fileData.visible) objectsToCheck.push(fileData.object);
        });
        
        // Use recursive=true if objects might be groups, but for PLY meshes usually false is fine. 
        // Using false for performance unless structure changes.
        const intersects = app.raycaster.intersectObjects(objectsToCheck, false);
        
        if (intersects.length > 0) {
            const clickedObject = intersects[0].object;
            if (app.selectedFile) {
                const prevData = app.loadedFiles.get(app.selectedFile);
                if (prevData && prevData.object && app.renderMode === 'points') {
                    prevData.object.material.vertexColors = true;
                    prevData.object.material.color.set(0xffffff);
                    prevData.object.material.needsUpdate = true;
                }
            }
            for (const [filename, fileData] of app.loadedFiles.entries()) {
                if (fileData.object === clickedObject) {
                    app.selectedFile = filename;
                    
                    if (app.renderMode === 'points') {
                        clickedObject.material.vertexColors = false;
                        clickedObject.material.color.set(0xffffff);
                        clickedObject.material.needsUpdate = true;
                    }
                    
                    // Attach transform controls only in Pan Mode
                    app.transformControl.detach(); // Detach first to ensure clean state
                    
                    if (app.currentMode === 'pan') {
                        // Make sure object has proper matrix
                        clickedObject.updateMatrixWorld(true);
                        
                        app.transformControl.attach(clickedObject);
                        app.transformControl.enabled = true;
                        app.transformControl.visible = true;
                        
                        // Switch to translate mode by default for easier moving
                        app.transformControl.setMode('translate');
                    }
                    
                    if (ui) ui.updateObjectLabelsUI();
                    const fd = app.loadedFiles.get(filename);
                    if (fd && fd.geometry) {
                        if (!fd.geometry.boundingBox) fd.geometry.computeBoundingBox();
                        const center = fd.geometry.boundingBox.getCenter(new THREE.Vector3()).toArray();
                        const size = fd.geometry.boundingBox.getSize(new THREE.Vector3()).toArray();
                        createHighlightBox({ name: filename, filename: filename, center, size });
                    }
                    break;
                }
            }
        } else {
            // Only deselect if we didn't click on the transform controls (difficult to check directly here, 
            // but usually transform controls consume the event if interacted with).
            // However, if we click empty space, we should deselect.
            deselectFile();
        }
    }

    function toggleFileVisibility(filename, visible) {
        const fileData = app.loadedFiles.get(filename);
        if (!fileData) return;
        fileData.visible = visible;
        if (visible) updateFileRender(filename);
        else {
            if (fileData.object) app.scene.remove(fileData.object);
            if (app.selectedFile === filename) deselectFile();
            clearHighlights();
        }
    }

    function deselectFile() {
        if (app.selectedFile) {
            const prevData = app.loadedFiles.get(app.selectedFile);
            if (prevData && prevData.object && app.renderMode === 'points') {
                prevData.object.material.vertexColors = true;
                prevData.object.material.color.set(0xffffff);
                prevData.object.material.needsUpdate = true;
            }
        }
        app.selectedFile = null;
        if (app.transformControl) {
            app.transformControl.detach();
            app.transformControl.enabled = false;
            app.transformControl.visible = false;
        }
        if (app.infoIcon) app.infoIcon.style.display = 'none';
        clearHighlights();
        if (ui) ui.updateObjectLabelsUI();
    }

    function clearHighlights() {
        if (!app.highlightBoxes) app.highlightBoxes = new Map();
        app.highlightBoxes.forEach((mesh, name) => {
            app.scene.remove(mesh);
            if (mesh.geometry) mesh.geometry.dispose();
            if (mesh.material) mesh.material.dispose();
        });
        app.highlightBoxes.clear();
    }

    function createHighlightBox({ name, filename, center = [0,0,0], size = [1,1,1] }) {
        clearHighlights();
        const boxSize = new THREE.Vector3(size[0] || 1, size[1] || 1, size[2] || 1);
        const geometry = new THREE.BoxGeometry(boxSize.x, boxSize.y, boxSize.z);
        const material = new THREE.MeshBasicMaterial({ color: 0x00ff00, transparent: true, opacity: 0.12, depthTest: false });
        const boxMesh = new THREE.Mesh(geometry, material);
        const centerVec = new THREE.Vector3(center[0] || 0, center[1] || 0, center[2] || 0);
        if (filename) {
            const fileEntry = Array.from(app.loadedFiles.values()).find(f => f.filepath && (f.filepath.endsWith(filename) || f.filepath.includes(filename)));
            if (fileEntry && fileEntry.object) {
                fileEntry.geometry.computeBoundingBox();
                const localCenter = new THREE.Vector3();
                fileEntry.geometry.boundingBox.getCenter(localCenter);
                fileEntry.object.localToWorld(localCenter);
                boxMesh.position.copy(localCenter);
            } else {
                boxMesh.position.copy(centerVec);
            }
        } else {
            boxMesh.position.copy(centerVec);
        }
        const edges = new THREE.EdgesGeometry(geometry);
        const line = new THREE.LineSegments(edges, new THREE.LineBasicMaterial({ color: 0x00ff00, linewidth: 2 }));
        line.position.copy(boxMesh.position);
        app.scene.add(boxMesh);
        app.scene.add(line);
        if (!app.highlightBoxes) app.highlightBoxes = new Map();
        const key = name || filename || 'highlight';
        app.highlightBoxes.set(key, boxMesh);
        app.highlightBoxes.set(key + ':outline', line);
        animateCameraTo(boxMesh.position, { size: boxSize }, 700);
    }

    function animateCameraTo(targetCenter, options = {}, duration = 700) {
        if (!app.camera) return;
        if (app.cameraAnim) cancelAnimationFrame(app.cameraAnim.raf);
        const startPos = app.camera.position.clone();
        const startTarget = app.controls.target.clone();
        const endTarget = targetCenter.clone();
        const size = options.size || new THREE.Vector3(1,1,1);
        const maxSize = Math.max(size.x, size.y, size.z);
        const fov = (app.camera.fov * Math.PI) / 180.0;
        const distance = Math.max(1.0, maxSize * 1.8 / Math.tan(fov / 2));
        const dir = app.camera.position.clone().sub(app.controls.target).normalize();
        const endPos = endTarget.clone().add(dir.multiplyScalar(distance));
        const startTime = performance.now();
        function tick(now) {
            const t = Math.min(1, (now - startTime) / duration);
            const s = t * t * (3 - 2 * t);
            app.camera.position.lerpVectors(startPos, endPos, s);
            app.controls.target.lerpVectors(startTarget, endTarget, s);
            app.controls.update();
            if (t < 1) app.cameraAnim.raf = requestAnimationFrame(tick);
            else app.cameraAnim = null;
        }
        app.cameraAnim = { raf: requestAnimationFrame(tick) };
    }

    /**
     * Frame all visible objects in the scene
     */
    function frameAllObjects(duration = 700) {
        const visibleObjects = [];
        app.loadedFiles.forEach((fileData) => {
            if (fileData.object && fileData.visible) {
                visibleObjects.push(fileData.object);
            }
        });

        if (visibleObjects.length === 0) return;

        // Calculate combined bounding box
        const boundingBox = new THREE.Box3();
        visibleObjects.forEach(obj => {
            obj.geometry.computeBoundingBox();
            boundingBox.expandByObject(obj);
        });

        const center = boundingBox.getCenter(new THREE.Vector3());
        const size = boundingBox.getSize(new THREE.Vector3());
        const maxSize = Math.max(size.x, size.y, size.z);

        // Animate camera to frame the objects
        const fov = (app.camera.fov * Math.PI) / 180.0;
        const distance = Math.max(1.0, maxSize * 1.8 / Math.tan(fov / 2));
        const dir = app.camera.position.clone().sub(app.controls.target).normalize();
        const endPos = center.clone().add(dir.multiplyScalar(distance));

        if (app.cameraAnim) cancelAnimationFrame(app.cameraAnim.raf);
        const startPos = app.camera.position.clone();
        const startTarget = app.controls.target.clone();
        const startTime = performance.now();

        function tick(now) {
            const t = Math.min(1, (now - startTime) / duration);
            const s = t * t * (3 - 2 * t);
            app.camera.position.lerpVectors(startPos, endPos, s);
            app.controls.target.lerpVectors(startTarget, center, s);
            app.controls.update();
            if (t < 1) app.cameraAnim.raf = requestAnimationFrame(tick);
            else app.cameraAnim = null;
        }
        app.cameraAnim = { raf: requestAnimationFrame(tick) };
    }
    
    function optimizeInstances() {
        if (!app.instanceManager) return 0;
        const count = app.instanceManager.optimizeScene(app.loadedFiles);
        return count;
    }
    
    function toggleInstancing(enabled) {
        if (!app.instanceManager) return;
        app.instanceManager.setEnabled(enabled);
        if (enabled) {
            optimizeInstances();
        }
    }
    
    function getInstanceStats() {
        if (!app.instanceManager) return null;
        return app.instanceManager.getStats();
    }
}
