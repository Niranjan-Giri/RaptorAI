import * as THREE from 'three';

// Downsampling configuration
const TARGET_POINTS = 3000000; 
const DOWNSAMPLE_THRESHOLD = 4000000;
const USE_RANDOM_SELECTION = false;
const CHUNK_SIZE = 500000;

const GLOBAL_MIN_BOUNDS = {
    x: -1000.0,
    y: -1000.0,
    z: -1000.0
};

// WASM Module state
let wasmModule = null;
let wasmInitialized = false;
let wasmInitPromise = null;
let createPlyModule = null;

/**
 * Initialize WASM module
 */
async function initWasm() {
    if (wasmInitialized) return wasmModule;
    if (wasmInitPromise) return wasmInitPromise;
    
    wasmInitPromise = (async () => {
        try {
            console.log('[WASM] Starting initialization...');
            
            // Try multiple loading strategies
            let loadSuccess = false;
            
            // Strategy 1: Try dynamic import from bin folder
            try {
                console.log('[WASM] Attempting to load from bin folder...');
                const plyModuleImport = await import('../../../bin/PLY.js');
                createPlyModule = plyModuleImport.default || plyModuleImport;
                console.log('[WASM] PLY.js loaded successfully from bin folder');
                
                // Get the WASM file URL
                const wasmUrl = new URL('../../../bin/PLY.wasm', import.meta.url);
                console.log('[WASM] WASM file URL:', wasmUrl.href);
                
                wasmModule = await createPlyModule({
                    locateFile: (path) => {
                        if (path.endsWith('.wasm')) {
                            return wasmUrl.href;
                        }
                        return path;
                    }
                });
                
                loadSuccess = true;
            } catch (binError) {
                console.warn('[WASM] Failed to load from bin folder:', binError.message);
                
                // Strategy 2: Try loading from public folder via script tag
                try {
                    console.log('[WASM] Attempting to load from public folder...');
                    
                    // Load the Emscripten module via script
                    const scriptUrl = '/bin/PLY.js';
                    const scriptResponse = await fetch(scriptUrl);
                    if (!scriptResponse.ok) throw new Error(`Failed to fetch ${scriptUrl}`);
                    
                    const scriptText = await scriptResponse.text();
                    
                    // Execute the script in a function scope to capture the module
                    const moduleFunc = new Function(scriptText + '; return createPlyModule;');
                    createPlyModule = moduleFunc();
                    
                    wasmModule = await createPlyModule({
                        locateFile: (path) => {
                            if (path.endsWith('.wasm')) {
                                return '/bin/PLY.wasm';
                            }
                            return path;
                        }
                    });
                    
                    loadSuccess = true;
                    console.log('[WASM] Loaded successfully from public folder');
                } catch (publicError) {
                    console.warn('[WASM] Failed to load from public folder:', publicError.message);
                }
            }
            
            if (!loadSuccess) {
                throw new Error('All WASM loading strategies failed');
            }
            
            wasmInitialized = true;
            console.log('[WASM] Module initialized successfully');
            return wasmModule;
        } catch (error) {
            console.error('[WASM] Failed to initialize WASM module:', error);
            console.error('[WASM] Stack:', error.stack);
            wasmInitPromise = null;
            throw error;
        }
    })();
    
    return wasmInitPromise;
}

/**
 * Calculate optimal grid size based on point cloud bounds
 * Returns a suggested GRID_SIZE value for target point density
 * 
 * @param {THREE.BufferGeometry} geometry - The geometry to analyze
 * @param {number} targetPoints - Desired number of points after downsampling
 * @returns {number} Suggested grid size
 */
function calculateOptimalGridSize(geometry, targetPoints = TARGET_POINTS) {
    if (!geometry.boundingBox) {
        geometry.computeBoundingBox();
    }
    const bbox = geometry.boundingBox;
    const size = new THREE.Vector3();
    bbox.getSize(size);

    // Handle flat or thin geometries by ensuring non-zero dimensions
    const dx = Math.max(size.x, 0.001);
    const dy = Math.max(size.y, 0.001);
    const dz = Math.max(size.z, 0.001);
    
    const volume = dx * dy * dz;
    
    // Assuming uniform distribution, voxel size = (volume / target points) ^ (1/3)
    const gridSize = Math.pow(volume / targetPoints, 1/3);
    return Math.max(0.001, gridSize); // Minimum 1mm grid size
}

/**
 * Parse PLY file incrementally and send chunks back to main thread
 */
async function loadAndProcessPLY(url, filename, centerOffset, qualityMode = 'downsampled') {
    const startTime = performance.now();
    let useWasm = false;
    
    try {
        // Try to initialize WASM module first
        try {
            await initWasm();
            useWasm = true;
            console.log(`[WASM] Using WASM parser for ${filename}`);
        } catch (wasmError) {
            console.warn('[WASM] WASM initialization failed, falling back to JS parser:', wasmError.message);
            useWasm = false;
        }
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
        }

        const arrayBuffer = await response.arrayBuffer();
        const fetchTime = performance.now() - startTime;
        console.log(`[${useWasm ? 'WASM' : 'JS'}] Fetched ${filename} in ${fetchTime.toFixed(2)}ms`);
        
        // Parse PLY - try WASM first, fallback to JS
        let geometry;
        const parseStartTime = performance.now();
        
        if (useWasm) {
            try {
                geometry = await parsePLYWithWasm(arrayBuffer, filename);
                const parseTime = performance.now() - parseStartTime;
                console.log(`[WASM] Parsed ${geometry.attributes.position.count.toLocaleString()} points in ${parseTime.toFixed(2)}ms`);
            } catch (wasmParseError) {
                console.warn('[WASM] WASM parsing failed, falling back to JS parser:', wasmParseError.message);
                geometry = parsePLY(arrayBuffer);
                const parseTime = performance.now() - parseStartTime;
                console.log(`[JS] Parsed ${geometry.attributes.position.count.toLocaleString()} points in ${parseTime.toFixed(2)}ms`);
            }
        } else {
            geometry = parsePLY(arrayBuffer);
            const parseTime = performance.now() - parseStartTime;
            console.log(`[JS] Parsed ${geometry.attributes.position.count.toLocaleString()} points in ${parseTime.toFixed(2)}ms`);
        }

        if (!geometry.attributes.position) {
            throw new Error('Missing position attribute');
        }

        // Send initial metadata
        postMessage({
            type: 'metadata',
            filename,
            totalPoints: geometry.attributes.position.count
        });

        // Ensure normals and colors exist
        ensureGeometryHasNormals(geometry);
        if (!geometry.attributes.color) {
            const defaultColors = createDefaultColors(geometry.attributes.position.count);
            geometry.setAttribute('color', new THREE.Float32BufferAttribute(defaultColors, 3));
        }

        // Decide whether to downsample based on quality mode
        const needsDownsampling = qualityMode === 'downsampled' && geometry.attributes.position.count > DOWNSAMPLE_THRESHOLD;

        if (needsDownsampling) {
            // Calculate optimal grid size for this specific geometry
            const gridSize = calculateOptimalGridSize(geometry, TARGET_POINTS);
            
            const downsampleStartTime = performance.now();
            // Downsample and send in chunks
            const downsampled = downsampleGeometryStreaming(geometry, filename, gridSize);
            const downsampleTime = performance.now() - downsampleStartTime;
            console.log(`[${useWasm ? 'WASM' : 'JS'}] Downsampled to ${downsampled.attributes.position.count.toLocaleString()} points in ${downsampleTime.toFixed(2)}ms`);
            
            sendGeometryInChunks(downsampled, filename, true);
        } else {
            // Send as-is in chunks
            sendGeometryInChunks(geometry, filename, false);
        }

        const totalTime = performance.now() - startTime;
        console.log(`[${useWasm ? 'WASM' : 'JS'}] Total processing time for ${filename}: ${totalTime.toFixed(2)}ms`);

        // Send completion message
        postMessage({
            type: 'complete',
            filename
        });

    } catch (error) {
        console.error('[PLY Loader] Error loading PLY:', error);
        console.error('[PLY Loader] Stack trace:', error.stack);
        postMessage({
            type: 'error',
            filename,
            error: error.message
        });
    }
}

/**
 * Parse PLY using WASM module for faster processing
 */
async function parsePLYWithWasm(arrayBuffer, filename) {
    const wasm = wasmModule;
    
    try {
        postMessage({
            type: 'progress',
            filename,
            message: 'Parsing with WASM...',
            progress: 10
        });
        
        // Allocate memory in WASM heap
        const byteLength = arrayBuffer.byteLength;
        const dataPtr = wasm._wasm_alloc(byteLength);
        
        if (!dataPtr) {
            throw new Error('Failed to allocate WASM memory');
        }
        
        // Copy data to WASM heap
        const heapBytes = new Uint8Array(wasm.HEAPU8.buffer, dataPtr, byteLength);
        heapBytes.set(new Uint8Array(arrayBuffer));
        
        // Process PLY buffer
        const result = wasm._process_ply_buffer(dataPtr, byteLength);
        
        if (result !== 0) {
            wasm._wasm_free(dataPtr);
            throw new Error(`WASM PLY parsing failed with code: ${result}`);
        }
        
        postMessage({
            type: 'progress',
            filename,
            message: 'Extracting geometry data...',
            progress: 30
        });
        
        // Get vertex count
        const vertexCount = wasm._get_vertex_count();
        
        if (vertexCount === 0) {
            wasm._wasm_free(dataPtr);
            throw new Error('No vertices found in PLY file');
        }
        
        // Get pointers to data arrays
        const positionsPtr = wasm._get_positions();
        const colorsPtr = wasm._get_colors();
        const normalsPtr = wasm._get_normals();
        
        // Copy data from WASM heap to JavaScript arrays
        const positions = new Float32Array(wasm.HEAPF32.buffer, positionsPtr, vertexCount * 3);
        const colors = new Float32Array(wasm.HEAPF32.buffer, colorsPtr, vertexCount * 3);
        const normals = normalsPtr ? new Float32Array(wasm.HEAPF32.buffer, normalsPtr, vertexCount * 3) : null;
        
        // Create copies since WASM memory will be freed
        const positionsCopy = new Float32Array(positions);
        const colorsCopy = new Float32Array(colors);
        const normalsCopy = normals ? new Float32Array(normals) : null;
        
        // Free WASM memory
        wasm._wasm_free(dataPtr);
        
        postMessage({
            type: 'progress',
            filename,
            message: 'Creating geometry...',
            progress: 50
        });
        
        // Create Three.js geometry
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.Float32BufferAttribute(positionsCopy, 3));
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(colorsCopy, 3));
        
        if (normalsCopy) {
            geometry.setAttribute('normal', new THREE.Float32BufferAttribute(normalsCopy, 3));
        }
        
        postMessage({
            type: 'progress',
            filename,
            message: `Loaded ${vertexCount.toLocaleString()} vertices with WASM`,
            progress: 60
        });
        
        return geometry;
        
    } catch (error) {
        console.error('[WASM] Parsing error details:', error);
        throw error; // Let the caller handle fallback
    }
}

/**
 * Parse PLY binary/ASCII data
 * Simplified PLY parser based on THREE.PLYLoader
 */
function parsePLY(data) {
    const geometry = new THREE.BufferGeometry();
    const dataView = new DataView(data);
    
    // Parse header
    let headerLength = 0;
    let headerText = '';
    const decoder = new TextDecoder();
    
    // Read header (ASCII)
    for (let i = 0; i < data.byteLength; i++) {
        headerText += String.fromCharCode(dataView.getUint8(i));
        if (headerText.endsWith('end_header\n') || headerText.endsWith('end_header\r\n')) {
            headerLength = i + 1;
            break;
        }
    }

    const header = parseHeader(headerText);
    
    if (header.format === 'binary_little_endian' || header.format === 'binary_big_endian') {
        parseBinaryPLY(dataView, headerLength, header, geometry);
    } else {
        parseASCIIPLY(headerText, data, headerLength, header, geometry);
    }

    return geometry;
}

/**
 * Parse PLY header
 */

function parseHeader(headerText) {
    const lines = headerText.split('\n');
    const header = {
        format: null,
        vertices: 0,
        faces: 0,
        properties: []
    };

    for (const line of lines) {
        const parts = line.trim().split(/\s+/);
        
        if (parts[0] === 'format') {
            header.format = parts[1];
        } else if (parts[0] === 'element') {
            if (parts[1] === 'vertex') {
                header.vertices = parseInt(parts[2]);
            } else if (parts[1] === 'face') {
                header.faces = parseInt(parts[2]);
            }
        } else if (parts[0] === 'property') {
            header.properties.push({
                type: parts[1],
                name: parts[2]
            });
        }
    }

    return header;
}

/**
 * Helper function to read property value based on its type
 */
function readPropertyValue(dataView, offset, property, littleEndian) {
    const type = property.type;
    
    switch (type) {
        case 'float':
        case 'float32':
            return dataView.getFloat32(offset, littleEndian);
        case 'double':
        case 'float64':
            return dataView.getFloat64(offset, littleEndian);
        case 'int':
        case 'int32':
            return dataView.getInt32(offset, littleEndian);
        case 'uint':
        case 'uint32':
            return dataView.getUint32(offset, littleEndian);
        case 'short':
        case 'int16':
            return dataView.getInt16(offset, littleEndian);
        case 'ushort':
        case 'uint16':
            return dataView.getUint16(offset, littleEndian);
        case 'char':
        case 'int8':
            return dataView.getInt8(offset);
        case 'uchar':
        case 'uint8':
            return dataView.getUint8(offset);
        default:
            console.warn(`Unknown property type: ${type}, defaulting to float32`);
            return dataView.getFloat32(offset, littleEndian);
    }
}

/**
 * Helper function to get property size in bytes
 */
function getPropertySize(type) {
    switch (type) {
        case 'float':
        case 'float32':
        case 'int':
        case 'int32':
        case 'uint':
        case 'uint32':
            return 4;
        case 'double':
        case 'float64':
            return 8;
        case 'short':
        case 'int16':
        case 'ushort':
        case 'uint16':
            return 2;
        case 'char':
        case 'int8':
        case 'uchar':
        case 'uint8':
            return 1;
        default:
            console.warn(`Unknown property type: ${type}, defaulting to 4 bytes`);
            return 4;
    }
}

/**
 * Parse binary PLY data
 */
function parseBinaryPLY(dataView, offset, header, geometry) {
    const vertices = header.vertices;
    const properties = header.properties;
    const littleEndian = header.format === 'binary_little_endian';

    const positions = [];
    const colors = [];
    const normals = [];

    let hasColor = properties.some(p => p.name === 'red' || p.name === 'diffuse_red');
    let hasNormal = properties.some(p => p.name === 'nx');

    // Calculate stride and store property info
    let stride = 0;
    const propertyInfo = {};
    for (const prop of properties) {
        propertyInfo[prop.name] = {
            offset: stride,
            type: prop.type
        };
        stride += getPropertySize(prop.type);
    }

    // Read vertices
    for (let i = 0; i < vertices; i++) {
        const vertexOffset = offset + (i * stride);

        // Position - handle any numeric type
        const xProp = propertyInfo['x'];
        const yProp = propertyInfo['y'];
        const zProp = propertyInfo['z'];
        
        const x = readPropertyValue(dataView, vertexOffset + xProp.offset, xProp, littleEndian);
        const y = readPropertyValue(dataView, vertexOffset + yProp.offset, yProp, littleEndian);
        const z = readPropertyValue(dataView, vertexOffset + zProp.offset, zProp, littleEndian);
        positions.push(x, y, z);

        // Color
        if (hasColor) {
            const rProp = propertyInfo['red'] ?? propertyInfo['diffuse_red'];
            const gProp = propertyInfo['green'] ?? propertyInfo['diffuse_green'];
            const bProp = propertyInfo['blue'] ?? propertyInfo['diffuse_blue'];
            
            if (rProp && gProp && bProp) {
                let r = readPropertyValue(dataView, vertexOffset + rProp.offset, rProp, littleEndian);
                let g = readPropertyValue(dataView, vertexOffset + gProp.offset, gProp, littleEndian);
                let b = readPropertyValue(dataView, vertexOffset + bProp.offset, bProp, littleEndian);
                
                // Normalize color values to 0-1 range
                // If values are already 0-1 (float), leave as is
                // If values are 0-255 (uchar/uint8), divide by 255
                if (rProp.type.includes('char') || rProp.type.includes('int8')) {
                    r = r / 255;
                    g = g / 255;
                    b = b / 255;
                } else if (r > 1 || g > 1 || b > 1) {
                    // Handle cases where colors are stored as larger integers
                    r = r / 255;
                    g = g / 255;
                    b = b / 255;
                }
                
                colors.push(r, g, b);
            }
        }

        // Normal
        if (hasNormal) {
            const nxProp = propertyInfo['nx'];
            const nyProp = propertyInfo['ny'];
            const nzProp = propertyInfo['nz'];
            
            if (nxProp && nyProp && nzProp) {
                const nx = readPropertyValue(dataView, vertexOffset + nxProp.offset, nxProp, littleEndian);
                const ny = readPropertyValue(dataView, vertexOffset + nyProp.offset, nyProp, littleEndian);
                const nz = readPropertyValue(dataView, vertexOffset + nzProp.offset, nzProp, littleEndian);
                normals.push(nx, ny, nz);
            }
        }
    }

    geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    if (colors.length > 0) {
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
    }
    if (normals.length > 0) {
        geometry.setAttribute('normal', new THREE.Float32BufferAttribute(normals, 3));
    }
}

/**
 * Parse ASCII PLY data
 */
function parseASCIIPLY(headerText, data, headerLength, header, geometry) {
    const decoder = new TextDecoder();
    const bodyText = decoder.decode(data.slice(headerLength));
    const lines = bodyText.split('\n');

    const positions = [];
    const colors = [];
    const normals = [];
    
    // Build property map for index lookup
    const propMap = {};
    header.properties.forEach((prop, idx) => {
        propMap[prop.name] = { index: idx, type: prop.type };
    });
    
    const hasColor = propMap['red'] || propMap['diffuse_red'];
    const hasNormal = propMap['nx'];
    
    for (let i = 0; i < header.vertices && i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        const values = line.split(/\s+/).map(v => {
            const num = parseFloat(v);
            // Check if parsing failed
            if (isNaN(num)) {
                // Try parsing as integer
                const intNum = parseInt(v);
                return isNaN(intNum) ? 0 : intNum;
            }
            return num;
        });
        
        if (values.length >= 3) {
            // Position - handle all numeric types
            const xIdx = propMap['x']?.index ?? 0;
            const yIdx = propMap['y']?.index ?? 1;
            const zIdx = propMap['z']?.index ?? 2;
            
            positions.push(values[xIdx], values[yIdx], values[zIdx]);
            
            // Color
            if (hasColor && values.length >= 6) {
                const rIdx = propMap['red']?.index ?? propMap['diffuse_red']?.index ?? 3;
                const gIdx = propMap['green']?.index ?? propMap['diffuse_green']?.index ?? 4;
                const bIdx = propMap['blue']?.index ?? propMap['diffuse_blue']?.index ?? 5;
                
                let r = values[rIdx];
                let g = values[gIdx];
                let b = values[bIdx];
                
                // Normalize color values - if they're > 1, assume 0-255 range
                if (r > 1 || g > 1 || b > 1) {
                    r = r / 255;
                    g = g / 255;
                    b = b / 255;
                }
                
                colors.push(r, g, b);
            }
            
            // Normals
            if (hasNormal) {
                const nxIdx = propMap['nx']?.index;
                const nyIdx = propMap['ny']?.index;
                const nzIdx = propMap['nz']?.index;
                
                if (nxIdx !== undefined && nyIdx !== undefined && nzIdx !== undefined &&
                    values.length > Math.max(nxIdx, nyIdx, nzIdx)) {
                    normals.push(values[nxIdx], values[nyIdx], values[nzIdx]);
                }
            }
        }
    }

    geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    if (colors.length > 0) {
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
    }
    if (normals.length > 0) {
        geometry.setAttribute('normal', new THREE.Float32BufferAttribute(normals, 3));
    }
}

/**
 * Optimized grid-based downsampling using deterministic spatial hashing
 * Ensures consistent point counts across all devices by:
 * 1. Using strict floor-based grid calculation with local bounds
 * 2. Deterministic point selection (first point in each cell)
 * 3. Consistent floating-point arithmetic
 */
function downsampleGeometryStreaming(geometry, filename, gridSize) {
    const positions = geometry.attributes.position;
    const colors = geometry.attributes.color;
    const normals = geometry.attributes.normal;

    postMessage({
        type: 'progress',
        filename,
        message: 'Downsampling...',
        progress: 0
    });

    const totalPoints = positions.count;
    
    // Use Set with BigInt keys for efficient cell tracking
    // This avoids string allocation and array storage overhead
    const seenCells = new Set();
    const keptIndices = [];
    
    // Use local bounds for grid alignment
    if (!geometry.boundingBox) geometry.computeBoundingBox();
    const min = geometry.boundingBox.min;
    
    // Pre-calculate constants
    const invGridSize = 1.0 / gridSize;
    const minX = min.x;
    const minY = min.y;
    const minZ = min.z;

    // First pass: collect unique points
    for (let i = 0; i < totalPoints; i++) {
        const x = positions.getX(i);
        const y = positions.getY(i);
        const z = positions.getZ(i);

        // Calculate grid indices relative to bounding box min
        const cx = Math.floor((x - minX) * invGridSize);
        const cy = Math.floor((y - minY) * invGridSize);
        const cz = Math.floor((z - minZ) * invGridSize);
        
        // Create unique key using BigInt bit shifting
        // 21 bits per dimension allows for >2 million cells per axis
        const key = BigInt(cx) | (BigInt(cy) << 21n) | (BigInt(cz) << 42n);

        if (!seenCells.has(key)) {
            seenCells.add(key);
            keptIndices.push(i);
        }

        // Report progress every 100k points
        if (i % 100000 === 0) {
            postMessage({
                type: 'progress',
                filename,
                message: 'Downsampling...',
                progress: (i / totalPoints) * 50 // 0-50%
            });
        }
    }

    postMessage({
        type: 'progress',
        filename,
        message: 'Constructing geometry...',
        progress: 50
    });

    const resultCount = keptIndices.length;
    const resultPositions = new Float32Array(resultCount * 3);
    const resultColors = new Float32Array(resultCount * 3);
    const resultNormals = normals ? new Float32Array(resultCount * 3) : null;
    
    // Second pass: copy data
    for (let i = 0; i < resultCount; i++) {
        const srcIdx = keptIndices[i];
        
        resultPositions[i * 3] = positions.getX(srcIdx);
        resultPositions[i * 3 + 1] = positions.getY(srcIdx);
        resultPositions[i * 3 + 2] = positions.getZ(srcIdx);
        
        if (colors) {
            resultColors[i * 3] = colors.getX(srcIdx);
            resultColors[i * 3 + 1] = colors.getY(srcIdx);
            resultColors[i * 3 + 2] = colors.getZ(srcIdx);
        } else {
            resultColors[i * 3] = 1;
            resultColors[i * 3 + 1] = 1;
            resultColors[i * 3 + 2] = 1;
        }
        
        if (resultNormals) {
            resultNormals[i * 3] = normals.getX(srcIdx);
            resultNormals[i * 3 + 1] = normals.getY(srcIdx);
            resultNormals[i * 3 + 2] = normals.getZ(srcIdx);
        }
        
        if (i % 20000 === 0) {
            postMessage({
                type: 'progress',
                filename,
                message: 'Constructing geometry...',
                progress: 50 + (i / resultCount) * 50 // 50-100%
            });
        }
    }

    // Create new geometry
    const newGeometry = new THREE.BufferGeometry();
    newGeometry.setAttribute('position', new THREE.BufferAttribute(resultPositions, 3));
    newGeometry.setAttribute('color', new THREE.BufferAttribute(resultColors, 3));
    if (resultNormals) {
        newGeometry.setAttribute('normal', new THREE.BufferAttribute(resultNormals, 3));
    }

    postMessage({
        type: 'progress',
        filename,
        message: `Downsampled from ${totalPoints.toLocaleString()} to ${resultCount.toLocaleString()} points`,
        progress: 100
    });

    return newGeometry;
}

/**
 * Send geometry data in chunks to avoid blocking
 */
function sendGeometryInChunks(geometry, filename, wasDownsampled) {
    const positions = geometry.attributes.position;
    const colors = geometry.attributes.color;
    const normals = geometry.attributes.normal;
    
    const totalPoints = positions.count;
    let sentPoints = 0;

    while (sentPoints < totalPoints) {
        const chunkPoints = Math.min(CHUNK_SIZE, totalPoints - sentPoints);
        const startIdx = sentPoints;
        const endIdx = sentPoints + chunkPoints;

        // Extract chunk data
        const chunkPositions = new Float32Array(chunkPoints * 3);
        const chunkColors = new Float32Array(chunkPoints * 3);
        const chunkNormals = normals ? new Float32Array(chunkPoints * 3) : null;

        for (let i = 0; i < chunkPoints; i++) {
            const srcIdx = startIdx + i;
            
            chunkPositions[i * 3] = positions.getX(srcIdx);
            chunkPositions[i * 3 + 1] = positions.getY(srcIdx);
            chunkPositions[i * 3 + 2] = positions.getZ(srcIdx);

            if (colors) {
                chunkColors[i * 3] = colors.getX(srcIdx);
                chunkColors[i * 3 + 1] = colors.getY(srcIdx);
                chunkColors[i * 3 + 2] = colors.getZ(srcIdx);
            } else {
                chunkColors[i * 3] = 1;
                chunkColors[i * 3 + 1] = 1;
                chunkColors[i * 3 + 2] = 1;
            }

            if (chunkNormals && normals) {
                chunkNormals[i * 3] = normals.getX(srcIdx);
                chunkNormals[i * 3 + 1] = normals.getY(srcIdx);
                chunkNormals[i * 3 + 2] = normals.getZ(srcIdx);
            }
        }

        // Send chunk with transferable arrays for better performance
        postMessage({
            type: 'chunk',
            filename,
            positions: chunkPositions,
            colors: chunkColors,
            normals: chunkNormals,
            isFirst: sentPoints === 0,
            isLast: endIdx >= totalPoints,
            totalPoints,
            chunkStart: sentPoints,
            chunkEnd: endIdx,
            wasDownsampled
        }, [chunkPositions.buffer, chunkColors.buffer, chunkNormals ? chunkNormals.buffer : null].filter(Boolean));

        sentPoints = endIdx;
    }
}

/**
 * Helper functions
 */
function ensureGeometryHasNormals(geometry) {
    if (!geometry.attributes.normal) {
        geometry.computeVertexNormals();
    }
}

function createDefaultColors(count) {
    const colors = new Float32Array(count * 3);
    for (let i = 0; i < count * 3; i++) {
        colors[i] = 1.0;
    }
    return colors;
}

/**
 * Worker message handler
 */
self.onmessage = function(e) {
    const { type, url, filename, centerOffset, qualityMode } = e.data;

    if (type === 'load') {
        loadAndProcessPLY(url, filename, centerOffset, qualityMode);
    }
};
