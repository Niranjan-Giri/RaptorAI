import * as THREE from 'three';
import createPlyModule from '../../../public/bin/PLY.js';

// ── Early message queue ─────────────────────────────────────────────────────
// Register the message handler IMMEDIATELY — before any top-level await.
// Without this, messages posted by the main thread during WASM init would be
// dispatched while self.onmessage is still null and silently dropped.
const _earlyMessages = [];
self.onmessage = function(e) {
    _earlyMessages.push(e);
};

// Downsampling configuration
const TARGET_POINTS = 3000000; 
const DOWNSAMPLE_THRESHOLD = 400000;
const USE_RANDOM_SELECTION = false;
const CHUNK_SIZE = 500000;

const GLOBAL_MIN_BOUNDS = {
    x: -1000.0,
    y: -1000.0,
    z: -1000.0
};

// ── WASM module initialization ──────────────────────────────────────────────
// Eagerly initialize at module load. Since PLY.js is built with pthreads +
// EXPORT_ES6, Emscripten will try to spawn classic Web Workers for threads.
// We wrap initialization in a try/catch so the module still works
// single-threaded if pthread spawning fails in the nested worker context.
let wasmModule = null;
try {
    wasmModule = await createPlyModule({
        // For pthread workers: point at the public/bin/ copy
        mainScriptUrlOrBlob: new URL('../../../bin/PLY.js', import.meta.url).href,
        locateFile(path) {
            if (path.endsWith('.wasm')) {
                return new URL('../../../bin/PLY.wasm', import.meta.url).href;
            }
            // For the .js file too (pthread workers may request it)
            if (path.endsWith('.js') || path.endsWith('.mjs')) {
                return new URL('../../../bin/' + path, import.meta.url).href;
            }
            return path;
        }
    });
    console.log('[WASM] Module initialized successfully');
} catch (err) {
    console.error('[WASM] Module init failed:', err);
    wasmModule = null;
}

/**
 * Safely get the underlying WASM linear-memory ArrayBuffer.
 * This build only exposes Module["HEAPF32"] — we use its .buffer
 * as the canonical source for all typed-array views.
 */
function getWasmBuffer() {
    if (wasmModule.HEAPF32 && wasmModule.HEAPF32.buffer) return wasmModule.HEAPF32.buffer;
    if (wasmModule.HEAPU8 && wasmModule.HEAPU8.buffer) return wasmModule.HEAPU8.buffer;
    if (wasmModule.wasmMemory && wasmModule.wasmMemory.buffer) return wasmModule.wasmMemory.buffer;
    if (wasmModule.asm?.memory?.buffer) return wasmModule.asm.memory.buffer;
    throw new Error('Cannot locate WASM linear memory buffer');
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
        // WASM module is already initialized at the top level
        useWasm = !!wasmModule;
        if (useWasm) {
            console.log(`[WASM] Using WASM parser for ${filename}`);
        }
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
        }

        // Ensure we're getting binary data
        const arrayBuffer = await response.arrayBuffer();
        const fetchTime = performance.now() - startTime;
        console.log(`[${useWasm ? 'WASM' : 'JS'}] Fetched ${filename} in ${fetchTime.toFixed(2)}ms`);
        
        // ---- WASM fast path: parse + downsample entirely in WASM ----
        if (useWasm) {
            try {
                const needsDownsampling = qualityMode === 'downsampled';
                const result = await parsePLYWithWasm(arrayBuffer, filename, needsDownsampling);
                const parseTime = performance.now() - startTime - fetchTime;
                console.log(`[WASM] Parsed${result.wasDownsampled ? ' + downsampled' : ''} ${result.vertexCount.toLocaleString()} points in ${parseTime.toFixed(2)}ms`);

                // Send metadata
                postMessage({ type: 'metadata', filename, totalPoints: result.originalCount });

                // Send raw Float32Arrays directly in chunks (no THREE.js overhead)
                sendRawArraysInChunks(result.positions, result.colors, result.normals, filename, result.wasDownsampled);

                const totalTime = performance.now() - startTime;
                console.log(`[WASM] Total processing time for ${filename}: ${totalTime.toFixed(2)}ms`);
                postMessage({ type: 'complete', filename });
                return; // Done — skip JS path
            } catch (wasmParseError) {
                console.warn('[WASM] WASM parse/resample failed, falling back to JS:', wasmParseError.message);
            }
        }

        // ---- JS fallback path ----
        const parseStartTime = performance.now();
        const geometry = parsePLY(arrayBuffer);
        console.log(`[JS] Parsed ${geometry.attributes.position.count.toLocaleString()} points in ${(performance.now() - parseStartTime).toFixed(2)}ms`);

        if (!geometry.attributes.position) {
            throw new Error('Missing position attribute');
        }

        postMessage({ type: 'metadata', filename, totalPoints: geometry.attributes.position.count });

        ensureGeometryHasNormals(geometry);
        if (!geometry.attributes.color) {
            const defaultColors = createDefaultColors(geometry.attributes.position.count);
            geometry.setAttribute('color', new THREE.Float32BufferAttribute(defaultColors, 3));
        }

        const needsDownsampling = qualityMode === 'downsampled' && geometry.attributes.position.count > DOWNSAMPLE_THRESHOLD;

        if (needsDownsampling) {
            const gridSize = calculateOptimalGridSize(geometry, TARGET_POINTS);
            const downsampleStartTime = performance.now();
            const downsampled = downsampleGeometryStreaming(geometry, filename, gridSize);
            console.log(`[JS] Downsampled to ${downsampled.attributes.position.count.toLocaleString()} points in ${(performance.now() - downsampleStartTime).toFixed(2)}ms`);
            sendGeometryInChunks(downsampled, filename, true);
        } else {
            sendGeometryInChunks(geometry, filename, false);
        }

        console.log(`[JS] Total processing time for ${filename}: ${(performance.now() - startTime).toFixed(2)}ms`);
        postMessage({ type: 'complete', filename });

    } catch (error) {
        console.error('[PLY Loader] Error loading PLY:', error);
        console.error('[PLY Loader] Stack trace:', error.stack);
        postMessage({ type: 'error', filename, error: error.message });
    }
}

/**
 * Parse PLY using WASM module — returns raw Float32Arrays (no THREE.js)
 * When downsample=true, calls resample_ply() in WASM so the heavy work stays native.
 */
async function parsePLYWithWasm(arrayBuffer, filename, downsample = false) {
    const wasm = wasmModule;
    
    try {
        postMessage({ type: 'progress', filename, message: 'Parsing with WASM...', progress: 10 });
        
        // Allocate memory in WASM heap and copy PLY data in
        const byteLength = arrayBuffer.byteLength;
        const dataPtr = wasm._wasm_alloc(byteLength);
        if (!dataPtr) throw new Error('Failed to allocate WASM memory');
        
        // Re-fetch buffer after alloc (memory may have grown, detaching old views)
        new Uint8Array(getWasmBuffer(), dataPtr, byteLength).set(new Uint8Array(arrayBuffer));
        
        // Parse the PLY file (header + vertex data)
        // C signature: process_ply_buffer(data, length, voxelSize, threadCount)
        //   voxelSize=0 → parse only (no downsampling yet)
        //   threadCount=1 (WASM is single-threaded)
        //   Returns 1 on success, 0 on failure
        const parseResult = wasm._process_ply_buffer(dataPtr, byteLength, 0.0, 1);
        if (parseResult !== 1) {
            wasm._wasm_free(dataPtr);
            throw new Error(`WASM PLY parsing failed (returned ${parseResult})`);
        }
        
        const originalCount = wasm._get_vertex_count();
        if (originalCount === 0) {
            wasm._wasm_free(dataPtr);
            throw new Error('No vertices found in PLY file');
        }
        
        postMessage({ type: 'progress', filename, message: `Parsed ${originalCount.toLocaleString()} vertices`, progress: 30 });
        
        // ---- Downsample in WASM if requested ----
        let wasDownsampled = false;
        if (downsample && originalCount > DOWNSAMPLE_THRESHOLD) {
            postMessage({ type: 'progress', filename, message: 'Downsampling in WASM...', progress: 40 });
            
            // Calculate optimal grid size from bounding box (read positions pointer)
            const gridSize = calculateOptimalGridSizeFromWasm(wasm, originalCount, TARGET_POINTS);
            console.log(`[WASM] resample_ply gridSize=${gridSize.toFixed(6)} for ${originalCount.toLocaleString()} -> ~${TARGET_POINTS.toLocaleString()} target`);
            
            // C signature: resample_ply(voxelSize, threadCount)
            //   threadCount=1 (WASM is single-threaded)
            //   Returns 1 on success, 0 on failure
            const resampleResult = wasm._resample_ply(gridSize, 1);
            if (resampleResult === 1) {
                wasDownsampled = true;
                console.log(`[WASM] Resampled from ${originalCount.toLocaleString()} to ${wasm._get_vertex_count().toLocaleString()} points`);
            } else {
                console.warn(`[WASM] resample_ply failed (returned ${resampleResult}), using full data`);
            }
        }
        
        postMessage({ type: 'progress', filename, message: 'Extracting geometry data...', progress: 60 });
        
        // Read final vertex count (may have changed after resample)
        const vertexCount = wasm._get_vertex_count();
        const positionsPtr = wasm._get_positions();
        const colorsPtr = wasm._get_colors();
        const normalsPtr = wasm._get_normals();
        
        // Copy from WASM heap in one shot — much faster than per-vertex accessor calls
        // Re-fetch buffer (resample may have grown memory)
        const memBuffer = getWasmBuffer();
        const positions = new Float32Array(memBuffer, positionsPtr, vertexCount * 3).slice();
        const colors = new Float32Array(memBuffer, colorsPtr, vertexCount * 3).slice();
        // normalsPtr is a C pointer — 0 means null (no normals in the file)
        const normals = (normalsPtr && normalsPtr !== 0)
            ? new Float32Array(memBuffer, normalsPtr, vertexCount * 3).slice()
            : null;
        
        // Free WASM memory
        wasm._wasm_free(dataPtr);
        
        // Ensure colors are valid (fill white if all zeros)
        let hasColor = false;
        for (let i = 0; i < Math.min(colors.length, 30); i++) {
            if (colors[i] !== 0) { hasColor = true; break; }
        }
        if (!hasColor) colors.fill(1.0);
        
        postMessage({ type: 'progress', filename, message: `Ready: ${vertexCount.toLocaleString()} points`, progress: 80 });
        
        return { positions, colors, normals, vertexCount, originalCount, wasDownsampled };
        
    } catch (error) {
        console.error('[WASM] Parsing error details:', error);
        throw error;
    }
}

/**
 * Calculate optimal grid size by scanning WASM position data for bounding box.
 * Avoids creating a THREE.BufferGeometry just for bounds calculation.
 */
function calculateOptimalGridSizeFromWasm(wasm, vertexCount, targetPoints) {
    const posPtr = wasm._get_positions();
    const pos = new Float32Array(getWasmBuffer(), posPtr, vertexCount * 3);
    
    let minX = Infinity, minY = Infinity, minZ = Infinity;
    let maxX = -Infinity, maxY = -Infinity, maxZ = -Infinity;
    
    // Sample every Nth point for speed (bounds don't need every point)
    const step = Math.max(1, Math.floor(vertexCount / 100000)) * 3;
    for (let i = 0; i < pos.length; i += step) {
        const x = pos[i], y = pos[i + 1], z = pos[i + 2];
        if (x < minX) minX = x; if (x > maxX) maxX = x;
        if (y < minY) minY = y; if (y > maxY) maxY = y;
        if (z < minZ) minZ = z; if (z > maxZ) maxZ = z;
    }
    
    const dx = Math.max(maxX - minX, 0.001);
    const dy = Math.max(maxY - minY, 0.001);
    const dz = Math.max(maxZ - minZ, 0.001);
    const volume = dx * dy * dz;
    return Math.max(0.001, Math.pow(volume / targetPoints, 1 / 3));
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
    
    // Read header (ASCII) - limit to first 10KB to find header
    const maxHeaderSize = Math.min(10000, data.byteLength);
    for (let i = 0; i < maxHeaderSize; i++) {
        headerText += String.fromCharCode(dataView.getUint8(i));
        if (headerText.endsWith('end_header\n') || headerText.endsWith('end_header\r\n')) {
            headerLength = i + 1;
            break;
        }
    }

    if (headerLength === 0) {
        throw new Error('Could not find PLY header end marker');
    }

    const header = parseHeader(headerText);
    
    if (!header.format) {
        throw new Error('Invalid PLY format in header');
    }
    
    console.log(`[Worker] PLY format: ${header.format}, vertices: ${header.vertices}, faces: ${header.faces}`);
    
    if (header.format === 'binary_little_endian' || header.format === 'binary_big_endian') {
        parseBinaryPLY(dataView, headerLength, header, geometry);
    } else if (header.format === 'ascii') {
        parseASCIIPLY(headerText, data, headerLength, header, geometry);
    } else {
        throw new Error(`Unsupported PLY format: ${header.format}`);
    }
    
    // CRITICAL: Ensure no index buffer exists to prevent line rendering artifacts
    // This is especially important when PLY files contain face data
    if (geometry.index !== null) {
        console.log(`[Worker] Removing ${geometry.index.count} face indices from geometry`);
        geometry.setIndex(null);
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
    
    // CRITICAL: Remove any index buffer (faces) to prevent line artifacts in point cloud rendering
    // Face data causes THREE.Points to render connected vertices as lines
    geometry.setIndex(null);
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
    
    // CRITICAL: Remove any index buffer (faces) to prevent line artifacts in point cloud rendering
    // Face data causes THREE.Points to render connected vertices as lines
    geometry.setIndex(null);
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
 * Send raw Float32Arrays in chunks (WASM fast path — no THREE.js accessors).
 * Uses .slice() for zero-copy-friendly chunk extraction + transferable buffers.
 */
function sendRawArraysInChunks(positions, colors, normals, filename, wasDownsampled) {
    const totalPoints = positions.length / 3;
    let sentPoints = 0;

    while (sentPoints < totalPoints) {
        const chunkPoints = Math.min(CHUNK_SIZE, totalPoints - sentPoints);
        const startF = sentPoints * 3;
        const endF = startF + chunkPoints * 3;
        const endIdx = sentPoints + chunkPoints;

        // Slice out chunk — creates a compact copy that can be transferred
        const chunkPositions = positions.slice(startF, endF);
        const chunkColors = colors.slice(startF, endF);
        const chunkNormals = normals ? normals.slice(startF, endF) : null;

        const transferList = [chunkPositions.buffer, chunkColors.buffer];
        if (chunkNormals) transferList.push(chunkNormals.buffer);

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
        }, transferList);

        sentPoints = endIdx;
    }
}

/**
 * Send geometry data in chunks to avoid blocking (JS fallback path)
 */
function sendGeometryInChunks(geometry, filename, wasDownsampled) {
    const posAttr = geometry.attributes.position;
    const colAttr = geometry.attributes.color;
    const norAttr = geometry.attributes.normal;
    
    // Fast path: if underlying arrays are contiguous Float32Arrays, use raw sender
    if (posAttr.array instanceof Float32Array) {
        sendRawArraysInChunks(
            posAttr.array,
            colAttr ? colAttr.array : new Float32Array(posAttr.count * 3).fill(1),
            norAttr ? norAttr.array : null,
            filename,
            wasDownsampled
        );
        return;
    }

    // Slow fallback with per-element accessors
    const totalPoints = posAttr.count;
    let sentPoints = 0;

    while (sentPoints < totalPoints) {
        const chunkPoints = Math.min(CHUNK_SIZE, totalPoints - sentPoints);
        const endIdx = sentPoints + chunkPoints;

        const chunkPositions = new Float32Array(chunkPoints * 3);
        const chunkColors = new Float32Array(chunkPoints * 3);
        const chunkNormals = norAttr ? new Float32Array(chunkPoints * 3) : null;

        for (let i = 0; i < chunkPoints; i++) {
            const s = sentPoints + i;
            const d = i * 3;
            chunkPositions[d] = posAttr.getX(s);
            chunkPositions[d + 1] = posAttr.getY(s);
            chunkPositions[d + 2] = posAttr.getZ(s);
            if (colAttr) {
                chunkColors[d] = colAttr.getX(s);
                chunkColors[d + 1] = colAttr.getY(s);
                chunkColors[d + 2] = colAttr.getZ(s);
            } else {
                chunkColors[d] = 1; chunkColors[d + 1] = 1; chunkColors[d + 2] = 1;
            }
            if (chunkNormals && norAttr) {
                chunkNormals[d] = norAttr.getX(s);
                chunkNormals[d + 1] = norAttr.getY(s);
                chunkNormals[d + 2] = norAttr.getZ(s);
            }
        }

        const transferList = [chunkPositions.buffer, chunkColors.buffer];
        if (chunkNormals) transferList.push(chunkNormals.buffer);

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
        }, transferList);

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
 * Replaces the early-queue handler now that all functions are defined.
 */
function handleWorkerMessage(e) {
    const { type, url, filename, centerOffset, qualityMode } = e.data;

    if (type === 'load') {
        loadAndProcessPLY(url, filename, centerOffset, qualityMode);
    }
}

// Install the real handler and replay anything that arrived during init
self.onmessage = handleWorkerMessage;
for (const msg of _earlyMessages) {
    handleWorkerMessage(msg);
}
