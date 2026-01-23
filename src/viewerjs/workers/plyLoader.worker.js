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
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
        }

        const arrayBuffer = await response.arrayBuffer();
        const geometry = parsePLY(arrayBuffer);

        if (!geometry.attributes.position) {
            throw new Error('Missing position attribute');
        }

        // Send initial metadata
        postMessage({
            type: 'metadata',
            filename,
            totalPoints: geometry.attributes.position.count
        });

        // Center the geometry
        if (centerOffset) {
            geometry.translate(-centerOffset.x, -centerOffset.y, -centerOffset.z);
        } else {
            geometry.computeBoundingBox();
            const center = geometry.boundingBox.getCenter(new THREE.Vector3());
            geometry.translate(-center.x, -center.y, -center.z);
        }

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
            
            // Downsample and send in chunks
            const downsampled = downsampleGeometryStreaming(geometry, filename, gridSize);
            sendGeometryInChunks(downsampled, filename, true);
        } else {
            // Send as-is in chunks
            sendGeometryInChunks(geometry, filename, false);
        }

        // Send completion message
        postMessage({
            type: 'complete',
            filename
        });

    } catch (error) {
        postMessage({
            type: 'error',
            filename,
            error: error.message
        });
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

    // Calculate stride
    let stride = 0;
    const propertyOffsets = {};
    for (const prop of properties) {
        propertyOffsets[prop.name] = stride;
        
        if (prop.type === 'float' || prop.type === 'float32') {
            stride += 4;
        } else if (prop.type === 'uchar' || prop.type === 'uint8') {
            stride += 1;
        } else if (prop.type === 'int' || prop.type === 'int32') {
            stride += 4;
        } else if (prop.type === 'double') {
            stride += 8;
        }
    }

    // Read vertices
    for (let i = 0; i < vertices; i++) {
        const vertexOffset = offset + (i * stride);

        // Position
        const x = dataView.getFloat32(vertexOffset + propertyOffsets['x'], littleEndian);
        const y = dataView.getFloat32(vertexOffset + propertyOffsets['y'], littleEndian);
        const z = dataView.getFloat32(vertexOffset + propertyOffsets['z'], littleEndian);
        positions.push(x, y, z);

        // Color
        if (hasColor) {
            const rOffset = propertyOffsets['red'] ?? propertyOffsets['diffuse_red'];
            const gOffset = propertyOffsets['green'] ?? propertyOffsets['diffuse_green'];
            const bOffset = propertyOffsets['blue'] ?? propertyOffsets['diffuse_blue'];
            
            const r = dataView.getUint8(vertexOffset + rOffset) / 255;
            const g = dataView.getUint8(vertexOffset + gOffset) / 255;
            const b = dataView.getUint8(vertexOffset + bOffset) / 255;
            colors.push(r, g, b);
        }

        // Normal
        if (hasNormal) {
            const nx = dataView.getFloat32(vertexOffset + propertyOffsets['nx'], littleEndian);
            const ny = dataView.getFloat32(vertexOffset + propertyOffsets['ny'], littleEndian);
            const nz = dataView.getFloat32(vertexOffset + propertyOffsets['nz'], littleEndian);
            normals.push(nx, ny, nz);
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
    
    for (let i = 0; i < header.vertices && i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        const values = line.split(/\s+/).map(parseFloat);
        if (values.length >= 3) {
            positions.push(values[0], values[1], values[2]);
            
            // If we have RGB values (usually values[3], [4], [5])
            if (values.length >= 6) {
                colors.push(values[3] / 255, values[4] / 255, values[5] / 255);
            }
        }
    }

    geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    if (colors.length > 0) {
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
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
