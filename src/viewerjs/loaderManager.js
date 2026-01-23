/**
 * Manages Web Worker-based PLY file loading
 * Coordinates multiple workers and streams data to the main thread
 */

import * as THREE from 'three';

export class LoaderManager {
    constructor(onFileLoaded, onFileProgress, onFileError) {
        this.onFileLoaded = onFileLoaded;
        this.onFileProgress = onFileProgress;
        this.onFileError = onFileError;
        
        this.workers = [];
        this.maxWorkers = navigator.hardwareConcurrency || 4;
        this.activeLoads = new Map(); // filename -> {geometry chunks, worker}
        this.loadQueue = [];
        this.pendingUpdates = new Map(); // Throttle updates per file
        this.updateInterval = 500; // Minimum ms between updates
        this.qualityMode = 'downsampled'; // 'downsampled' or 'original'
        
        // Buffer reuse pool to avoid repeated allocations
        this.bufferPool = {
            positions: [],
            colors: [],
            normals: []
        };
    }

    /**
     * Set quality mode for loading
     */
    setQualityMode(mode) {
        this.qualityMode = mode;
    }

    /**
     * Load a PLY file using a web worker
     */
    loadPLY(filepath, filename) {
        return new Promise((resolve, reject) => {
            const loadTask = {
                filepath,
                filename,
                resolve,
                reject
            };

            this.loadQueue.push(loadTask);
            this.processQueue();
        });
    }

    /**
     * Process the load queue
     */
    processQueue() {
        // Start as many loads as we have workers available
        while (this.loadQueue.length > 0 && this.workers.length < this.maxWorkers) {
            const task = this.loadQueue.shift();
            this.startLoad(task);
        }
    }

    /**
     * Start loading a file with a worker
     */
    startLoad(task) {
        const { filepath, filename, resolve, reject } = task;

        // Create worker
        const worker = new Worker(
            new URL('./workers/plyLoader.worker.js', import.meta.url),
            { type: 'module' }
        );

        // Initialize load state
        this.activeLoads.set(filename, {
            worker,
            chunks: [],
            totalPoints: 0,
            receivedPoints: 0,
            resolve,
            reject,
            startTime: performance.now()
        });

        // Handle worker messages
        worker.onmessage = (e) => this.handleWorkerMessage(filename, e.data);
        worker.onerror = (error) => {
            console.error(`Worker error for ${filename}:`, error);
            this.cleanupLoad(filename);
            reject(error);
            if (this.onFileError) {
                this.onFileError(filename, error.message);
            }
        };

        // Start the load
        worker.postMessage({
            type: 'load',
            url: filepath,
            filename,
            centerOffset: null,
            qualityMode: this.qualityMode
        });

        this.workers.push(worker);
        
        console.log(`[LoaderManager] Started loading ${filename} with worker`);
    }

    /**
     * Handle messages from worker
     */
    handleWorkerMessage(filename, data) {
        const loadState = this.activeLoads.get(filename);
        if (!loadState) return;

        switch (data.type) {
            case 'metadata':
                loadState.totalPoints = data.totalPoints;
                console.log(`[${filename}] Total points: ${data.totalPoints.toLocaleString()}`);
                break;

            case 'progress':
                if (this.onFileProgress) {
                    this.onFileProgress(filename, data.message, data.progress);
                }
                break;

            case 'chunk':
                this.handleChunk(filename, data);
                break;

            case 'complete':
                this.completeLoad(filename);
                break;

            case 'error':
                console.error(`[${filename}] Error:`, data.error);
                this.cleanupLoad(filename);
                loadState.reject(new Error(data.error));
                if (this.onFileError) {
                    this.onFileError(filename, data.error);
                }
                break;
        }
    }

    /**
     * Handle a geometry chunk from worker
     */
    handleChunk(filename, chunkData) {
        const loadState = this.activeLoads.get(filename);
        if (!loadState) return;

        const {
            positions,
            colors,
            normals,
            isFirst,
            isLast,
            totalPoints,
            chunkStart,
            chunkEnd,
            wasDownsampled
        } = chunkData;

        // Store chunk
        loadState.chunks.push({
            positions,
            colors,
            normals
        });

        loadState.receivedPoints = chunkEnd;

        // If this is the first chunk, immediately create preview geometry
        if (isFirst) {
            const previewGeometry = this.createGeometryFromChunks([loadState.chunks[0]]);
            
            if (this.onFileLoaded) {
                this.onFileLoaded(filename, previewGeometry, {
                    isPreview: !isLast,
                    totalExpectedPoints: totalPoints,
                    wasDownsampled
                });
            }
            
            // Mark the last update time
            this.pendingUpdates.set(filename, Date.now());
        }

        // Report progress
        const progress = (chunkEnd / totalPoints) * 100;
        if (this.onFileProgress) {
            this.onFileProgress(
                filename,
                `Loading: ${chunkEnd.toLocaleString()} / ${totalPoints.toLocaleString()} points`,
                progress
            );
        }

        // Throttled incremental updates - only update if enough time has passed
        if (!isFirst && !isLast) {
            const lastUpdate = this.pendingUpdates.get(filename) || 0;
            const now = Date.now();
            
            if (now - lastUpdate >= this.updateInterval) {
                // Schedule update during idle time to avoid blocking interactions
                this.scheduleIdleUpdate(filename, loadState, totalPoints, wasDownsampled);
                this.pendingUpdates.set(filename, now);
            }
        }
    }

    /**
     * Schedule geometry update during browser idle time
     * Optimized to grow buffers incrementally instead of full recreation
     */
    scheduleIdleUpdate(filename, loadState, totalPoints, wasDownsampled) {
        // Use requestIdleCallback if available, otherwise setTimeout
        const scheduleFunc = window.requestIdleCallback || ((cb) => setTimeout(cb, 16));
        
        scheduleFunc(() => {
            // Check if load is still active
            if (!this.activeLoads.has(filename)) return;
            
            // If existing geometry exists, grow it incrementally
            if (loadState.currentGeometry && loadState.lastChunkIndex !== undefined) {
                const newChunks = loadState.chunks.slice(loadState.lastChunkIndex);
                if (newChunks.length > 0) {
                    this.growGeometryWithChunks(loadState.currentGeometry, newChunks);
                    loadState.lastChunkIndex = loadState.chunks.length;
                    
                    if (this.onFileLoaded) {
                        this.onFileLoaded(filename, loadState.currentGeometry, {
                            isPreview: true,
                            totalExpectedPoints: totalPoints,
                            wasDownsampled,
                            isIdleUpdate: true,
                            isIncremental: true
                        });
                    }
                }
            } else {
                // First update - create new geometry
                const incrementalGeometry = this.createGeometryFromChunks(loadState.chunks);
                loadState.currentGeometry = incrementalGeometry;
                loadState.lastChunkIndex = loadState.chunks.length;
                
                if (this.onFileLoaded) {
                    this.onFileLoaded(filename, incrementalGeometry, {
                        isPreview: true,
                        totalExpectedPoints: totalPoints,
                        wasDownsampled,
                        isIdleUpdate: true
                    });
                }
            }
        }, { timeout: 100 });
    }
    
    /**
     * Grow existing geometry with new chunks using exponential growth strategy
     * Uses ArrayBuffer views to avoid unnecessary copies
     */
    growGeometryWithChunks(geometry, newChunks) {
        if (newChunks.length === 0) return;
        
        const posAttr = geometry.attributes.position;
        const colorAttr = geometry.attributes.color;
        const normalAttr = geometry.attributes.normal;
        
        // Calculate new total size
        let newPointsCount = 0;
        for (const chunk of newChunks) {
            newPointsCount += chunk.positions.length / 3;
        }
        
        const oldCount = posAttr.count;
        const newTotalCount = oldCount + newPointsCount;
        const newTotalSize = newTotalCount * 3;
        
        // Get or initialize growth metadata
        if (!geometry.userData.bufferCapacity) {
            // First growth - initialize metadata
            geometry.userData.bufferCapacity = posAttr.array.length;
            geometry.userData.bufferCount = oldCount;
        }
        
        const currentCapacity = geometry.userData.bufferCapacity;
        
        // Check if we need to grow the underlying buffers
        let newPositions, newColors, newNormals;
        
        if (newTotalSize <= currentCapacity) {
            // We have enough capacity - reuse existing ArrayBuffer with new view
            newPositions = new Float32Array(posAttr.array.buffer, 0, newTotalSize);
            newColors = new Float32Array(colorAttr.array.buffer, 0, newTotalSize);
            newNormals = normalAttr ? new Float32Array(normalAttr.array.buffer, 0, newTotalSize) : null;
            
            // Append new chunks at the end
            let offset = oldCount * 3;
            for (const chunk of newChunks) {
                newPositions.set(chunk.positions, offset);
                newColors.set(chunk.colors, offset);
                if (newNormals && chunk.normals) {
                    newNormals.set(chunk.normals, offset);
                }
                offset += chunk.positions.length;
            }
        } else {
            // Need to grow - use exponential growth (2x strategy)
            const newCapacity = Math.max(newTotalSize, currentCapacity * 2);
            
            // Allocate new buffers with extra capacity
            newPositions = new Float32Array(newCapacity);
            newColors = new Float32Array(newCapacity);
            newNormals = normalAttr ? new Float32Array(newCapacity) : null;
            
            // Copy existing data
            newPositions.set(posAttr.array.subarray(0, oldCount * 3));
            newColors.set(colorAttr.array.subarray(0, oldCount * 3));
            if (newNormals && normalAttr) {
                newNormals.set(normalAttr.array.subarray(0, oldCount * 3));
            }
            
            // Append new chunks
            let offset = oldCount * 3;
            for (const chunk of newChunks) {
                newPositions.set(chunk.positions, offset);
                newColors.set(chunk.colors, offset);
                if (newNormals && chunk.normals) {
                    newNormals.set(chunk.normals, offset);
                }
                offset += chunk.positions.length;
            }
            
            // Create views for the actual used portion
            newPositions = new Float32Array(newPositions.buffer, 0, newTotalSize);
            newColors = new Float32Array(newColors.buffer, 0, newTotalSize);
            if (newNormals) {
                newNormals = new Float32Array(newNormals.buffer, 0, newTotalSize);
            }
            
            // Update capacity tracking
            geometry.userData.bufferCapacity = newCapacity;
        }
        
        // Update geometry attributes with new views
        geometry.setAttribute('position', new THREE.Float32BufferAttribute(newPositions, 3));
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(newColors, 3));
        if (newNormals) {
            geometry.setAttribute('normal', new THREE.Float32BufferAttribute(newNormals, 3));
        }
        
        geometry.userData.bufferCount = newTotalCount;
        geometry.computeBoundingBox();
    }

    /**
     * Complete the load and create final geometry
     */
    completeLoad(filename) {
        const loadState = this.activeLoads.get(filename);
        if (!loadState) return;

        const elapsedTime = ((performance.now() - loadState.startTime) / 1000).toFixed(2);
        
        // Create final geometry from all chunks
        const finalGeometry = this.createGeometryFromChunks(loadState.chunks);
        
        console.log(`[${filename}] Loaded in ${elapsedTime}s - ${loadState.receivedPoints.toLocaleString()} points`);

        // Resolve promise
        loadState.resolve(finalGeometry);

        // Send final callback
        if (this.onFileLoaded) {
            this.onFileLoaded(filename, finalGeometry, {
                isPreview: false,
                totalExpectedPoints: loadState.totalPoints,
                wasDownsampled: loadState.receivedPoints < loadState.totalPoints
            });
        }

        // Cleanup
        this.cleanupLoad(filename);

        // Process next item in queue
        this.processQueue();
    }

    /**
     * Create THREE.BufferGeometry from chunks with buffer reuse and exponential pre-allocation
     */
    createGeometryFromChunks(chunks) {
        // Calculate total size
        let totalPoints = 0;
        for (const chunk of chunks) {
            totalPoints += chunk.positions.length / 3;
        }

        const totalSize = totalPoints * 3;
        
        // Pre-allocate with extra capacity for potential growth (1.5x)
        // This reduces reallocations during incremental updates
        const allocSize = Math.ceil(totalSize * 1.5);
        
        // Try to reuse buffers from pool, or allocate new ones
        let positions = this.getPooledBuffer('positions', allocSize);
        let colors = this.getPooledBuffer('colors', allocSize);
        let normals = chunks[0].normals ? this.getPooledBuffer('normals', allocSize) : null;

        // Merge chunks
        let offset = 0;
        for (const chunk of chunks) {
            const chunkSize = chunk.positions.length;
            
            positions.set(chunk.positions, offset);
            colors.set(chunk.colors, offset);
            if (normals && chunk.normals) {
                normals.set(chunk.normals, offset);
            }
            
            offset += chunkSize;
        }

        // Create views of the exact used size
        const posView = new Float32Array(positions.buffer, 0, totalSize);
        const colorView = new Float32Array(colors.buffer, 0, totalSize);
        const normalView = normals ? new Float32Array(normals.buffer, 0, totalSize) : null;

        // Create geometry
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.Float32BufferAttribute(posView, 3));
        geometry.setAttribute('color', new THREE.Float32BufferAttribute(colorView, 3));
        
        if (normalView) {
            geometry.setAttribute('normal', new THREE.Float32BufferAttribute(normalView, 3));
        } else {
            geometry.computeVertexNormals();
        }

        geometry.computeBoundingBox();
        
        // Track capacity for efficient growth
        geometry.userData.bufferCapacity = allocSize;
        geometry.userData.bufferCount = totalPoints;
        
        // Free buffers back to pool when geometry is disposed
        geometry.userData.bufferPooled = true;
        const originalDispose = geometry.dispose.bind(geometry);
        geometry.dispose = () => {
            if (geometry.userData.bufferPooled) {
                this.returnBufferToPool('positions', positions);
                this.returnBufferToPool('colors', colors);
                if (normals) this.returnBufferToPool('normals', normals);
                geometry.userData.bufferPooled = false;
            }
            originalDispose();
        };

        return geometry;
    }
    
    /**
     * Get a buffer from pool or allocate new one
     */
    getPooledBuffer(type, size) {
        const pool = this.bufferPool[type];
        
        // Find a buffer that's large enough
        for (let i = 0; i < pool.length; i++) {
            const buffer = pool[i];
            if (buffer.length >= size) {
                // Remove from pool and return (reuse existing or slice if too large)
                pool.splice(i, 1);
                return buffer.length === size ? buffer : new Float32Array(buffer.buffer, 0, size);
            }
        }
        
        // No suitable buffer found, allocate new one
        return new Float32Array(size);
    }
    
    /**
     * Return a buffer to pool for reuse
     */
    returnBufferToPool(type, buffer) {
        const pool = this.bufferPool[type];
        
        // Only pool buffers up to 50MB to avoid memory bloat
        const maxPooledSize = 50 * 1024 * 1024 / 4; // 50MB in floats
        if (buffer.length <= maxPooledSize) {
            pool.push(buffer);
            
            // Keep pool size reasonable (max 5 buffers per type)
            if (pool.length > 5) {
                pool.shift();
            }
        }
    }

    /**
     * Cleanup after load completes or fails
     */
    cleanupLoad(filename) {
        const loadState = this.activeLoads.get(filename);
        if (!loadState) return;

        // Terminate worker
        if (loadState.worker) {
            loadState.worker.terminate();
            const workerIndex = this.workers.indexOf(loadState.worker);
            if (workerIndex > -1) {
                this.workers.splice(workerIndex, 1);
            }
        }

        // Clear chunks to free memory
        loadState.chunks = [];
        
        this.activeLoads.delete(filename);
        this.pendingUpdates.delete(filename);
    }

    /**
     * Cancel all active loads
     */
    cancelAll() {
        for (const [filename, loadState] of this.activeLoads.entries()) {
            if (loadState.worker) {
                loadState.worker.terminate();
            }
            loadState.reject(new Error('Load cancelled'));
        }
        
        this.activeLoads.clear();
        this.workers = [];
        this.loadQueue = [];
    }

    /**
     * Get load progress for a file
     */
    getProgress(filename) {
        const loadState = this.activeLoads.get(filename);
        if (!loadState) return null;

        return {
            received: loadState.receivedPoints,
            total: loadState.totalPoints,
            percentage: loadState.totalPoints > 0 
                ? (loadState.receivedPoints / loadState.totalPoints) * 100 
                : 0
        };
    }
}
