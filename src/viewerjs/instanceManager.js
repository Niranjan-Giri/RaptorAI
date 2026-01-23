/**
 * Instance Manager for WebGL2 Instanced Rendering
 * Detects and optimizes repeated geometries using GPU instancing
 */

import * as THREE from 'three';

export class InstanceManager {
    constructor(scene) {
        this.scene = scene;
        this.instances = new Map(); // geometryHash -> InstancedMesh
        this.objectToInstance = new Map(); // original object -> {instancedMesh, instanceId}
        this.enabled = true;
        
        // Minimum instances required to use instancing (avoid overhead for unique objects)
        this.minInstanceCount = 2;
        
        // Geometry similarity threshold
        this.similarityThreshold = 0.95;
    }
    
    /**
     * Enable or disable instancing
     */
    setEnabled(enabled) {
        this.enabled = enabled;
        if (!enabled) {
            this.clearAllInstances();
        }
    }
    
    /**
     * Analyze scene and convert repeated objects to instanced rendering
     */
    optimizeScene(loadedFiles) {
        if (!this.enabled) return;
        
        const startTime = performance.now();
        
        // Group objects by geometry signature
        const geometryGroups = this.groupByGeometry(loadedFiles);
        
        let optimizedCount = 0;
        
        // Convert groups with multiple instances
        for (const [signature, group] of geometryGroups.entries()) {
            if (group.objects.length >= this.minInstanceCount) {
                this.createInstancedMesh(signature, group);
                optimizedCount += group.objects.length;
            }
        }
        
        const elapsed = performance.now() - startTime;
        
        if (optimizedCount > 0) {
            console.log(`[InstanceManager] Optimized ${optimizedCount} objects into ${geometryGroups.size} instanced meshes in ${elapsed.toFixed(1)}ms`);
        }
        
        return optimizedCount;
    }
    
    /**
     * Group objects by geometry similarity
     */
    groupByGeometry(loadedFiles) {
        const groups = new Map();
        
        for (const [filename, fileData] of loadedFiles.entries()) {
            if (!fileData.object || !fileData.visible || !fileData.geometry) continue;
            
            // Skip already instanced objects
            if (this.objectToInstance.has(fileData.object)) continue;
            
            const signature = this.getGeometrySignature(fileData.geometry);
            
            if (!groups.has(signature)) {
                groups.set(signature, {
                    geometry: fileData.geometry,
                    material: fileData.object.material,
                    objects: [],
                    type: fileData.object.type
                });
            }
            
            groups.get(signature).objects.push({
                object: fileData.object,
                filename,
                fileData
            });
        }
        
        return groups;
    }
    
    /**
     * Generate a signature for geometry to detect duplicates
     */
    getGeometrySignature(geometry) {
        const positions = geometry.attributes.position;
        const count = positions.count;
        
        // For point clouds, use vertex count and bounding box as signature
        geometry.computeBoundingBox();
        const bbox = geometry.boundingBox;
        
        const size = bbox.getSize(new THREE.Vector3());
        
        // Create signature: vertexCount_sizeX_sizeY_sizeZ
        const signature = `${count}_${size.x.toFixed(3)}_${size.y.toFixed(3)}_${size.z.toFixed(3)}`;
        
        return signature;
    }
    
    /**
     * Check if two geometries are similar enough to instance
     */
    geometriesAreSimilar(geom1, geom2) {
        const count1 = geom1.attributes.position.count;
        const count2 = geom2.attributes.position.count;
        
        // Must have same vertex count
        if (count1 !== count2) return false;
        
        // Check bounding box similarity
        geom1.computeBoundingBox();
        geom2.computeBoundingBox();
        
        const size1 = geom1.boundingBox.getSize(new THREE.Vector3());
        const size2 = geom2.boundingBox.getSize(new THREE.Vector3());
        
        const sizeRatio = Math.min(size1.length(), size2.length()) / Math.max(size1.length(), size2.length());
        
        return sizeRatio >= this.similarityThreshold;
    }
    
    /**
     * Create an InstancedMesh from a group of similar objects
     */
    createInstancedMesh(signature, group) {
        const { geometry, material, objects, type } = group;
        
        const count = objects.length;
        
        // Create instanced mesh
        let instancedMesh;
        if (type === 'Points') {
            // For point clouds, create instanced points (custom implementation)
            instancedMesh = this.createInstancedPoints(geometry, material, count);
        } else {
            // For meshes, use THREE.InstancedMesh
            instancedMesh = new THREE.InstancedMesh(geometry, material, count);
        }
        
        instancedMesh.castShadow = true;
        instancedMesh.receiveShadow = true;
        
        // Set up instance transforms
        const matrix = new THREE.Matrix4();
        objects.forEach((item, index) => {
            const { object } = item;
            
            // Get world matrix from original object
            object.updateMatrixWorld();
            matrix.copy(object.matrixWorld);
            
            instancedMesh.setMatrixAt(index, matrix);
            
            // Track the mapping
            this.objectToInstance.set(object, {
                instancedMesh,
                instanceId: index,
                signature
            });
            
            // Remove original object from scene
            this.scene.remove(object);
        });
        
        // Update instance matrices
        instancedMesh.instanceMatrix.needsUpdate = true;
        
        // Add instanced mesh to scene
        this.scene.add(instancedMesh);
        
        // Store instance
        this.instances.set(signature, {
            instancedMesh,
            objects: objects,
            count
        });
        
        console.log(`[InstanceManager] Created instanced mesh for ${count} objects (signature: ${signature})`);
    }
    
    /**
     * Create instanced points (for point clouds)
     * Note: THREE.js doesn't have InstancedPoints, so we use custom shader approach
     */
    createInstancedPoints(geometry, material, count) {
        // Clone geometry to avoid modifying original
        const instancedGeometry = geometry.clone();
        
        // Add instance matrix attribute (4x4 matrix = 16 floats per instance)
        const instanceMatrices = new Float32Array(count * 16);
        
        // Initialize with identity matrices
        for (let i = 0; i < count; i++) {
            const offset = i * 16;
            instanceMatrices[offset + 0] = 1;
            instanceMatrices[offset + 5] = 1;
            instanceMatrices[offset + 10] = 1;
            instanceMatrices[offset + 15] = 1;
        }
        
        // Note: Full instanced points implementation requires custom shader
        // For now, we'll use InstancedMesh approach which works for both points and meshes
        const instancedMesh = new THREE.InstancedMesh(instancedGeometry, material, count);
        
        return instancedMesh;
    }
    
    /**
     * Update instance transform for a specific object
     */
    updateInstance(object) {
        const instanceData = this.objectToInstance.get(object);
        if (!instanceData) return false;
        
        const { instancedMesh, instanceId } = instanceData;
        
        object.updateMatrixWorld();
        instancedMesh.setMatrixAt(instanceId, object.matrixWorld);
        instancedMesh.instanceMatrix.needsUpdate = true;
        
        return true;
    }
    
    /**
     * Remove an instance
     */
    removeInstance(object) {
        const instanceData = this.objectToInstance.get(object);
        if (!instanceData) return false;
        
        const { instancedMesh, instanceId, signature } = instanceData;
        const instanceGroup = this.instances.get(signature);
        
        if (!instanceGroup) return false;
        
        // Hide this instance by setting scale to 0
        const matrix = new THREE.Matrix4();
        matrix.makeScale(0, 0, 0);
        instancedMesh.setMatrixAt(instanceId, matrix);
        instancedMesh.instanceMatrix.needsUpdate = true;
        
        // Remove from tracking
        this.objectToInstance.delete(object);
        
        return true;
    }
    
    /**
     * Clear all instances and restore original objects
     */
    clearAllInstances() {
        // Restore original objects to scene
        for (const [object, instanceData] of this.objectToInstance.entries()) {
            this.scene.add(object);
        }
        
        // Remove instanced meshes from scene
        for (const [signature, instanceGroup] of this.instances.entries()) {
            this.scene.remove(instanceGroup.instancedMesh);
            instanceGroup.instancedMesh.dispose();
        }
        
        this.instances.clear();
        this.objectToInstance.clear();
    }
    
    /**
     * Get statistics about instancing
     */
    getStats() {
        let totalInstances = 0;
        let totalObjects = 0;
        
        for (const [signature, group] of this.instances.entries()) {
            totalInstances++;
            totalObjects += group.count;
        }
        
        return {
            instancedMeshCount: totalInstances,
            totalInstancedObjects: totalObjects,
            drawCallReduction: totalObjects > 0 ? totalObjects - totalInstances : 0
        };
    }
}
