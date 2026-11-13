/**
 * geometryAnalysis.js
 * Point-cloud geometry analysis utilities for industrial scenes.
 *
 * Provides:
 *   • circleRANSAC(geometry)   – Fit a circle in the XY-plane via RANSAC
 *   • analyzeGeometry(geometry, filename) → { objectType, height, diameter, radius, volume, surface_area, bbox }
 */

// ─── Constants ─────────────────────────────────────────────────────────────
const RANSAC_ITERATIONS    = 400;   // more iterations → better convergence
const RANSAC_INLIER_THRESH = 0.05;  // default; overridden adaptively per-object
const RANSAC_SAMPLE_LIMIT  = 10000; // slightly more points for accuracy

// ─── Object-type keywords ──────────────────────────────────────────────────
const TYPE_KEYWORDS = {
    tank: ['tank', 'vessel', 'silo', 'container', 'storage'],
    pipe: ['pipe', 'tube', 'duct', 'conduit', 'line', 'hose'],
    pump: ['pump', 'motor', 'compressor', 'blower', 'fan'],
};

/** Infer industrial object type from filename. */
export function inferObjectType(filename) {
    if (!filename) return 'unknown';
    const lower = filename.toLowerCase();
    for (const [type, keywords] of Object.entries(TYPE_KEYWORDS)) {
        if (keywords.some(k => lower.includes(k))) return type;
    }
    return 'unknown';
}

/**
 * Circle RANSAC – fit a 2-D circle to the XY projection of a point cloud.
 * Returns { cx, cy, radius, diameter, inlierRatio } or null.
 */
export function circleRANSAC(geometry, opts = {}) {
    const iterations  = opts.iterations      || RANSAC_ITERATIONS;
    const threshold   = opts.inlierThreshold || RANSAC_INLIER_THRESH;
    const sampleLimit = opts.sampleLimit     || RANSAC_SAMPLE_LIMIT;

    if (!geometry?.attributes?.position) return null;
    const posAttr  = geometry.attributes.position;
    const totalPts = posAttr.count;
    if (totalPts < 3) return null;

    const stride = Math.max(1, Math.floor(totalPts / sampleLimit));
    const xs = [], ys = [];
    for (let i = 0; i < totalPts; i += stride) {
        xs.push(posAttr.getX(i));
        ys.push(posAttr.getY(i));
    }
    const n = xs.length;
    if (n < 3) return null;

    let bestInliers = -1, bestCx = 0, bestCy = 0, bestR = 1;

    for (let iter = 0; iter < iterations; iter++) {
        let i0 = (Math.random() * n) | 0;
        let i1 = (Math.random() * n) | 0;
        let i2 = (Math.random() * n) | 0;
        if (i1 === i0) i1 = (i1 + 1) % n;
        if (i2 === i0 || i2 === i1) i2 = (i2 + 2) % n;

        const res = circleFrom3Points(xs[i0], ys[i0], xs[i1], ys[i1], xs[i2], ys[i2]);
        if (!res || res.r <= 0 || res.r > 500) continue;

        let inliers = 0;
        for (let j = 0; j < n; j++) {
            const dx = xs[j] - res.cx, dy = ys[j] - res.cy;
            if (Math.abs(Math.sqrt(dx*dx + dy*dy) - res.r) < threshold) inliers++;
        }
        if (inliers > bestInliers) { bestInliers = inliers; bestCx = res.cx; bestCy = res.cy; bestR = res.r; }
    }

    if (bestInliers < 3) return null;

    // Collect inliers and refine with least-squares
    const iXs = [], iYs = [];
    for (let j = 0; j < n; j++) {
        const dx = xs[j] - bestCx, dy = ys[j] - bestCy;
        if (Math.abs(Math.sqrt(dx*dx + dy*dy) - bestR) < threshold) { iXs.push(xs[j]); iYs.push(ys[j]); }
    }
    const refined = algebraicCircleFit(iXs, iYs);
    if (refined && refined.r > 0) { bestCx = refined.cx; bestCy = refined.cy; bestR = refined.r; }

    return { cx: bestCx, cy: bestCy, radius: bestR, diameter: bestR * 2, inlierRatio: bestInliers / n };
}

function circleFrom3Points(x1,y1, x2,y2, x3,y3) {
    const ax = x2-x1, ay = y2-y1, bx = x3-x1, by = y3-y1;
    const D = 2*(ax*by - ay*bx);
    if (Math.abs(D) < 1e-10) return null;
    const ux = (by*(ax*ax+ay*ay) - ay*(bx*bx+by*by))/D;
    const uy = (ax*(bx*bx+by*by) - bx*(ax*ax+ay*ay))/D;
    return { cx: x1+ux, cy: y1+uy, r: Math.sqrt(ux*ux+uy*uy) };
}

function algebraicCircleFit(xs, ys) {
    const n = xs.length; if (n < 3) return null;
    let Sx=0,Sy=0,Sx2=0,Sy2=0,Sxy=0,Sx3=0,Sy3=0,Sx2y=0,Sxy2=0;
    for (let i=0;i<n;i++) {
        const x=xs[i],y=ys[i],x2=x*x,y2=y*y;
        Sx+=x;Sy+=y;Sx2+=x2;Sy2+=y2;Sxy+=x*y;Sx3+=x2*x;Sy3+=y2*y;Sx2y+=x2*y;Sxy2+=x*y2;
    }
    const A = [[Sx2,Sxy,Sx],[Sxy,Sy2,Sy],[Sx,Sy,n]];
    const b = [-(Sx3+Sxy2),-(Sy3+Sx2y),-(Sx2+Sy2)];
    const sol = solve3x3(A,b);
    if (!sol) return null;
    const [D,E,F] = sol;
    const cx = -D/2, cy = -E/2, r = Math.sqrt(cx*cx+cy*cy-F);
    return (isFinite(r) && r > 0) ? {cx,cy,r} : null;
}

function solve3x3(A,b) {
    const M = A.map((row,i) => [...row,b[i]]);
    for (let col=0;col<3;col++) {
        let maxRow=col;
        for (let row=col+1;row<3;row++) if (Math.abs(M[row][col])>Math.abs(M[maxRow][col])) maxRow=row;
        [M[col],M[maxRow]]=[M[maxRow],M[col]];
        if (Math.abs(M[col][col])<1e-12) return null;
        for (let row=col+1;row<3;row++) {
            const f=M[row][col]/M[col][col];
            for (let k=col;k<=3;k++) M[row][k]-=f*M[col][k];
        }
    }
    const x=new Array(3).fill(0);
    for (let i=2;i>=0;i--) { let s=M[i][3]; for (let j=i+1;j<3;j++) s-=M[i][j]*x[j]; x[i]=s/M[i][i]; }
    return x;
}

/**
 * Full geometry analysis for an industrial scene object.
 * @param {THREE.BufferGeometry} geometry
 * @param {string} filename
 */
export function analyzeGeometry(geometry, filename) {
    geometry.computeBoundingBox();
    const bbox   = geometry.boundingBox;
    const minArr = bbox.min.toArray();
    const maxArr = bbox.max.toArray();
    const sizeX  = maxArr[0] - minArr[0];
    const sizeY  = maxArr[1] - minArr[1];
    const sizeZ  = maxArr[2] - minArr[2];
    const center = [(minArr[0]+maxArr[0])/2, (minArr[1]+maxArr[1])/2, (minArr[2]+maxArr[2])/2];

    const objectType = inferObjectType(filename);

    // ── Adaptive RANSAC threshold ────────────────────────────────────────────
    // Use 2% of the estimated radius from the bbox so both small and large
    // objects get reasonable inlier counts (a fixed 5 cm is too tight for
    // a 3-metre-radius tank and too loose for a 20-cm pipe).
    const estimatedRadius = Math.max(sizeX, sizeY) / 2;
    const adaptiveThresh  = Math.max(0.03, estimatedRadius * 0.04); // 4% of radius, min 3 cm

    const circleResult = circleRANSAC(geometry, {
        iterations:      RANSAC_ITERATIONS,
        inlierThreshold: adaptiveThresh,
        sampleLimit:     RANSAC_SAMPLE_LIMIT,
    });

    // ── Diameter selection ───────────────────────────────────────────────────
    // Accept RANSAC if it found a reasonable circle (inlier ratio > 8%).
    // Fall back to the largest XY bounding-box extent (which equals the
    // true diameter for a perfect circular cross-section).
    const ransacOk = circleResult && circleResult.inlierRatio > 0.08;
    const diameter = ransacOk ? circleResult.diameter : Math.max(sizeX, sizeY);
    const radius   = diameter / 2;

    // ── Height selection ─────────────────────────────────────────────────────
    // For a vertical cylinder the height axis is Z (scan-space up).
    // But the Z bbox may include ground-level offset, so use the raw extent.
    // If the object is taller in X or Y (horizontal tank / pipe), pick that.
    // We define "height" as the dimension that makes the cylinder taller than wide.
    let height;
    if (objectType === 'tank' || objectType === 'pipe') {
        // Primary candidate: Z-axis (vertical installation)
        // Cross-check: if sizeZ < diameter it's likely a horizontal tank, use longest axis
        if (sizeZ >= diameter * 0.5) {
            // Vertical: height = sizeZ
            height = sizeZ;
        } else {
            // Horizontal: height = longest of X / Y
            height = Math.max(sizeX, sizeY, sizeZ);
        }
    } else {
        height = Math.max(sizeX, sizeY, sizeZ);
    }
    height = Math.max(height, 0.001);

    // ── Volume & surface area ────────────────────────────────────────────────
    let volume       = null;
    let surface_area = null;

    if (objectType === 'tank') {
        // Closed vertical cylinder: V = πr²h,  SA = 2πr(r + h)
        volume       = Math.PI * radius * radius * height;
        surface_area = 2 * Math.PI * radius * (radius + height);
    } else if (objectType === 'pipe') {
        // Open-ended cylinder: V = πr²h,  SA = 2πrh
        volume       = Math.PI * radius * radius * height;
        surface_area = 2 * Math.PI * radius * height;
    } else if (objectType === 'pump') {
        volume       = sizeX * sizeY * sizeZ;
        surface_area = 2 * (sizeX * sizeY + sizeY * sizeZ + sizeX * sizeZ);
    }

    // ── Debug diagnostics (tanks only) ──────────────────────────────────────
    if (objectType === 'tank') {
        const ranAns = ransacOk
            ? `RANSAC ✓ (r=${circleResult.radius.toFixed(3)}m, inliers=${(circleResult.inlierRatio*100).toFixed(1)}%, thresh=${adaptiveThresh.toFixed(3)}m)`
            : `RANSAC ✗ → bbox fallback (inlierRatio=${circleResult ? (circleResult.inlierRatio*100).toFixed(1)+'%' : 'n/a'}, thresh=${adaptiveThresh.toFixed(3)}m)`;
        console.log(
            `%c[GeoAnalysis] TANK: ${filename}`,
            'color:#00e5ff; font-weight:bold',
            `\n  BBox:     X=${sizeX.toFixed(3)}m  Y=${sizeY.toFixed(3)}m  Z=${sizeZ.toFixed(3)}m` +
            `\n  Diameter: ${diameter.toFixed(3)} m  (${ranAns})` +
            `\n  Height:   ${height.toFixed(3)} m` +
            `\n  Volume:   ${volume?.toFixed(3)} m³` +
            `\n  Surf.Area:${surface_area?.toFixed(3)} m²`
        );
    }

    return {
        objectType,
        height,
        diameter,
        radius,
        volume,
        surface_area,
        ransacInlierRatio: circleResult?.inlierRatio ?? null,
        bbox: { min: minArr, max: maxArr, size: [sizeX, sizeY, sizeZ], center },
    };
}

// ── Scene-load metric cache ────────────────────────────────────────────────
const _geoCache = new Map();

/** Invalidate analysis cache (call when the scene is reloaded). */
export function clearGeoAnalysisCache() { _geoCache.clear(); }

/** Drop a single file so it is re-analyzed after geometry changes. */
export function invalidateGeoAnalysis(filename) {
    if (filename) _geoCache.delete(filename);
}

/**
 * Analyze one loaded file and store volume / area / height / diameter.
 * @param {string} filename
 * @param {{ geometry?: object }} fileData
 */
export function analyzeLoadedFile(filename, fileData) {
    if (!filename || !fileData?.geometry) return null;
    try {
        const result = analyzeGeometry(fileData.geometry, filename);
        _geoCache.set(filename, result);
        return result;
    } catch (e) {
        console.warn('[GeoAnalysis] Failed for', filename, e);
        return null;
    }
}

/** Cached analysis, or compute now if missing. */
export function getCachedAnalysis(filename, fileData) {
    if (!filename) return null;
    if (_geoCache.has(filename)) return _geoCache.get(filename);
    return analyzeLoadedFile(filename, fileData);
}

/**
 * Pre-calculate volume, surface area, height, and diameter for every
 * loaded object. Used when a scene (or cached project) finishes loading.
 * Work is chunked so RANSAC does not freeze the UI.
 * @param {Map<string, { geometry?: object }>} loadedFiles
 */
export function precomputeSceneMetrics(loadedFiles) {
    if (!loadedFiles || typeof loadedFiles.forEach !== 'function') return;

    const pending = [];
    loadedFiles.forEach((fileData, filename) => {
        if (fileData?.geometry && !_geoCache.has(filename)) {
            pending.push([filename, fileData]);
        }
    });
    if (pending.length === 0) return;

    let index = 0;
    const step = () => {
        const start = performance.now();
        while (index < pending.length && performance.now() - start < 12) {
            const [filename, fileData] = pending[index++];
            analyzeLoadedFile(filename, fileData);
        }
        if (index < pending.length) {
            if (typeof requestIdleCallback === 'function') {
                requestIdleCallback(step, { timeout: 80 });
            } else {
                setTimeout(step, 0);
            }
        } else {
            console.log(`[GeoAnalysis] Precomputed metrics for ${pending.length} object(s)`);
        }
    };
    step();
}
