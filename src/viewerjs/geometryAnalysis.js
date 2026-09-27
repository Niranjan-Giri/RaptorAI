/**
 * geometryAnalysis.js
 * Point-cloud geometry analysis utilities for industrial scenes.
 *
 * Provides:
 *   • circleRANSAC(geometry)   – Fit a circle in the XY-plane via RANSAC
 *   • analyzeGeometry(geometry, filename) → { objectType, height, diameter, radius, volume, surface_area, bbox }
 */

// ─── Constants ─────────────────────────────────────────────────────────────
const RANSAC_ITERATIONS    = 200;
const RANSAC_INLIER_THRESH = 0.05;
const RANSAC_SAMPLE_LIMIT  = 8000;

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

    // Height = vertical extent (Z)
    const height = Math.max(sizeZ, 0.001);

    const objectType    = inferObjectType(filename);
    const isCylindrical = objectType === 'tank' || objectType === 'pipe';

    const circleResult = circleRANSAC(geometry);

    // Prefer RANSAC diameter; fall back to largest XY bbox extent
    const diameter = (circleResult && circleResult.inlierRatio > 0.15)
        ? circleResult.diameter
        : Math.max(sizeX, sizeY);
    const radius = diameter / 2;

    let volume       = null;
    let surface_area = null;

    if (objectType === 'tank') {
        // Closed vertical cylinder: V = πr²h,  SA = 2πr² + 2πrh
        volume       = Math.PI * radius * radius * height;
        surface_area = 2 * Math.PI * radius * (radius + height);
    } else if (objectType === 'pipe') {
        // Open-ended cylinder: V = πr²h,  SA = 2πrh (lateral only)
        volume       = Math.PI * radius * radius * height;
        surface_area = 2 * Math.PI * radius * height;
    } else if (objectType === 'pump') {
        // Bounding-box approximation
        volume       = sizeX * sizeY * sizeZ;
        surface_area = 2 * (sizeX * sizeY + sizeY * sizeZ + sizeX * sizeZ);
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
