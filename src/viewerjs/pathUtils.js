function extractPathAndSuffix(rawPath) {
    const match = String(rawPath || '').match(/^([^?#]*)(.*)$/);
    return {
        pathPart: match ? match[1] : String(rawPath || ''),
        suffix: match ? match[2] : ''
    };
}

function toPublicAssetPath(localPath) {
    if (!localPath) return '';

    const normalized = localPath.replace(/\\/g, '/');
    const lower = normalized.toLowerCase();

    if (lower.startsWith('/public/')) {
        return normalized.slice('/public'.length);
    }

    if (lower.startsWith('public/')) {
        return `/${normalized.slice('public/'.length)}`;
    }

    const publicSegment = '/public/';
    const index = lower.lastIndexOf(publicSegment);
    if (index >= 0) {
        return normalized.slice(index + '/public'.length);
    }

    const parts = normalized.split('/').filter(Boolean);
    if (parts.length > 0) {
        return `/${parts[parts.length - 1]}`;
    }

    return normalized;
}

export function normalizeViewerFileUrl(inputUrl) {
    if (typeof inputUrl !== 'string') return inputUrl;

    const trimmed = inputUrl.trim();
    if (!trimmed) return trimmed;

    const { pathPart, suffix } = extractPathAndSuffix(trimmed);
    let normalizedPath = pathPart.replace(/\\/g, '/');

    if (/^(https?:|blob:|data:|\/\/)/i.test(normalizedPath)) {
        return `${normalizedPath}${suffix}`;
    }

    if (/^file:\/\//i.test(normalizedPath)) {
        try {
            const fileUrl = new URL(normalizedPath);
            const decodedPath = decodeURIComponent(fileUrl.pathname || '');
            return `${toPublicAssetPath(decodedPath)}${suffix}`;
        } catch (error) {
            normalizedPath = normalizedPath.replace(/^file:\/\//i, '');
            return `${toPublicAssetPath(normalizedPath)}${suffix}`;
        }
    }

    if (/^[a-zA-Z]:\//.test(normalizedPath) || /^\/[a-zA-Z]:\//.test(normalizedPath)) {
        return `${toPublicAssetPath(normalizedPath)}${suffix}`;
    }

    if (normalizedPath.startsWith('/')) {
        return `${normalizedPath}${suffix}`;
    }

    if (normalizedPath.startsWith('./') || normalizedPath.startsWith('../')) {
        return `${normalizedPath}${suffix}`;
    }

    return `/${normalizedPath}${suffix}`;
}

export function inferFilenameFromUrl(fileUrl, fallback = 'model.ply') {
    if (!fileUrl || typeof fileUrl !== 'string') return fallback;

    const normalized = normalizeViewerFileUrl(fileUrl);
    const { pathPart } = extractPathAndSuffix(normalized);
    const pieces = pathPart.split('/').filter(Boolean);
    if (pieces.length === 0) return fallback;

    return decodeURIComponent(pieces[pieces.length - 1]);
}