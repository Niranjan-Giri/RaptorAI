import api from "../api";

/**
 * Fetch individual pointcloud items (DRC files) for a given pointcloud ID.
 * Returns an array of item objects with signed .drc download URLs.
 * Returns an empty array if the endpoint is unavailable or has no items.
 */
export const fetchPointcloudItems = async (pointcloudId) => {
  try {
    const response = await api.get(`/api/pointcloud-items/${pointcloudId}/`);
    const items = Array.isArray(response.data)
      ? response.data
      : response.data?.data || response.data?.items || [];
    return items;
  } catch (error) {
    // 404 is expected for older scans that don't have item records
    if (error.response?.status === 404) {
      console.warn(
        `[PointcloudService] No items endpoint for pointcloud ${pointcloudId} (404). Falling back to PLY URLs.`
      );
      return [];
    }
    console.error(
      `[PointcloudService] Failed to fetch items for pointcloud ${pointcloudId}:`,
      error.message
    );
    return [];
  }
};

/**
 * Fetch detail for a single pointcloud (including download URLs if available).
 */
export const fetchPointcloudDetail = async (pointcloudId) => {
  try {
    const response = await api.get(`/api/pointclouds/${pointcloudId}/`);
    return response.data;
  } catch (error) {
    console.error(
      `[PointcloudService] Failed to fetch detail for pointcloud ${pointcloudId}:`,
      error.message
    );
    return null;
  }
};

/**
 * Fetch all processed pointclouds (lightweight — no GCS calls).
 * GET /api/pointclouds/ uses the lightweight PointcloudResponse constructor,
 * so downloadUrl / slamOutputDownloadUrl / processedDownloadUrls are all null.
 * Download URLs are fetched on-demand when the user actually opens a project
 * in the viewer (via fetchPointcloudItems).
 */
/**
 * Fetch all processed pointclouds (lightweight — no GCS calls).
 * GET /api/pointclouds/ uses the lightweight PointcloudResponse constructor,
 * so downloadUrl / slamOutputDownloadUrl / processedDownloadUrls are all null.
 * Download URLs are fetched on-demand when the user actually opens a project
 * in the viewer (via fetchPointcloudItems).
 */
export const fetchProcessedPointclouds = async (username) => {
  try {
    const response = await api.get('/api/pointclouds/');
    console.log("[PointcloudService] Raw pointcloud list response:", response.data);

    const allPointclouds = Array.isArray(response.data)
      ? response.data
      : response.data?.data || [];

    // Get custom renamed names and deleted IDs stored locally for this user
    let customNames = {};
    let deletedIds = [];
    if (username) {
      try {
        customNames = JSON.parse(localStorage.getItem(`raptor_custom_project_names_${username}`) || '{}');
        deletedIds = JSON.parse(localStorage.getItem(`raptor_deleted_projects_${username}`) || '[]');
      } catch (e) {
        console.warn("[PointcloudService] Error reading local project persistence:", e);
      }
    }

    return allPointclouds
      .filter((pcl) => !deletedIds.includes(pcl.id))
      .map((pcl) => ({
        id: pcl.id,
        name: customNames[pcl.id] || pcl.name || pcl.fileName || `Project ${pcl.id}`,
        ownerId: pcl.ownerId,
        username: pcl.username,
        createdAt: pcl.createdAt,
        processed: pcl.processed,
        fileName: pcl.fileName,
        categories: pcl.categories || [],
      }));
  } catch (error) {
    console.error(
      "[PointcloudService] Failed to fetch pointcloud list:",
      error.message
    );
    throw error;
  }
};

/**
 * Rename a pointcloud project by ID.
 * Sends PATCH/PUT request to backend and stores custom name in localStorage so it remains after login.
 */
export const renamePointcloud = async (pointcloudId, newName, username) => {
  if (username && pointcloudId) {
    try {
      const key = `raptor_custom_project_names_${username}`;
      const existing = JSON.parse(localStorage.getItem(key) || '{}');
      existing[pointcloudId] = newName;
      localStorage.setItem(key, JSON.stringify(existing));
    } catch (e) {
      console.warn("[PointcloudService] Could not save renamed project name locally:", e);
    }
  }

  try {
    const response = await api.patch(`/api/pointclouds/${pointcloudId}/`, { name: newName });
    return response.data;
  } catch (error) {
    try {
      const putResp = await api.put(`/api/pointclouds/${pointcloudId}/`, { name: newName });
      return putResp.data;
    } catch (e) {
      console.warn(`[PointcloudService] Backend rename endpoint unhandled for project ${pointcloudId}. Using local persistence.`);
      return { id: pointcloudId, name: newName };
    }
  }
};

/**
 * Delete a pointcloud project by ID.
 * Sends DELETE request to backend and records deletion in localStorage so it remains deleted after login.
 */
export const deletePointcloud = async (pointcloudId, username) => {
  if (username && pointcloudId) {
    try {
      const key = `raptor_deleted_projects_${username}`;
      const existing = JSON.parse(localStorage.getItem(key) || '[]');
      if (!existing.includes(pointcloudId)) {
        existing.push(pointcloudId);
        localStorage.setItem(key, JSON.stringify(existing));
      }
    } catch (e) {
      console.warn("[PointcloudService] Could not save deleted project ID locally:", e);
    }
  }

  try {
    await api.delete(`/api/pointclouds/${pointcloudId}/`);
    return true;
  } catch (error) {
    console.warn(`[PointcloudService] Backend delete endpoint unhandled for project ${pointcloudId}. Using local persistence.`);
    return true;
  }
};
