import api from "../api";

/**
 * Fetch individual pointcloud items (DRC files) for a given pointcloud ID.
 * Returns an array of item objects with signed .drc download URLs.
 * Returns an empty array if the endpoint is unavailable or has no items.
 */
export const fetchPointcloudItems = async (pointcloudId) => {
  try {
    const response = await api.get(`pointcloud-items/${pointcloudId}/`);
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
 * Fetch all processed pointclouds (lightweight — no GCS calls).
 * GET /api/pointclouds/ uses the lightweight PointcloudResponse constructor,
 * so downloadUrl / slamOutputDownloadUrl / processedDownloadUrls are all null.
 * Download URLs are fetched on-demand when the user actually opens a project
 * in the viewer (via fetchPointcloudItems).
 */
export const fetchProcessedPointclouds = async () => {
  try {
    const response = await api.get('/api/pointclouds/');
    console.log("[PointcloudService] Raw pointcloud list response:", response.data);

    const allPointclouds = Array.isArray(response.data)
      ? response.data
      : response.data?.data || [];

    // Return all pointclouds (processed or not) with lightweight metadata.
    // No GCS calls are made here — download URLs are null from the backend.
    return allPointclouds.map((pcl) => ({
      id: pcl.id,
      name: pcl.name,
      ownerId: pcl.ownerId,
      username: pcl.username,
      createdAt: pcl.createdAt,
      processed: pcl.processed,
      fileName: pcl.fileName,
      categories: pcl.categories || [],
      // These are intentionally null from the lightweight endpoint:
      // downloadUrl: null,
      // slamOutputDownloadUrl: null,
      // processedDownloadUrls: null,
    }));
  } catch (error) {
    console.error(
      "[PointcloudService] Failed to fetch pointcloud list:",
      error.message
    );
    throw error;
  }
};
