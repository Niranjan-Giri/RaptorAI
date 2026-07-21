import api from "../api";

/**
 * Fetch individual pointcloud items (DRC files) for a given pointcloud ID.
 * Returns an array of item objects with signed .drc download URLs.
 * Returns an empty array if the endpoint is unavailable or has no items.
 */
export const fetchPointcloudItems = async (pointcloudId) => {
  try {
    const response = await api.get(`/pointcloud-items/${pointcloudId}/`);
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
 * Fetch all processed pointclouds.
 * For each scan, tries the /pointcloud-items/ endpoint first for .drc URLs.
 * Falls back to the existing processedDownloadUrls (PLY) if no items are found.
 */
export const fetchProcessedPointclouds = async () => {
  try {
    const response = await api.get('/pointclouds/');
    console.log("Raw pointcloud response:", response.data);

    const allPointclouds = Array.isArray(response.data)
      ? response.data
      : response.data?.data || [];

    const processedPointclouds = allPointclouds.filter(
      (pcl) => pcl.processed === true
    );

    // For each processed pointcloud, try to get .drc items first
    const enrichedPointclouds = await Promise.all(
      processedPointclouds.map(async (pcl) => {
        let files = [];

        try {
          // Attempt to fetch .drc items from the new endpoint
          const items = await fetchPointcloudItems(pcl.id);

          if (items.length > 0) {
            // Build files list from .drc items
            files = items.map((item) => ({
              name: item.name || item.object_name || item.filename || 'unnamed',
              url: item.download_url || item.signed_url || item.url,
            })).filter((f) => f.url); // Only include items that have a valid URL

            console.log(
              `[PointcloudService] Using ${files.length} DRC item(s) for '${pcl.name}'`
            );
          }
        } catch (itemsError) {
          console.warn(
            `[PointcloudService] Error fetching items for '${pcl.name}', falling back to PLY:`,
            itemsError.message
          );
        }

        // Fallback: use existing processedDownloadUrls (PLY) if no DRC items found
        if (files.length === 0 && pcl.processedDownloadUrls) {
          const flattenedUrls = Object.entries(
            pcl.processedDownloadUrls
          ).map(([category, urls]) => ({
            category,
            urls: Array.isArray(urls) ? urls : [urls],
          }));

          files = Object.entries(pcl.processedDownloadUrls).flatMap(
            ([category, urls]) => {
              const urlList = Array.isArray(urls) ? urls : [urls];
              return urlList.map((url, index) => ({
                name:
                  urlList.length > 1
                    ? `${category}${index + 1}`
                    : category,
                url: url,
              }));
            }
          );

          console.log(
            `[PointcloudService] Falling back to ${files.length} PLY URL(s) for '${pcl.name}'`
          );

          return {
            id: pcl.id,
            name: pcl.name,
            description: pcl.description || "Processed pointcloud project",
            createdAt: pcl.createdAt,
            uploadedAt: pcl.uploadedAt,
            thumbnail: pcl.thumbnail,
            allUrls: flattenedUrls,
            processedDownloadUrls: pcl.processedDownloadUrls,
            files,
          };
        }

        return {
          id: pcl.id,
          name: pcl.name,
          description: pcl.description || "Processed pointcloud project",
          createdAt: pcl.createdAt,
          uploadedAt: pcl.uploadedAt,
          thumbnail: pcl.thumbnail,
          allUrls: [],
          processedDownloadUrls: pcl.processedDownloadUrls || {},
          files,
        };
      })
    );

    return enrichedPointclouds;
  } catch (error) {
    console.error(
      "[PointcloudService] Failed to fetch processed pointclouds:",
      error.message
    );
    throw error;
  }
};
