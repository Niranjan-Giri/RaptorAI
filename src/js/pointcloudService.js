import api from "../api";

export const fetchProcessedPointclouds = async () => {
  try {
    const response = await api.get('/pointclouds/');
    // Filter for processed pointclouds and map to project format
    const allPointclouds = Array.isArray(response.data)
      ? response.data
      : response.data?.data || [];
    const processedPointclouds = allPointclouds
      .filter((pcl) => pcl.processed === true && pcl.processedDownloadUrls)
      .map((pcl) => {
        const flattenedUrls = Object.entries(
          pcl.processedDownloadUrls || {},
        ).map(([category, urls]) => ({
          category,
          urls: Array.isArray(urls) ? urls : [urls],
        }));

        return {
          id: pcl.id,
          name: pcl.name,
          description: pcl.description || "Processed pointcloud project",
          createdAt: pcl.createdAt,
          uploadedAt: pcl.uploadedAt,
          thumbnail: pcl.thumbnail,
          allUrls: flattenedUrls, // All URLs grouped by category
          processedDownloadUrls: pcl.processedDownloadUrls, // Keep original for reference
          
          // Helper: Flattened list of files with friendly names
          // e.g. "bag1", "bag2" instead of "bag" array
          files: Object.entries(pcl.processedDownloadUrls || {}).flatMap(([category, urls]) => {
              const urlList = Array.isArray(urls) ? urls : [urls];
              return urlList.map((url, index) => ({
                  name: urlList.length > 1 ? `${category}${index + 1}` : category,
                  url: url
              }));
          })
        };
      });

    return processedPointclouds;
  } catch (error) {
    throw error;
  }
};
