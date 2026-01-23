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
        };
      });

    return processedPointclouds;
  } catch (error) {
    throw error;
  }
};
