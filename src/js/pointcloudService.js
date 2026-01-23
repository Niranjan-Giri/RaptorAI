import api from "../api";

/**
 * Fetch and transform processed pointclouds from the API
 * @returns {Promise<Array>} Array of processed pointcloud projects
 */
export const fetchProcessedPointclouds = async () => {
  try {
    const response = await api.get('/user/pointclouds/processed/');
    console.log('[Profile] Pointclouds response:', response.data);
    
    // Filter for processed pointclouds and map to project format
    const allPointclouds = Array.isArray(response.data) ? response.data : response.data?.data || [];
    const processedPointclouds = allPointclouds
      .filter(pcl => pcl.processed === true && pcl.processedDownloadUrls)
      .map(pcl => {
        // Flatten processedDownloadUrls object into array of {category, urls}
        const flattenedUrls = Object.entries(pcl.processedDownloadUrls || {}).map(([category, urls]) => ({
          category,
          urls: Array.isArray(urls) ? urls : [urls]
        }));
        
        return {
          id: pcl.id,
          name: pcl.name,
          description: pcl.description || 'Processed pointcloud project',
          createdAt: pcl.createdAt,
          uploadedAt: pcl.uploadedAt,
          thumbnail: pcl.thumbnail,
          allUrls: flattenedUrls, // All URLs grouped by category
          processedDownloadUrls: pcl.processedDownloadUrls // Keep original for reference
        };
      });
    
    return processedPointclouds;
  } catch (error) {
    console.error('[Profile] Error fetching processed pointclouds:', error);
    throw error;
  }
};
