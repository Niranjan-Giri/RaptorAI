import React, { useEffect, useState } from "react";
import { useSearchParams, useLocation } from "react-router-dom";
import "./style.css";
import { initializeApp } from "../../viewerjs/app.entry.js";
import { EXAMPLE_PLY_FILES } from "../../components/username.jsx";
import api from "../../api";
const Viewer = () => {
  const location = useLocation();
  const { projectName, processedDownloadUrls } = location.state || {};
  const [searchParams] = useSearchParams();
  const [isLoading, setIsLoading] = useState(true);
  
  useEffect(() => {
    // Small delay to ensure canvas-container is in the DOM
    const initTimer = setTimeout(() => {
      const canvasContainer = document.getElementById('canvas-container');
      console.log('[Viewer] Canvas container check:', {
        found: !!canvasContainer,
        visible: canvasContainer?.offsetHeight > 0
      });
      
      if (!canvasContainer) {
        return;
      }
      
      // Initialize app after component mounts and canvas-container exists
      const app = initializeApp();
      window.__app = app;
      
      if (!app) {
        return;
      }
      
      const checkAndSetupUI = setInterval(() => {
        const btnOrbit = document.getElementById('btn-orbit');
        
        if (app && btnOrbit && app.ui) {
          if (typeof app.ui.setupMenuControls === 'function') {
            app.ui.setupMenuControls();
          }
          
          // Check if a specific example was selected via query parameters
          const exampleName = searchParams.get('example');
          const pointcloudId = searchParams.get('pointcloudId');
          
          if (exampleName && EXAMPLE_PLY_FILES.some(ex => ex.name === exampleName)) {
            const selectedPlyUrl = EXAMPLE_PLY_FILES.find(ex => ex.name === exampleName).plyUrl;            
            // Show loading screen
            setIsLoading(true);
            
            // Replace default PLY files with the selected one
            app.plyFiles = [selectedPlyUrl];
            
            // Clear previously loaded files from the scene
            app.loadedFiles.forEach((fileData) => {
              if (fileData.object && fileData.object.parent) {
                fileData.object.parent.remove(fileData.object);
              }
              if (fileData.geometry) {
                fileData.geometry.dispose();
              }
              if (fileData.object && fileData.object.material) {
                if (Array.isArray(fileData.object.material)) {
                  fileData.object.material.forEach(m => m.dispose());
                } else {
                  fileData.object.material.dispose();
                }
              }
            });
            app.loadedFiles.clear();
            
            // Cancel any pending loads
            if (app.loaderManager && typeof app.loaderManager.cancelAll === 'function') {
              app.loaderManager.cancelAll();
            }
            
            // Load the selected PLY file
            if (app.sceneManager && typeof app.sceneManager.loadAllPLYFiles === 'function') {
              app.sceneManager.loadAllPLYFiles();
              
              // Frame the objects after a short delay to ensure they're loaded
              setTimeout(() => {
                if (app.sceneManager && typeof app.sceneManager.frameAllObjects === 'function') {
                  console.log('[Viewer] Framing loaded objects');
                  app.sceneManager.frameAllObjects(1000);
                }
                // Hide loading screen when done
                setIsLoading(false);
              }, 500);
            }
          }else if(processedDownloadUrls && Object.keys(processedDownloadUrls).length > 0){
            // Load from processed download URLs (project clicked)
            setIsLoading(true);
                        
            // Transform processedDownloadUrls into labeled array
            const plyFilesWithNames = [];
            const plyUrls = [];
            
            Object.entries(processedDownloadUrls).forEach(([category, urls]) => {
              const urlArray = Array.isArray(urls) ? urls : [urls];
              urlArray.forEach((url, index) => {
                const name = urlArray.length > 1 ? `${category} ${index}` : category;
                plyFilesWithNames.push({ name, url });
                plyUrls.push(url);
              });
            });
                        
            // Set the PLY files and names
            app.plyFiles = plyUrls;
            app.plyFileNames = plyFilesWithNames.map(f => f.name);
            
            // Clear previously loaded files from the scene
            app.loadedFiles.forEach((fileData) => {
              if (fileData.object && fileData.object.parent) {
                fileData.object.parent.remove(fileData.object);
              }
              if (fileData.geometry) {
                fileData.geometry.dispose();
              }
              if (fileData.object && fileData.object.material) {
                if (Array.isArray(fileData.object.material)) {
                  fileData.object.material.forEach(m => m.dispose());
                } else {
                  fileData.object.material.dispose();
                }
              }
            });
            app.loadedFiles.clear();
            
            // Cancel any pending loads
            if (app.loaderManager && typeof app.loaderManager.cancelAll === 'function') {
              app.loaderManager.cancelAll();
            }
            
            // Load all PLY files
            if (app.sceneManager && typeof app.sceneManager.loadAllPLYFiles === 'function') {
              app.sceneManager.loadAllPLYFiles();
              
              // Frame the objects after a short delay
              setTimeout(() => {
                if (app.sceneManager && typeof app.sceneManager.frameAllObjects === 'function') {
                  console.log('[Viewer] Framing loaded objects');
                  app.sceneManager.frameAllObjects(1000);
                }
                setIsLoading(false);
              }, 500);
            }
          } 
          else {
            // No example selected, hide loading screen
            setIsLoading(false);
          }
          clearInterval(checkAndSetupUI);
        }
      }, 100);
    }, 50);

    return () => {
      clearTimeout(initTimer);
      const app = window.__app;
      const canvasContainer = document.getElementById('canvas-container');
      if (app && app.renderer && canvasContainer) {
        if (canvasContainer.contains(app.renderer.domElement)) {
          canvasContainer.removeChild(app.renderer.domElement);
        }
      }
      // Also check body as fallback
      if (app && app.renderer && document.body.contains(app.renderer.domElement)) {
        document.body.removeChild(app.renderer.domElement);
      }
    };
  }, []);

  return (
    <div className="viewer-wrapper">
      <div id="canvas-container"></div>
      
      {/* Loading Screen */}
      {isLoading && (
        <div className="loading-screen">
          <div className="loading-spinner"></div>
          <p>Loading...</p>
        </div>
      )}
      
      <div id="icon-toolbar">
        <button className="icon-btn active" id="btn-orbit" title="Orbit">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <circle cx="12" cy="12" r="10" />
            <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" />
            <path d="M2 12h20" />
          </svg>
        </button>
        <button className="icon-btn" id="btn-pan" title="Pan">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <path d="M5 9l-3 3 3 3M9 5l3-3 3 3M15 19l-3 3-3-3M19 9l3 3-3 3" />
            <path d="M2 12h20M12 2v20" />
          </svg>
        </button>
        <button className="icon-btn" id="btn-select" title="Select">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <path d="M3 3l7.07 16.97 2.51-7.39 7.39-2.51L3 3z" />
          </svg>
        </button>
        <div className="toolbar-divider"></div>
        <button className="icon-btn" id="btn-zoom-in" title="Zoom In">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35M11 8v6M8 11h6" />
          </svg>
        </button>
        <button className="icon-btn" id="btn-zoom-out" title="Zoom Out">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35M8 11h6" />
          </svg>
        </button>
        <button className="icon-btn" id="btn-reset" title="Reset View">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
            <path d="M21 3v5h-5" />
            <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
            <path d="M3 21v-5h5" />
          </svg>
        </button>
      </div>

      <div id="control-menu">
        <div className="control-group">
          <div className="control-label">RENDER MODE</div>
          <button className="control-btn active" id="btn-point-cloud">
            Point Cloud
          </button>
          <button className="control-btn" id="btn-3d-mesh">
            3D Mesh
          </button>
        </div>

        <div className="section-divider"></div>

        <div id="query-section">
          <div className="section-title">QUERY</div>
          <div className="section-content">
            <div className="query-input-container">
              <input
                type="text"
                id="query-input"
                placeholder="Ask a question..."
              />
              <button
                id="query-send-btn"
                className="query-send-btn"
                title="Send Query"
              >
                <svg
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z" />
                </svg>
              </button>
            </div>
          </div>
        </div>

        <div className="section-divider"></div>

        <div id="object-labels-section">
          <div className="section-title">OBJECT LABELS</div>
          <div className="section-content">Object labels will appear here...</div>
        </div>

        <div className="section-divider"></div>

        <div className="control-group">
          <div className="control-label">QUALITY MODE</div>
          <button className="control-btn active" id="btn-downsampled">
            Downsampled
          </button>
          <button className="control-btn" id="btn-original-quality">
            Original Quality
          </button>
        </div>

        <div className="section-divider"></div>

        <div className="control-group">
          <div className="control-label">COLOR MODE</div>
          <button className="control-btn active" id="btn-original-color">
            Original Color
          </button>
          <button className="control-btn" id="btn-coded-color">
            Coded Color
          </button>
        </div>
      </div>
    </div>
  );
};

export default Viewer;
