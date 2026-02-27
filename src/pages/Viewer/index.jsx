import React, { useEffect, useState } from "react";
import { useSearchParams, useLocation, useNavigate } from "react-router-dom";
import "./style.css";
import { initializeApp } from "../../viewerjs/app.entry.js";
import { EXAMPLE_PLY_FILES } from "../../components/username.jsx";
import api from "../../api";
const Viewer = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { projectName, processedDownloadUrls } = location.state || {};
  const [searchParams] = useSearchParams();
  const [isLoading, setIsLoading] = useState(true);
  const [isMenuOpen, setIsMenuOpen] = useState(window.innerWidth > 768);

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth <= 768) {
        setIsMenuOpen(false);
      } else {
        setIsMenuOpen(true);
      }
    };

    window.addEventListener('resize', handleResize);
    // Initial check
    handleResize();
    
    return () => window.removeEventListener('resize', handleResize);
  }, []);
  
  useEffect(() => {
    let initTimer = null;
    let checkAndSetupUI = null;
    
    // Small delay to ensure canvas-container is in the DOM
    initTimer = setTimeout(() => {
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
      
      let hasInitialized = false; // Flag to prevent multiple initializations
      checkAndSetupUI = setInterval(() => {
        const btnOrbit = document.getElementById('btn-orbit');
        
        if (app && btnOrbit && app.ui && !hasInitialized) {
          hasInitialized = true; // Set flag immediately to prevent re-entry
          clearInterval(checkAndSetupUI); // Clear interval first
          
          if (typeof app.ui.setupMenuControls === 'function') {
            app.ui.setupMenuControls();
          }
          
          // Check if a specific example was selected via query parameters
          const exampleName = searchParams.get('example');
          const pointcloudId = searchParams.get('pointcloudId');
          
          // Check if files were passed from location.state (example or project)
          if (location.state && location.state.files && location.state.files.length > 0) {
            setIsLoading(true);
            
            const filesToLoad = location.state.files;
            
            // Set the PLY files and names
            app.plyFiles = filesToLoad.map(f => f.url);
            app.plyFileNames = filesToLoad.map(f => f.name);
            
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
        }
      }, 100);
    }, 50);

    return () => {
      if (initTimer) clearTimeout(initTimer);
      if (checkAndSetupUI) clearInterval(checkAndSetupUI);
      
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
      
      <button 
        id="menu-toggle-btn" 
        className={isMenuOpen ? 'active' : ''} 
        onClick={() => setIsMenuOpen(!isMenuOpen)}
        title={isMenuOpen ? "Close Menu" : "Open Menu"}
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          {isMenuOpen ? (
            <path d="M18 6L6 18M6 6l12 12" />
          ) : (
            <path d="M3 12h18M3 6h18M3 18h18" />
          )}
        </svg>
      </button>

      <div id="control-menu" className={isMenuOpen ? 'open' : 'closed'}>
        <button 
          id="back-btn" 
          onClick={() => navigate(-1)}
          title="Go Back"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M19 12H5M12 19l-7-7 7-7" />
          </svg>
        </button>
        
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

        <div className="control-group">
          <div className="control-label">POINT SIZE</div>
          <div className="point-size-control">
            <button className="size-btn" id="btn-size-decrease" title="Decrease Point Size">-</button>
            <span className="size-value" id="point-size-value">0.015</span>
            <button className="size-btn" id="btn-size-increase" title="Increase Point Size">+</button>
          </div>
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
