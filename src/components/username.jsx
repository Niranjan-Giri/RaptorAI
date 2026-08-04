import { USER_INFO } from "../constants";
import { useState, useEffect, useRef } from "react";
import { SettingsIcon, HomeIcon, X } from "lucide-react";
import { useParams, useNavigate } from "react-router-dom";
import NotFound from "../pages/NotFound";
import { handleLogout } from "../js/logout";
import { fetchProcessedPointclouds, fetchPointcloudItems, fetchPointcloudDetail } from "../js/pointcloudService";
import { gsap } from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";

gsap.registerPlugin(ScrollTrigger);

export const EXAMPLE_PLY_FILES = [
  {
    id: 1,
    name: "Room 314",
    description: "Butler Room 314",
    thumbnail: "/images/room314.png",
    bucketUrl: "https://storage.googleapis.com/examples_ply/room314/",
    plyFiles: [
      "backpack_1",
      "backpack_2",
      "backpack_3",
      "backpack_4",
      "ceiling",
      "chair_1",
      "chair_10",
      "chair_11",
      "chair_12",
      "chair_13",
      "chair_14",
      "chair_15",
      "chair_16",
      "chair_17",
      "chair_18",
      "chair_2",
      "chair_3",
      "chair_4",
      "chair_5",
      "chair_6",
      "chair_7",
      "chair_8",
      "chair_9",
      "computer box_1",
      "computer box_2",
      "computer keyboard_1",
      "computer keyboard_2",
      "computer keyboard_3",
      "computer keyboard_4",
      "computer keyboard_5",
      "computer keyboard_6",
      "computer monitor_1",
      "computer monitor_2",
      "computer monitor_3",
      "desk_1",
      "desk_10",
      "desk_2",
      "desk_3",
      "desk_4",
      "desk_5",
      "desk_6",
      "desk_7",
      "desk_8",
      "desk_9",
      "floor",
      "shelf_1",
      "shelf_10",
      "shelf_11",
      "shelf_12",
      "shelf_13",
      "shelf_14",
      "shelf_15",
      "shelf_16",
      "shelf_17",
      "shelf_18",
      "shelf_19",
      "shelf_2",
      "shelf_20",
      "shelf_21",
      "shelf_3",
      "shelf_4",
      "shelf_5",
      "shelf_6",
      "shelf_7",
      "shelf_8",
      "shelf_9",
      "thermostat_1",
      "thermostat_2",
      "trash can_1",
      "trash can_2",
      "trash can_3",
      "trash can_4",
      "trash can_5",
      "trash can_6",
      "trash can_7",
      "unlabeled",
      "wall_1",
      "wall_2",
      "wall_3",
      "wall_4",
      "wall_5",
    ],
  },
  {
    id: 2,
    name: "Room 104",
    description: "Butler Room 104",
    thumbnail: "/images/room104.png",
    bucketUrl: "https://storage.googleapis.com/examples_ply/room104/",
    plyFiles: [
  "air vent_1",
  "air vent_2",
  "air vent_3",
  "air vent_4",
  "air vent_5",
  "box fan_1",
  "ceiling light_1",
  "ceiling",
  "door_1",
  "door_2",
  "floor",
  "laptop_1",
  "office chair_1",
  "office chair_10",
  "office chair_11",
  "office chair_2",
  "office chair_3",
  "office chair_4",
  "office chair_5",
  "office chair_6",
  "office chair_7",
  "office chair_8",
  "office chair_9",
  "table_1",
  "table_2",
  "table_3",
  "table_4",
  "table_5",
  "thermostat_1",
  "thermostat_2",
  "thermostat_3",
  "thermostat_4",
  "trash can_1",
  "trash can_2",
  "trash can_3",
  "tv_1",
  "unlabeled",
  "wall_1",
  "wall_2",
  "wall_3",
  "wall_4",
  "wall_5",
  "white board_1",
  "wifi router_1"
],
  },
];

export function Username() {
  const { username } = useParams();
  const navigate = useNavigate();
  const [userInfo, setUserInfo] = useState(null);
  const [isValid, setIsValid] = useState(null); // null = loading, true/false = result
  const [showSettings, setShowSettings] = useState(false);
  const [projects, setProjects] = useState([]);
  const [loadingProjects, setLoadingProjects] = useState(false);

  // Refs for parallax effect
  const bannerRef = useRef(null);
  const profileImageRef = useRef(null);
  const profileInfoRef = useRef(null);

  useEffect(() => {
    console.log("[Profile] Component mounted, checking auth");
    const stored = localStorage.getItem(USER_INFO);
    console.log(
      "[Profile] User info from storage:",
      stored ? JSON.parse(stored) : "none",
    );
    console.log(
      "[Profile] JWT token exists:",
      !!localStorage.getItem("jwtToken"),
    );

    if (stored) {
      try {
        const parsedUser = JSON.parse(stored);
        // Check if the URL username matches the logged-in user's username
        if (parsedUser.username !== username) {
          // Username doesn't match - show NotFound
          setIsValid(false);
          return;
        }
        setUserInfo(parsedUser);
        setIsValid(true);
        // Update page title
        document.title = `${parsedUser.username}'s Profile - RaptorAI`;
        // Fetch processed pointclouds immediately since user is validated
        loadProjects();
      } catch (e) {
        console.error("Error parsing user info:", e);
        setIsValid(false);
      }
    } else {
      setIsValid(false);
    }
  }, [username]);

  const loadProjects = async () => {
    setLoadingProjects(true);
    try {
      const projects = await fetchProcessedPointclouds();
      console.log("[Profile] Fetched projects:", projects);
      setProjects(projects);
    } catch (error) {
      setProjects([]);
    } finally {
      setLoadingProjects(false);
    }
  };

  // Parallax effect with GSAP ScrollTrigger
  useEffect(() => {
    if (!isValid || !bannerRef.current) return;

    // Banner 3D parallax - zoom, rotate, and move with perspective
    gsap.to(bannerRef.current, {
      yPercent: 40,
      scale: 1.2,
      rotateX: -15,
      transformOrigin: "center top",
      ease: "none",
      scrollTrigger: {
        trigger: bannerRef.current,
        start: "top top",
        end: "bottom top",
        scrub: 1,
      },
    });

    // Profile image - faster movement with scale effect
    if (profileImageRef.current) {
      gsap.to(profileImageRef.current, {
        y: -50,
        scale: 1.1,
        rotateZ: -5,
        ease: "none",
        scrollTrigger: {
          trigger: bannerRef.current,
          start: "top top",
          end: "bottom top",
          scrub: 1.5,
        },
      });
    }

    // Profile info - subtle upward movement
    if (profileInfoRef.current) {
      gsap.to(profileInfoRef.current, {
        y: -80,
        opacity: 0.3,
        ease: "none",
        scrollTrigger: {
          trigger: bannerRef.current,
          start: "top top",
          end: "bottom top",
          scrub: 2,
        },
      });
    }

    return () => {
      ScrollTrigger.getAll().forEach((trigger) => trigger.kill());
    };
  }, [isValid]);

  const handleLoadExample = (example) => {
    // Navigate to viewer with example bucket
    const files = example.plyFiles.map((fileName, index) => {
      const url = `${example.bucketUrl}${encodeURIComponent(fileName)}.ply`;
      return {
        name: fileName.replace(/ /g, "_"),
        url: url,
      };
    });
    console.log("Generated files:", files);

    navigate(`/viewer?example=${example.name}`, {
      state: {
        projectName: example.name,
        files: files,
        isExample: true,
      },
    });
  };

  const [loadingProjectId, setLoadingProjectId] = useState(null);

  const handleLoadProjects = async (project) => {
    if (loadingProjectId) return; // Prevent double-clicks
    setLoadingProjectId(project.id);
    try {
      const items = await fetchPointcloudItems(project.id);
      console.log("[Profile] Fetched pointcloud items for project:", project.id, items);

      let files = (Array.isArray(items) ? items : [])
        .map((item, idx) => {
          // Robust check for URL field names across camelCase & snake_case
          const url =
            item.signedDrcUrl ||
            item.signed_drc_url ||
            item.signedPlyUrl ||
            item.signed_ply_url ||
            item.signedUrl ||
            item.signed_url ||
            item.downloadUrl ||
            item.download_url ||
            item.deliveryUrl ||
            item.delivery_url ||
            item.url ||
            item.filepath ||
            item.file;

          if (!url || typeof url !== "string") return null;

          const isDrc = !!(
            item.signedDrcUrl ||
            item.signed_drc_url ||
            item.deliveryUrl ||
            item.delivery_url ||
            url.toLowerCase().includes(".drc")
          );

          return {
            id: item.id || idx,
            name: item.label || item.name || item.filename || item.object_name || `model-${idx + 1}`,
            url: url,
            format: isDrc ? "drc" : "ply",
            sizeBytes: item.sizeBytes || item.size_bytes || item.size,
          };
        })
        .filter(Boolean);

      // Fallback: If no valid item files were returned, try fetching full pointcloud detail
      if (files.length === 0) {
        console.log(`[Profile] No item files found for project ${project.id}. Attempting fallback detail fetch...`);
        const detail = await fetchPointcloudDetail(project.id);
        if (detail) {
          if (detail.processedDownloadUrls) {
            files = Object.entries(detail.processedDownloadUrls).flatMap(([category, urls]) => {
              const urlList = Array.isArray(urls) ? urls : [urls];
              return urlList.map((url, index) => ({
                id: `${category}-${index}`,
                name: urlList.length > 1 ? `${category}${index + 1}` : category,
                url: url,
                format: typeof url === 'string' && url.toLowerCase().includes('.drc') ? 'drc' : 'ply',
              }));
            }).filter(f => f.url && typeof f.url === 'string');
          } else if (detail.downloadUrl || detail.download_url || detail.signedUrl || detail.signed_url) {
            const url = detail.downloadUrl || detail.download_url || detail.signedUrl || detail.signed_url;
            files = [{
              id: detail.id || project.id,
              name: detail.name || project.name || 'Model',
              url: url,
              format: typeof url === 'string' && url.toLowerCase().includes('.drc') ? 'drc' : 'ply',
            }];
          }
        }
      }

      if (files.length === 0) {
        alert(`No viewable 3D models available for project "${project.name || 'selected'}".`);
        return;
      }

      navigate(`/viewer?project=${project.id}`, {
        state: {
          projectName: project.name,
          files: files,
        },
      });
    } catch (error) {
      console.error("[Profile] Failed to load project items:", error);
      alert("Failed to load project files. Please try again.");
    } finally {
      setLoadingProjectId(null);
    }
  };

  // Show loading state while validating
  if (isValid === null) {
    return <div className="min-h-screen bg-gray-900" />;
  }

  // If username doesn't match, show NotFound page
  if (!isValid) {
    return <NotFound />;
  }

  return (
    <div className="min-h-screen bg-gray-900 overflow-y-auto">
      {/* Banner Section */}
      <div
        className="relative overflow-hidden"
        style={{ perspective: "1000px" }}
      >
        {/* Banner Background */}
        <div
          ref={bannerRef}
          className="h-100 bg-cover bg-center relative"
          style={{
            backgroundImage: "url(/images/profile-bg1.jpeg)",
            transformStyle: "preserve-3d",
          }}
        >
          {/* Overlay for better text visibility */}
          <div className="absolute inset-0 bg-black/30"></div>
        </div>
        {/* Profile Section */}
        <div className="relative px-8 pb-8">
          {/* Profile Picture positioned over banner */}
          <div className="flex items-end -mt-24 mb-4">
            <div ref={profileImageRef} className="relative">
              <img
                src={`https://api.dicebear.com/7.x/avataaars/svg?seed=${userInfo?.username}`}
                crossOrigin="anonymous"
                alt="Profile"
                className="w-40 h-40 rounded-full border-4 border-gray-900 shadow-lg bg-white"
              />
            </div>
            <div ref={profileInfoRef} className="ml-6 mb-2">
              <h1 className="text-4xl font-bold text-white">
                {userInfo?.username}
              </h1>
              <p className="text-gray-400 text-lg">{userInfo?.email}</p>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="px-8 py-8">
        {/* Examples Section */}
        <section className="mb-12 mx-auto max-w-7xl">
          <h2 className="text-2xl font-bold text-white mb-6">Examples</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {EXAMPLE_PLY_FILES.map((example) => (
              <div
                key={example.id}
                onClick={() => handleLoadExample(example)}
                className="bg-gray-800 rounded-lg shadow hover:shadow-lg transition-all p-6 cursor-pointer border border-gray-700 hover:border-blue-500 hover:scale-105"
              >
                <div
                  className="h-48 bg-gradient-to-br from-purple-600 to-blue-600 rounded-lg mb-4 flex flex-col items-center justify-center relative overflow-hidden bg-cover bg-center"
                  style={{
                    backgroundImage: `url(${example.thumbnail})`,
                  }}
                >
                  <div className="absolute inset-0 bg-black/40"></div>
                  <div className="relative z-10 text-center px-4">
                    <span className="text-white font-semibold text-xl mb-2 block">
                      {example.name}
                    </span>
                    <span className="text-gray-200 text-sm">
                      {example.plyFiles.length} Models
                    </span>
                  </div>
                </div>
                <h3 className="text-lg font-semibold text-white">
                  {example.name}
                </h3>
                <p className="text-gray-400 text-sm mt-2">
                  {example.description}
                </p>
              </div>
            ))}
          </div>
        </section>

        {/* Your Projects Section */}
        <section className="mb-2 mx-auto max-w-7xl">
          <h2 className="text-2xl font-bold text-white mb-6">Your Projects</h2>
          {loadingProjects ? (
            <div className="space-y-4">
              {/* Animated loading bar */}
              <div className="w-full h-1.5 bg-gray-700 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-blue-500 via-purple-500 to-blue-500 rounded-full"
                  style={{
                    width: "60%",
                    animation: "projectsLoadingBar 1.4s ease-in-out infinite",
                  }}
                />
              </div>
              {/* Shimmer skeleton cards */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {[1, 2, 3].map((i) => (
                  <div
                    key={i}
                    className="bg-gray-800 rounded-xl border border-gray-700 p-5 overflow-hidden relative"
                    style={{ opacity: 1 - (i - 1) * 0.25 }}
                  >
                    <div
                      className="h-5 rounded bg-gray-700 mb-3 w-3/4"
                      style={{ animation: "shimmer 1.6s ease-in-out infinite" }}
                    />
                    <div
                      className="h-4 rounded bg-gray-700 w-1/2"
                      style={{ animation: "shimmer 1.6s ease-in-out infinite 0.15s" }}
                    />
                  </div>
                ))}
              </div>
              <style>{`
                @keyframes projectsLoadingBar {
                  0% { transform: translateX(-100%); }
                  50% { transform: translateX(100%); }
                  100% { transform: translateX(100%); }
                }
                @keyframes shimmer {
                  0%, 100% { opacity: 0.4; }
                  50% { opacity: 0.8; }
                }
              `}</style>
            </div>
          ) : projects.length === 0 ? (
            <div className="bg-gray-800 rounded-lg border border-gray-700 p-12 text-center">
              <div className="mb-4 opacity-40">
                <svg xmlns="http://www.w3.org/2000/svg" className="w-12 h-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
                </svg>
              </div>
              <p className="text-gray-300 font-medium text-lg">Your uploaded projects will appear here</p>
              <p className="text-gray-500 text-sm mt-2">Use the RaptorTwin app to scan and upload your environment</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {projects.map((project, index) => (
                <div
                  key={project.id || index}
                  className={`bg-gray-800/90 rounded-xl shadow-md hover:shadow-xl transition-all duration-300 p-5 cursor-pointer border border-gray-700/80 hover:border-blue-500/80 hover:scale-[1.02] relative group flex flex-col justify-between ${
                    loadingProjectId && loadingProjectId !== project.id ? "opacity-50 pointer-events-none" : ""
                  }`}
                  onClick={() => handleLoadProjects(project)}
                >
                  {/* Loading overlay */}
                  {loadingProjectId === project.id && (
                    <div className="absolute inset-0 bg-gray-900/80 rounded-xl flex items-center justify-center z-20 backdrop-blur-xs">
                      <div className="flex flex-col items-center gap-3">
                        <div className="w-7 h-7 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />
                        <span className="text-blue-300 text-xs font-medium">Loading items…</span>
                      </div>
                    </div>
                  )}

                  <div className="flex items-center justify-between gap-4">
                    <div className="flex-1 min-w-0">
                      {/* File / Project Name */}
                      <h3 className="text-lg font-semibold text-white truncate group-hover:text-blue-400 transition-colors">
                        {project.name || project.fileName || "Untitled Project"}
                      </h3>

                      {/* Created At */}
                      <p className="text-gray-400 text-sm mt-2 flex items-center gap-2">
                        <svg
                          className="w-4 h-4 text-gray-500 flex-shrink-0"
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={1.5}
                            d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"
                          />
                        </svg>
                        <span>
                          Created:{" "}
                          {project.createdAt
                            ? new Date(project.createdAt).toLocaleDateString("en-US", {
                                year: "numeric",
                                month: "short",
                                day: "numeric",
                              })
                            : "Date unavailable"}
                        </span>
                      </p>
                    </div>

                    {/* 3D Model / File Icon */}
                    <div className="p-2.5 rounded-lg bg-gray-700/50 text-blue-400 border border-gray-600/50 group-hover:bg-blue-500/10 group-hover:border-blue-500/30 transition-all flex-shrink-0">
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
                      </svg>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
      <footer className="bg-white rounded-lg shadow-sm dark:bg-gray-900 m-20">
        <div className="w-full max-w-screen-xl mx-auto p-2 md:py-8">
          <div className="sm:flex sm:items-center sm:justify-between">
            <a
              href="/home"
              className="flex items-center mb-4 sm:mb-0 space-x-3 rtl:space-x-reverse"
            >
              <img
                src="./images/favicon.png"
                className="h-8"
                alt="Flowbite Logo"
              />
              <span className="self-center text-2xl font-semibold whitespace-nowrap dark:text-white">
                RAPTOR
              </span>
            </a>
            <ul className="flex flex-wrap items-center mb-6 text-sm font-medium text-gray-500 sm:mb-0 dark:text-gray-400">
              <li>
                <a href="#" className="hover:underline me-4 md:me-6">
                  About
                </a>
              </li>
              <li>
                <a href="#" className="hover:underline me-4 md:me-6">
                  Privacy Policy
                </a>
              </li>
              <li>
                <a href="#" className="hover:underline me-4 md:me-6">
                  Licensing
                </a>
              </li>
              <li>
                <a href="#" className="hover:underline">
                  Contact
                </a>
              </li>
            </ul>
          </div>
          <hr className="my-6 border-gray-200 sm:mx-auto dark:border-gray-700 lg:my-8" />
          <span className="block text-sm text-gray-500 sm:text-center dark:text-gray-400">
            © 2025{" "}
            <a href="/home" className="hover:underline">
              RAPTOR™
            </a>
            . All Rights Reserved.
          </span>
        </div>
      </footer>
      {/* Settings Drawer Overlay */}
      {showSettings && (
        <div
          className="fixed inset-0 bg-black bg-opacity-90 z-40 transition-opacity"
          onClick={() => setShowSettings(false)}
        />
      )}

      {/* Settings Drawer */}
      <div
        className={`fixed top-0 right-0 h-full w-80 bg-gray-800 shadow-2xl transform transition-transform duration-300 ease-in-out z-50 ${
          showSettings ? "translate-x-0" : "translate-x-full"
        }`}
      >
        {/* Drawer Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-700">
          <h2 className="text-xl font-bold text-white">Settings</h2>
          <button
            onClick={() => setShowSettings(false)}
            className="text-gray-400 hover:text-white transition-colors"
          >
            <X size={24} />
          </button>
        </div>

        {/* Drawer Content */}
        <div className="p-6 space-y-6 overflow-y-auto h-[calc(100%-80px)]">
          {/* Profile Section */}
          <div>
            <h3 className="text-sm font-semibold text-gray-300 uppercase mb-3">
              Profile
            </h3>
            <div className="space-y-3">
              <div>
                <label className="text-xs text-gray-400">Username</label>
                <p className="text-white font-medium">{userInfo?.username}</p>
              </div>
              <div>
                <label className="text-xs text-gray-400">Email</label>
                <p className="text-white font-medium">{userInfo?.email}</p>
              </div>
            </div>
          </div>

          {/* Account Actions */}
          <div className="border-t border-gray-700 pt-6">
            <button
              className="w-full bg-red-600 hover:bg-red-700 text-white font-medium py-2 px-4 rounded transition-colors"
              onClick={handleLogout}
            >
              Logout
            </button>
          </div>
        </div>
      </div>

      {/* Settings Button - Bottom Left */}
      <div className="fixed bottom-8 left-8">
        <button
          onClick={() => setShowSettings(true)}
          className="bg-blue-600 hover:bg-blue-700 text-white rounded-full p-4 shadow-lg transition-all hover:scale-110 z-40"
        >
          <SettingsIcon size={24} />
        </button>
      </div>

      {/* Home Button - Bottom Right */}
      <div className="fixed bottom-8 right-8">
        <button
          onClick={() => navigate("/")}
          className="bg-blue-600 hover:bg-blue-700 text-white rounded-full p-4 shadow-lg transition-all hover:scale-110"
        >
          <HomeIcon size={24} />
        </button>
      </div>
    </div>
  );
}
