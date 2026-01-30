import { USER_INFO } from "../constants";
import { useState, useEffect, useRef } from "react";
import { SettingsIcon, HomeIcon, X } from "lucide-react";
import { useParams, useNavigate } from "react-router-dom";
import NotFound from "../pages/NotFound";
import { handleLogout } from "../js/logout";
import { fetchProcessedPointclouds } from "../js/pointcloudService";
import { gsap } from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";

gsap.registerPlugin(ScrollTrigger);

export const EXAMPLE_PLY_FILES = [
  {
    id: 1,
    name: "Room 212",
    description: "Butler Room 212",
    thumbnail: "/images/room212.png",
    bucketUrl: "https://storage.googleapis.com/examples_ply/room212/",
    plyFiles: [
      "bookshelf_1",
      "ceiling light_1",
      "ceiling light_2",
      "ceiling light_3",
      "ceiling light_4",
      "ceiling light_5",
      "ceiling",
      "computer box_1",
      "computer box_2",
      "computer box_3",
      "computer keyboard_1",
      "computer keyboard_2",
      "computer keyboard_3",
      "computer keyboard_4",
      "computer keyboard_5",
      "computer monitor_1",
      "computer monitor_2",
      "computer monitor_3",
      "floor",
      "office chair_1",
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
      "unlabeled",
      "wall_1",
      "wall_2",
      "wall_3",
      "wall_4",
      "wall_5",
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

  const handleLoadProjects = (project) => {
    // Navigate to viewer with example name
    navigate(`/viewer?project=${project.id}`, {
      state: {
        projectName: project.name,
        processedDownloadUrls: project.processedDownloadUrls,
      },
    });
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
            <div className="bg-gray-800 rounded-lg border border-gray-700 p-8 text-center">
              <p className="text-gray-400">Loading your projects...</p>
            </div>
          ) : projects.length === 0 ? (
            <div className="bg-gray-800 rounded-lg border border-gray-700 p-8 text-center">
              <p className="text-gray-400">
                Projects will appear here once you upload a pointcloud
              </p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {projects.map((project, index) => (
                <div
                  key={project.id || index}
                  className="bg-gray-800 rounded-lg shadow hover:shadow-lg transition-all p-6 cursor-pointer border border-gray-700 hover:border-blue-500 hover:scale-105"
                  onClick={() => handleLoadProjects(project)}
                >
                  {/* Project Placard */}
                  <div className="h-48 bg-gradient-to-br from-slate-700 to-slate-900 rounded-lg mb-4 flex items-center justify-center relative overflow-hidden">
                    {/* Background accent */}
                    <div className="absolute inset-0 opacity-20 bg-blue-500"></div>

                    {/* Placard content */}
                    <div className="relative z-10 text-center px-6">
                      <h3 className="text-2xl font-bold text-white mb-3">
                        {project.name}
                      </h3>
                      <p className="text-gray-300 text-sm">
                        Created:{" "}
                        {new Date(project.createdAt).toLocaleDateString(
                          "en-US",
                          {
                            year: "numeric",
                            month: "short",
                            day: "numeric",
                          },
                        )}
                      </p>
                      <p className="text-gray-400 text-xs mt-2">
                        {project.allUrls.reduce(
                          (total, cat) => total + cat.urls.length,
                          0,
                        )}{" "}
                        Models
                      </p>
                    </div>
                  </div>

                  {/* Project info */}
                  <h4 className="text-lg font-semibold text-white">
                    {project.name}
                  </h4>
                  <p className="text-gray-400 text-sm mt-2">
                    {project.description}
                  </p>
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
