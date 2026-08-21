import axios from "axios";

const configuredApiBaseUrl = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "");
const apiBaseUrl = "/api";

const api = axios.create({
  baseURL: apiBaseUrl,
  headers: {
    "Content-Type": "application/json",
  },
});

api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem("jwtToken");
    if (localStorage.getItem("isLoggedIn") === "true" && token) {
      config.headers.Authorization = `Bearer ${token}`;
      console.log(
        "[API] Adding JWT token to request:",
        config.url,
        token.substring(0, 20) + "...",
      );
    } else {
      console.log(
        "[API] No JWT token found in localStorage for:",
        config.url,
      );
    }

    return config;
  },
  (error) => Promise.reject(error),
);

api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.log(
      "[API] Response error:",
      error.response?.status,
      error.config?.url,
    );

    if (error.response?.status === 401) {
      localStorage.clear();
      localStorage.setItem("isLoggedIn", "false");
      window.location.href = "/login";
    }

    return Promise.reject(error);
  },
);

export { apiBaseUrl };

export default api;