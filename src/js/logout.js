import { USER_INFO } from "../constants";

/**
 * Handle user logout - clears localStorage and redirects to login
 * This is a pure utility function that can be called from any component
 */
export const handleLogout = () => {
  try {
    // Optional: Call backend logout endpoint to clear server-side session
    fetch(`${import.meta.env.VITE_API_BASE_URL}/api/auth/logout`, {
      method: "POST",
      credentials: "include",
    }).catch(error => console.error("Logout error:", error));
  } catch (error) {
    console.error("Logout error:", error);
  } finally {
    // Clear all local storage
    localStorage.clear();
    // Redirect to login page
    window.location.href = "/login";
  }
};

