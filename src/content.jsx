export const isLoggedIn = localStorage.getItem("isLoggedIn") === "true";

export const handleLogin = () => {
  window.location.href = "/";
  localStorage.setItem("isLoggedIn", "true");
}
export const handleLoginButtonClick = () => {
  window.location.href = "/login";
};

export const navigation = [{ name: "Viewer", href: "/viewer", current: true }];

export const userNavigation = [
  {
    name: "Sign out",
    onClick: async () => {
      try {
        // Call backend logout endpoint to clear server-side session
        await fetch(`${import.meta.env.VITE_API_BASE_URL}/api/auth/logout`, {
          method: 'POST',
          credentials: 'include'
        });
      } catch (error) {
        console.error('Logout error:', error);
      } finally {
        // Clear all local storage
        localStorage.clear();
        window.location.href = "/login";
      }
    },
  },
];