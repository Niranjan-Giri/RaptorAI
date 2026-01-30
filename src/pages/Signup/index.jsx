import React, { useState } from 'react';
import './style.css';
import api from '../../api';
import { useNavigate } from 'react-router-dom';
import { USER_INFO } from '../../constants';

export default function Signup() {
  const [formData, setFormData] = useState({
    username: '',
    email: '',
    password: ''
  });
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prevState => ({
      ...prevState,
      [name]: value
    }));
    setError('');
  };

  const handleSignup = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      const response = await api.post('/auth/signup/', {
        username: formData.username,
        email: formData.email,
        password: formData.password,
        user: 'user'
      });

      if (response.status === 200) {
        // After signup, automatically log in the user
        try {
          const loginResponse = await api.post('/auth/signin/', {
            username: formData.username,
            password: formData.password
          });

          if (loginResponse.status === 200) {
            // Store JWT token for API requests - check both 'token' and 'accessToken' field names
            const token = loginResponse.data.token || loginResponse.data.accessToken;
            if (token) {
              localStorage.setItem('jwtToken', token);
            }
            // Store only non-sensitive user info
            const userInfo = {
              id: loginResponse.data.id,
              username: loginResponse.data.username,
              email: loginResponse.data.email,
              roles: loginResponse.data.roles
            };
            localStorage.setItem(USER_INFO, JSON.stringify(userInfo));
            localStorage.setItem("isLoggedIn", "true");
          navigate(`/${userInfo.username}`);
          }
        } catch (loginErr) {
          // If auto-login fails, redirect to login page
          setError('Account created! Please sign in.');
          setTimeout(() => navigate('/login'), 2000);
        }
      }
    } catch (err) {
      if (err.response?.data?.message) {
        setError(err.response.data.message);
      } else {
        setError('Failed to create account. Please try again.');
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
    <title>Sign Up | RAPTOR Lab</title>
      <div className="flex items-center flex-col justify-center min-h-screen px-6 py-12 lg:px-8">
        <div className="sm:mx-auto sm:w-full sm:max-w-sm">
          <a href="/">
          <img
            alt="RAPTOR Lab"
            src="./images/logo.png"
            className="mx-auto h-20 w-auto"
          />
          </a>
          <h2 className="mt-10 text-center text-2xl/9 font-bold tracking-tight text-white">Create an account</h2>
        </div>

        <div className="mt-10 sm:mx-auto sm:w-full sm:max-w-sm">
          {error && (
            <div className="mb-4 p-4 bg-red-500/20 border border-red-500 rounded-md">
              <p className="text-sm text-red-200">{error}</p>
            </div>
          )}
          <form onSubmit={handleSignup} className="space-y-6">
            <div>
              <label htmlFor="username" className="block text-sm/6 font-medium text-gray-100">
                Username
              </label>
              <div className="mt-2">
                <input
                  id="username"
                  name="username"
                  type="text"
                  value={formData.username}
                  onChange={handleChange}
                  required
                  autoComplete="username"
                  className="block w-full rounded-md bg-white/5 px-3 py-1.5 text-base text-white outline-1 -outline-offset-1 outline-white/10 placeholder:text-gray-500 focus:outline-2 focus:-outline-offset-2 focus:outline-indigo-500 sm:text-sm/6"
                />
              </div>
            </div>
            <div>
              <label htmlFor="email" className="block text-sm/6 font-medium text-gray-100">
                Email address
              </label>
              <div className="mt-2">
                <input
                  id="email"
                  name="email"
                  type="email"
                  value={formData.email}
                  onChange={handleChange}
                  required
                  autoComplete="email"
                  className="block w-full rounded-md bg-white/5 px-3 py-1.5 text-base text-white outline-1 -outline-offset-1 outline-white/10 placeholder:text-gray-500 focus:outline-2 focus:-outline-offset-2 focus:outline-indigo-500 sm:text-sm/6"
                />
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between">
                <label htmlFor="password" className="block text-sm/6 font-medium text-gray-100">
                  Password
                </label>
              </div>
              <div className="mt-2">
                <input
                  id="password"
                  name="password"
                  type="password"
                  value={formData.password}
                  onChange={handleChange}
                  required
                  autoComplete="current-password"
                  className="block w-full rounded-md bg-white/5 px-3 py-1.5 text-base text-white outline-1 -outline-offset-1 outline-white/10 placeholder:text-gray-500 focus:outline-2 focus:-outline-offset-2 focus:outline-indigo-500 sm:text-sm/6"
                />
              </div>
            </div>

            <div>
              <button
                type="submit"
                disabled={loading}
                className="flex w-full justify-center rounded-md bg-indigo-500 px-3 py-1.5 text-sm/6 font-semibold text-white hover:bg-indigo-400 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? 'Creating account...' : 'Sign up'}
              </button>
            </div>
          </form>

          <p className="mt-10 text-center text-sm/6 text-gray-400">
            Already a member?{' '}
            <a href="/login" className="font-semibold text-indigo-400 hover:text-indigo-300">
              Sign in now
            </a>
          </p>
        </div>
      </div>
    </>
  )
}
