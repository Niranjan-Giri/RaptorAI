import React from 'react';
import './style.css';


function NotFound() {
  return (
    <>
      <div className="flex items-center flex-col justify-center min-h-screen px-6 py-12 lg:px-8">
        <div className="sm:mx-auto sm:w-full sm:max-w-sm">
          <img
            alt="RAPTOR Lab"
            src="./public/images/logo.png"
            className="mx-auto h-20 w-auto"
          />
          <h2 className="mt-10 text-center text-2xl/9 font-bold tracking-tight text-white">404 - Page Not Found</h2>
        </div>

        <div className="mt-10 sm:mx-auto sm:w-full sm:max-w-sm">
          <p className="text-center text-white">The page you are looking for does not exist.</p>
          <div className="mt-6 text-center">
            <a
              href="/"
              className="text-indigo-400 hover:text-indigo-300 font-semibold" 
            >
              Go back to Home
            </a>
          </div>
        </div>
      </div>
    </>
  );
}

export default NotFound;