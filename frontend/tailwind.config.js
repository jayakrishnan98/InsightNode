/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./app/**/*.{js,ts,jsx,tsx}", "./components/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: {
          950: "#0c1222",
          900: "#121a2b",
          800: "#1a2438",
          700: "#243049",
        },
        mist: {
          100: "#e8eef7",
          200: "#c5d0e0",
          400: "#8a9bb5",
        },
        signal: {
          green: "#3dba7a",
          amber: "#e0a53a",
          red: "#e35d5d",
          blue: "#4d8fd6",
        },
      },
      fontFamily: {
        sans: ["var(--font-geist-sans)", "ui-sans-serif", "system-ui", "sans-serif"],
        mono: ["var(--font-geist-mono)", "ui-monospace", "monospace"],
      },
    },
  },
  plugins: [],
};
