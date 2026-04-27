import type { Config } from 'tailwindcss'

export default {
  content: [
    './docs/**/*.md',
    "./docs/**/*.mdx",
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  darkMode: ["class", '[data-theme="dark"]'],
  theme: {},
  plugins: [
    require('@tailwindcss/typography'),
  ],
  corePlugins: {
    preflight: false,
  }
} satisfies Config
