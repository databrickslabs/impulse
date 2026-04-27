import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Impulse',
  tagline: 'Large-scale time-series measurement data analytics on Apache Spark.',
  favicon: 'img/impulse_icon.svg',

  url: 'https://databrickslabs.github.io',
  baseUrl: '/impulse/',

  organizationName: 'databrickslabs',
  projectName: 'impulse',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',
  onDuplicateRoutes: 'throw',
  onBrokenAnchors: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    async function tailwindPlugin(context, options) {
      return {
        name: "docusaurus-plugin-tailwindcss",
        configurePostCss(postcssOptions) {
          postcssOptions.plugins = [
            require('tailwindcss'),
            require('autoprefixer'),
          ];
          return postcssOptions;
        },
      };
    },
    function suppressKnownWarnings() {
      return {
        name: 'suppress-known-warnings',
        configureWebpack() {
          return {
            ignoreWarnings: [
              { message: /Critical dependency: require function is used/ },
            ],
          };
        },
      };
    },
    'docusaurus-lunr-search',
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/databrickslabs/impulse/tree/main/docs/impulse/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/impulse_icon.svg',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false
    },
    navbar: {
      title: 'Impulse',
      logo: {
        alt: 'Impulse Logo',
        src: 'img/impulse_icon.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/databrickslabs/impulse',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      links: [],
      copyright: `Copyright © ${new Date().getFullYear()} Impulse. Docs built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
      additionalLanguages: ['python', 'bash', 'json'],
    },
  } satisfies Preset.ThemeConfig,
  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],
};

export default config;
