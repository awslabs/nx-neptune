import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://awslabs.github.io',
  base: '/nx-neptune',
  integrations: [
    starlight({
      title: 'nx-neptune',
      description:
        'Graph analytics for your data lake — powered by NetworkX and Amazon Neptune Analytics.',
      customCss: ['./src/styles/custom.css'],
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/awslabs/nx-neptune' },
      ],
      head: [
        {
          tag: 'link',
          attrs: {
            rel: 'stylesheet',
            href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap',
          },
        },
      ],
      sidebar: [
        { label: 'Overview', slug: 'overview' },
        {
          label: 'Getting Started',
          items: [
            { label: 'Installation', slug: 'getting-started/installation' },
            { label: 'Prerequisites', slug: 'getting-started/prerequisites' },
            { label: 'Quick Start', slug: 'getting-started/quickstart' },
          ],
        },
        {
          label: 'Graph Over Data Lake',
          items: [
            { label: 'Session Manager', slug: 'data-lake/session-manager' },
            { label: 'S3 Tables', slug: 'data-lake/s3-tables' },
            { label: 'S3 Vectors', slug: 'data-lake/s3-vectors' },
            { label: 'Databricks', slug: 'data-lake/databricks' },
            { label: 'Snowflake', slug: 'data-lake/snowflake' },
            { label: 'OpenSearch', slug: 'data-lake/opensearch' },
          ],
        },
        {
          label: 'NetworkX Backend',
          items: [
            { label: 'Interface', slug: 'networkx-backend/interface' },
            { label: 'Algorithms', slug: 'networkx-backend/algorithms' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Project Structure', slug: 'reference/source-layout' },
            { label: 'Known Considerations', slug: 'reference/limitations' },
            { label: 'Deployment', slug: 'reference/deployment' },
          ],
        },
      ],
    }),
  ],
});
