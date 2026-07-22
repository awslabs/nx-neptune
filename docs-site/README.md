# nx-neptune docs-site

Documentation site for [nx-neptune](https://github.com/awslabs/nx-neptune), built
with [Astro](https://astro.build/) and [Starlight](https://starlight.astro.build/).

## Local development

```bash
npm install
npm run dev      # start the dev server at http://localhost:4321/nx-neptune
npm run build    # build the static site into dist/
npm run preview  # preview the production build locally
```

Documentation pages live in `src/content/docs/`. The sidebar is configured in
`astro.config.mjs`.

## Blog

The blog is powered by the [`starlight-blog`](https://starlight-blog-docs.vercel.app/)
plugin, registered in `astro.config.mjs`. Blog posts live in
`src/content/docs/blog/` and are published at `/blog`, with an RSS feed at
`/blog/rss.xml`.

### Adding a new blog post

1. Create a new Markdown (`.md`) or MDX (`.mdx`) file in `src/content/docs/blog/`,
   e.g. `src/content/docs/blog/my-post.md`. The file name becomes the URL slug
   (`/blog/my-post`).
2. Add frontmatter at the top of the file:

   ```markdown
   ---
   title: My post title
   date: 2026-07-20
   excerpt: A short summary shown in the blog post list.
   authors: nxNeptuneTeam
   tags:
     - announcements
   ---

   Your post content goes here, written in Markdown.
   ```

   | Field     | Required | Notes                                                                 |
   | --------- | -------- | --------------------------------------------------------------------- |
   | `title`   | Yes      | Post title.                                                           |
   | `date`    | Yes      | Publication date (`YYYY-MM-DD`). Posts are sorted newest first.       |
   | `excerpt` | No       | Short summary shown in the post list and used for metadata.           |
   | `authors` | No       | Author key(s) defined in `astro.config.mjs`, or inline author object. |
   | `tags`    | No       | List of tags; each generates a `/blog/tags/<tag>` page.               |

3. Authors are defined once in the `starlightBlog({ authors: { ... } })` config in
   `astro.config.mjs`. To add a new author, add an entry there and reference its
   key from a post's `authors` field.
4. Run `npm run dev` and visit `/blog` to preview your post.

For the full list of supported frontmatter and configuration options, see the
[starlight-blog documentation](https://starlight-blog-docs.vercel.app/).
