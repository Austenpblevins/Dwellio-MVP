# Dwellio Web App

This Next.js app is the public Dwellio web frontend plus the internal admin pages.

Current public flow:
- `/`
- `/search?address={query}`
- `/parcel/{county_id}/{tax_year}/{account_number}`

The public web app consumes the canonical backend routes:
- `GET /search?address={query}`
- `GET /quote/{county_id}/{tax_year}/{account_number}`
- `GET /quote/{county_id}/{tax_year}/{account_number}/explanation`
- `POST /lead`

## Getting Started

Run the development server from `apps/web`:

```bash
npm install
export NEXT_PUBLIC_DWELLIO_API_BASE_URL='http://127.0.0.1:8000'
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

Runtime config:
- `NEXT_PUBLIC_DWELLIO_API_BASE_URL`
- `DWELLIO_API_BASE_URL`

If neither env var is set, the public app falls back to `http://127.0.0.1:8000`.
If the backend is unreachable or misconfigured, the public search/funnel pages render an explicit error state instead of fabricating quote data.

## Checks

```bash
npm run lint
npm run build
```

## Deploy on Vercel

Recommended project settings:
- project root: `apps/web`
- install command: `npm install`
- build command: `npm run build`
- output setting: default Next.js output

Required environment variables:
- production: `NEXT_PUBLIC_DWELLIO_API_BASE_URL`
- optional server-side fallback: `DWELLIO_API_BASE_URL`

Typical values:
- local preview: `http://127.0.0.1:8000`
- deployed preview/production: your public Dwellio API origin

Known limitations:
- the public funnel supports Harris and Fort Bend only
- the current quote funnel is SFR-only
- the parcel page is the quote-to-lead page; no alternate quote route or duplicate frontend funnel architecture exists

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
