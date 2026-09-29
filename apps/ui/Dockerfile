# Build context: repository root (npm workspaces share one lockfile).
FROM node:20-alpine AS build
WORKDIR /repo
COPY package.json package-lock.json ./
COPY apps/ui/package.json apps/ui/
COPY apps/server/package.json apps/server/
COPY verify/package.json verify/
RUN npm ci --workspace apps/ui --include-workspace-root=false --no-audit --no-fund
COPY apps/ui/ apps/ui/
RUN npm run build --workspace apps/ui

# Static files + a reverse proxy for /api: same origin, no CORS, no hard-coded api host in the bundle.
FROM nginx:1.27-alpine
COPY apps/ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /repo/apps/ui/dist /usr/share/nginx/html
