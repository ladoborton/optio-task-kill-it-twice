# Build context: repository root (npm workspaces share one lockfile).
FROM node:20-alpine AS build
WORKDIR /repo
COPY package.json package-lock.json ./
COPY apps/server/package.json apps/server/
COPY verify/package.json verify/
RUN npm ci --workspace apps/server --include-workspace-root=false --no-audit --no-fund
COPY apps/server/ apps/server/
RUN npm run build --workspace apps/server

FROM node:20-alpine
WORKDIR /repo
COPY package.json package-lock.json ./
COPY apps/server/package.json apps/server/
COPY verify/package.json verify/
RUN npm ci --workspace apps/server --include-workspace-root=false --omit=dev --no-audit --no-fund
COPY --from=build /repo/apps/server/dist apps/server/dist
WORKDIR /repo/apps/server
# ROLE selects what this container is (SPEC §5.1).
CMD ["node", "--enable-source-maps", "dist/main.js"]
