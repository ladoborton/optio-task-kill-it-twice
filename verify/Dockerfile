# Build context: repository root (npm workspaces share one lockfile).
FROM node:20-alpine
# docker CLI only — it talks to the host daemon through the mounted socket.
RUN apk add --no-cache docker-cli
WORKDIR /repo
COPY package.json package-lock.json ./
COPY verify/package.json verify/
COPY apps/ui/package.json apps/ui/
COPY apps/server/package.json apps/server/
RUN npm ci --workspace verify --include-workspace-root=false --no-audit --no-fund
COPY verify/ verify/
WORKDIR /repo/verify
ENTRYPOINT ["npx", "tsx", "src/main.ts"]
