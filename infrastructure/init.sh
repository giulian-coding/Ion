#!/usr/bin/env bash
set -e

# 1. Operator
nsc add operator --name GTOps --generate-signing-key --sys
nsc edit operator --require-signing-keys \
    --account-jwt-server-url nats://localhost:4222

# 2. Backend-Account mit JetStream
nsc add account --name APP
nsc edit account APP --js-mem-storage 512M --js-disk-storage 5G
nsc add user --account APP --name backend
nsc generate creds -a APP -n backend > backend.creds

# 3. Frontend-Account (nur lesen)
nsc add account --name WEB
nsc add user --account WEB --name browser \
    --allow-sub "public.>" --deny-pub ">"
nsc generate creds -a WEB -n browser > browser.creds

# 4. Auth-Block fuer nats.conf
nsc generate config --nats-resolver > auth.conf
sed -i "s|dir: .*|dir: '/jwt'|" auth.conf

echo "Fertig. Jetzt: cat auth.conf >> nats.conf && docker compose up -d && nsc push -A"