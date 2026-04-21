# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0

export CAROOT ?= $(shell cd dev ; go run filippo.io/mkcert -CAROOT)

.PHONY: run
run: dev/localhost.crt
	@docker compose up --build --remove-orphans

dev/localhost.crt:
	@dev/cert
