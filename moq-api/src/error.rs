// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("reqwest error: {0}")]
    Request(#[from] reqwest::Error),

    #[error("hyper error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("url error: {0}")]
    Url(#[from] url::ParseError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
