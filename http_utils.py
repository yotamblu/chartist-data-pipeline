"""Shared HTTP helpers: retry/backoff for rate limits and transient errors."""
import logging
import random
import time

import requests

import config

logger = logging.getLogger("chartist.http")


def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """Issue an HTTP request, retrying on 429/5xx with jittered exponential backoff.

    Retries are capped at config.MAX_RETRIES -- this never retries forever.
    Raises requests.HTTPError for non-retryable error statuses (e.g. 400),
    so callers can handle those (like batch-bisection on 400) explicitly.
    """
    backoff = 1.0
    last_exc = None
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, timeout=30, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(
                "Request error on attempt %d/%d for %s: %s",
                attempt, config.MAX_RETRIES, url, exc,
            )
            time.sleep(backoff * random.uniform(0.5, 1.5))
            backoff *= 2
            continue

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else backoff * random.uniform(0.5, 1.5)
            logger.warning(
                "Rate limited (429) on %s, attempt %d/%d, waiting %.1fs",
                url, attempt, config.MAX_RETRIES, wait,
            )
            time.sleep(wait)
            backoff *= 2
            continue

        if response.status_code >= 500:
            logger.warning(
                "Server error %d on %s, attempt %d/%d, retrying",
                response.status_code, url, attempt, config.MAX_RETRIES,
            )
            time.sleep(backoff * random.uniform(0.5, 1.5))
            backoff *= 2
            continue

        return response

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Exhausted retries for {url}")
