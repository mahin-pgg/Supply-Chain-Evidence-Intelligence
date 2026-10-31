from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass


class OllamaUnavailable(RuntimeError):
    """Raised when the configured local Ollama service cannot be reached."""


@dataclass(frozen=True)
class OllamaClient:
    base_url: str = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
    model: str = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
    timeout_seconds: float = 60.0

    def generate_json(self, prompt: str, schema: dict[str, object]) -> dict[str, object]:
        payload = {"model": self.model, "prompt": prompt, "stream": False, "format": schema}
        request = urllib.request.Request(
            f"{self.base_url.rstrip('/')}/api/generate",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                result = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise OllamaUnavailable(f"Ollama is unavailable at {self.base_url}: {error}") from error
        try:
            value = json.loads(result["response"])
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise OllamaUnavailable("Ollama returned a non-JSON or malformed response") from error
        if not isinstance(value, dict):
            raise OllamaUnavailable("Ollama JSON response must be an object")
        return value
