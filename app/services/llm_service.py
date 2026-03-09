import json
from typing import TypedDict
from urllib import error, request

from app.core.config import get_settings


class LLMMessage(TypedDict):
    role: str
    content: str


class LLMServiceError(RuntimeError):
    pass


class LLMService:
    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.llm_base_url.rstrip("/")
        self.api_key = settings.llm_api_key
        self.model = settings.llm_model
        self.timeout_seconds = settings.llm_timeout_seconds

    def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.2,
        max_tokens: int = 400,
    ) -> str:
        if not self.api_key:
            raise LLMServiceError("LLM_API_KEY is not configured.")

        payload = self._build_generate_payload(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=f"{self.base_url}/models/{self.model}:generateContent",
            data=body,
            headers={
                "x-goog-api-key": self.api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_data = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise LLMServiceError(f"LLM request failed ({exc.code}): {detail}") from exc
        except error.URLError as exc:
            raise LLMServiceError(f"LLM request failed: {exc.reason}") from exc

        candidates = response_data.get("candidates", [])
        if not candidates:
            raise LLMServiceError("LLM response did not include any candidates.")

        parts = candidates[0].get("content", {}).get("parts", [])
        text = "".join(part.get("text", "") for part in parts if part.get("text"))
        if not text:
            raise LLMServiceError("LLM response candidate did not include text content.")

        return text

    def count_input_tokens(self, messages: list[LLMMessage]) -> int:
        if not self.api_key:
            raise LLMServiceError("LLM_API_KEY is not configured.")

        generate_payload = self._build_generate_payload(messages=messages)
        generate_payload["model"] = f"models/{self.model}"
        body = json.dumps({"generateContentRequest": generate_payload}).encode("utf-8")
        req = request.Request(
            url=f"{self.base_url}/models/{self.model}:countTokens",
            data=body,
            headers={
                "x-goog-api-key": self.api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_data = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise LLMServiceError(f"LLM token count failed ({exc.code}): {detail}") from exc
        except error.URLError as exc:
            raise LLMServiceError(f"LLM token count failed: {exc.reason}") from exc

        total_tokens = response_data.get("totalTokens")
        if total_tokens is None:
            raise LLMServiceError("LLM token count response did not include totalTokens.")
        return int(total_tokens)

    def generate(
        self,
        prompt: str,
        system_prompt: str = "You are a helpful assistant.",
        temperature: float = 0.2,
        max_tokens: int = 400,
    ) -> str:
        messages: list[LLMMessage] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
        return self.chat(messages=messages, temperature=temperature, max_tokens=max_tokens)

    def count_generate_input_tokens(
        self,
        prompt: str,
        system_prompt: str = "You are a helpful assistant.",
    ) -> int:
        messages: list[LLMMessage] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
        return self.count_input_tokens(messages=messages)

    def _build_generate_payload(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.2,
        max_tokens: int = 400,
    ) -> dict:
        system_parts = [
            {"text": message["content"]}
            for message in messages
            if message["role"] == "system"
        ]
        contents = [
            {
                "role": self._map_role(message["role"]),
                "parts": [{"text": message["content"]}],
            }
            for message in messages
            if message["role"] != "system"
        ]

        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }
        if system_parts:
            payload["system_instruction"] = {"parts": system_parts}
        return payload

    @staticmethod
    def _map_role(role: str) -> str:
        if role == "assistant":
            return "model"
        return "user"
