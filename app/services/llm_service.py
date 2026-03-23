import json
import math
import re
from typing import TypedDict
from urllib import error, request

from app.core.config import get_settings


TOKEN_RE = re.compile(r"\w+|[^\w\s]", re.UNICODE)
TRUNCATION_MARKER = "\n...[truncated]...\n"


class LLMMessage(TypedDict):
    role: str
    content: str


class LLMServiceError(RuntimeError):
    pass


class LLMService:
    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.llm_base_url.rstrip("/")
        self.model = settings.llm_model
        self.timeout_seconds = settings.llm_timeout_seconds
        self.context_window = settings.llm_context_window
        self.max_input_tokens = settings.llm_max_input_tokens
        self.max_output_tokens = settings.llm_max_output_tokens
        self.response_buffer_tokens = settings.llm_response_buffer_tokens

    def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.2,
        max_tokens: int = 400,
    ) -> str:
        output_tokens = self._clamp_output_tokens(max_tokens)
        prepared_messages, _ = self._prepare_messages(messages=messages, max_tokens=output_tokens)
        payload = {
            "model": self.model,
            "messages": prepared_messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": output_tokens,
                "num_ctx": self.context_window,
            },
        }

        response_data = self._post_json("/api/chat", payload)
        text = self._extract_text_content(response_data)
        if not text:
            raise LLMServiceError(self._build_empty_content_error(response_data))
        return text

    def count_input_tokens(
        self,
        messages: list[LLMMessage],
        max_tokens: int | None = None,
    ) -> int:
        _, input_tokens = self._prepare_messages(messages=messages, max_tokens=max_tokens)
        return input_tokens

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
        max_tokens: int | None = None,
    ) -> int:
        messages: list[LLMMessage] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
        return self.count_input_tokens(messages=messages, max_tokens=max_tokens)

    def _prepare_messages(
        self,
        messages: list[LLMMessage],
        max_tokens: int | None,
    ) -> tuple[list[dict[str, str]], int]:
        normalized = [
            {
                "role": self._map_role(message["role"]),
                "content": self._normalize_content(message["content"]),
            }
            for message in messages
            if self._normalize_content(message["content"])
        ]
        if not normalized:
            raise LLMServiceError("LLM request did not include any message content.")

        input_budget = self._input_budget(max_tokens)
        prepared: list[dict[str, str]] = []

        system_messages = [message for message in normalized if message["role"] == "system"]
        chat_messages = [message for message in normalized if message["role"] != "system"]

        used_tokens = self._append_fitted_messages(
            target=prepared,
            messages=system_messages,
            budget_tokens=input_budget,
            keep_latest=False,
        )

        remaining_tokens = max(64, input_budget - used_tokens)
        used_tokens += self._append_fitted_messages(
            target=prepared,
            messages=chat_messages,
            budget_tokens=remaining_tokens,
            keep_latest=True,
        )

        if not prepared:
            raise LLMServiceError("LLM request could not fit any content into the configured token budget.")

        return prepared, used_tokens

    def _append_fitted_messages(
        self,
        target: list[dict[str, str]],
        messages: list[dict[str, str]],
        budget_tokens: int,
        keep_latest: bool,
    ) -> int:
        if budget_tokens <= 0 or not messages:
            return 0

        fitted: list[dict[str, str]] = []
        used_tokens = 0
        ordered_messages = list(reversed(messages)) if keep_latest else list(messages)

        for index, message in enumerate(ordered_messages):
            remaining_tokens = budget_tokens - used_tokens
            if remaining_tokens <= 0:
                break

            content = message["content"]
            content_tokens = self._estimate_tokens(content)

            if content_tokens > remaining_tokens:
                must_include = keep_latest and index == 0
                if not must_include and fitted:
                    continue

                truncated = self._truncate_text(content, remaining_tokens)
                if not truncated:
                    continue
                content = truncated
                content_tokens = self._estimate_tokens(content)

            fitted.append({"role": message["role"], "content": content})
            used_tokens += content_tokens

        if keep_latest:
            fitted.reverse()

        target.extend(fitted)
        return used_tokens

    def _input_budget(self, max_tokens: int | None) -> int:
        output_tokens = self._clamp_output_tokens(max_tokens or self.max_output_tokens)
        return max(
            512,
            min(
                self.max_input_tokens,
                self.context_window - output_tokens - self.response_buffer_tokens,
            ),
        )

    def _clamp_output_tokens(self, max_tokens: int) -> int:
        return max(64, min(max_tokens, self.max_output_tokens))

    def _truncate_text(self, text: str, budget_tokens: int) -> str:
        if budget_tokens <= 0:
            return ""

        if self._estimate_tokens(text) <= budget_tokens:
            return text

        if budget_tokens <= 32:
            return text[: max(32, budget_tokens * 4)].strip()

        char_budget = max(120, budget_tokens * 4)
        while char_budget >= 120:
            head_chars = max(80, int(char_budget * 0.75))
            tail_chars = max(0, char_budget - head_chars - len(TRUNCATION_MARKER))
            if tail_chars:
                candidate = f"{text[:head_chars]}{TRUNCATION_MARKER}{text[-tail_chars:]}"
            else:
                candidate = text[:char_budget]

            if self._estimate_tokens(candidate) <= budget_tokens:
                return candidate.strip()
            char_budget = int(char_budget * 0.85)

        return text[: max(80, budget_tokens * 3)].strip()

    def _post_json(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise LLMServiceError(f"LLM request failed ({exc.code}): {detail}") from exc
        except error.URLError as exc:
            raise LLMServiceError(f"LLM request failed: {exc.reason}") from exc

    @staticmethod
    def _normalize_content(content: str) -> str:
        return content.strip()

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        regex_tokens = len(TOKEN_RE.findall(text))
        char_tokens = math.ceil(len(text) / 4)
        return max(regex_tokens, char_tokens)

    @staticmethod
    def _map_role(role: str) -> str:
        if role in {"system", "assistant", "user"}:
            return role
        return "user"

    @staticmethod
    def _extract_text_content(response_data: dict) -> str:
        message = response_data.get("message") or {}
        candidates = [
            message.get("content"),
            response_data.get("response"),
        ]
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return ""

    @staticmethod
    def _build_empty_content_error(response_data: dict) -> str:
        message = response_data.get("message") or {}
        message_keys = sorted(message.keys()) if isinstance(message, dict) else []
        return (
            "LLM response did not include text content. "
            f"done={response_data.get('done')} "
            f"done_reason={response_data.get('done_reason')} "
            f"prompt_eval_count={response_data.get('prompt_eval_count')} "
            f"eval_count={response_data.get('eval_count')} "
            f"message_keys={message_keys}"
        )
