import json
from typing import Any, Dict

from app.services.llm_service import LLMService, LLMServiceError


class LLMJsonAgent:
    def __init__(self, llm_service: LLMService) -> None:
        self.llm_service = llm_service

    def count_input_tokens(
        self,
        prompt: str,
        system_prompt: str,
        max_tokens: int = 900,
    ) -> int:
        return self.llm_service.count_generate_input_tokens(
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
        )

    def generate_json(
        self,
        prompt: str,
        system_prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 900,
    ) -> Dict[str, Any]:
        raw_output = self.llm_service.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return self.parse_json_object(raw_output)

    @staticmethod
    def parse_json_object(text: str) -> Dict[str, Any]:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise LLMServiceError("LLM response did not include valid JSON.")
            data = json.loads(text[start : end + 1])

        if not isinstance(data, dict):
            raise LLMServiceError("LLM response JSON must be an object.")
        return data

    @staticmethod
    def normalize_string_list(value: Any, default_item: str) -> list[str]:
        if isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            if items:
                return items
        if isinstance(value, str) and value.strip():
            return [value.strip()]
        return [default_item]

    @staticmethod
    def normalize_priority(value: Any) -> str:
        priority = str(value or "").strip().lower()
        if priority in {"high", "medium", "low"}:
            return priority
        return "medium"

    @staticmethod
    def normalize_json_value(value: Any) -> Any:
        return json.loads(json.dumps(value, default=str))
