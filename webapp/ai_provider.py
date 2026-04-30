"""Provider-aware AI helpers.

This module keeps the current OpenAI setup working while allowing
OpenAI-compatible providers such as OpenRouter to be enabled per brand.
"""

import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from flask import current_app


OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_IMAGES_URL = "https://api.openai.com/v1/images/generations"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

OPENAI_MODELS_BY_PURPOSE = {
    "chat": "gpt-4o-mini",
    "analysis": "gpt-4o-mini",
    "ads": "gpt-4o",
    "images": "dall-e-3",
}

OPENROUTER_MODELS_BY_PURPOSE = {
    "chat": "openai/gpt-4o-mini",
    "analysis": "openai/gpt-4o-mini",
    "ads": "openai/gpt-4o",
    "images": "",
}


@dataclass
class AIProviderConfig:
    provider: str
    api_key: str
    base_url: str
    supports_images: bool


def normalize_provider(provider: str) -> str:
    provider = (provider or "openai").strip().lower()
    return provider if provider in {"openai", "openrouter", "custom"} else "openai"


def provider_label(provider: str) -> str:
    provider = normalize_provider(provider)
    return {
        "openai": "OpenAI",
        "openrouter": "OpenRouter",
        "custom": "Custom OpenAI-compatible",
    }.get(provider, "OpenAI")


def _app_setting(key: str, env_key: str = "", default: str = "") -> str:
    try:
        db = getattr(current_app, "db", None)
        if db:
            value = (db.get_setting(key, "") or "").strip()
            if value:
                return value
        value = (current_app.config.get(env_key or key.upper(), "") or "").strip()
        if value:
            return value
    except RuntimeError:
        pass
    return (os.environ.get(env_key or key.upper(), default) or default).strip()


def get_provider_config(brand: Optional[Dict[str, Any]]) -> AIProviderConfig:
    brand = brand or {}
    provider = normalize_provider(brand.get("ai_provider") or "openai")

    if provider == "openrouter":
        api_key = (
            (brand.get("ai_provider_api_key") or "").strip()
            or _app_setting("openrouter_api_key", "OPENROUTER_API_KEY")
        )
        return AIProviderConfig(
            provider=provider,
            api_key=api_key,
            base_url=OPENROUTER_BASE_URL,
            supports_images=False,
        )

    if provider == "custom":
        api_key = (
            (brand.get("ai_provider_api_key") or "").strip()
            or _app_setting("ai_provider_api_key", "AI_PROVIDER_API_KEY")
        )
        base_url = (
            (brand.get("ai_provider_base_url") or "").strip().rstrip("/")
            or _app_setting("ai_provider_base_url", "AI_PROVIDER_BASE_URL").rstrip("/")
        )
        return AIProviderConfig(
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            supports_images=False,
        )

    api_key = (
        (brand.get("openai_api_key") or "").strip()
        or _app_setting("openai_api_key", "OPENAI_API_KEY")
    )
    return AIProviderConfig(
        provider="openai",
        api_key=api_key,
        base_url="https://api.openai.com/v1",
        supports_images=True,
    )


def is_configured(brand: Optional[Dict[str, Any]]) -> bool:
    cfg = get_provider_config(brand)
    if not cfg.api_key:
        return False
    if cfg.provider == "custom" and not cfg.base_url:
        return False
    return True


def pick_model(brand: Optional[Dict[str, Any]], purpose: str = "chat") -> str:
    brand = brand or {}
    purpose = (purpose or "chat").strip().lower()
    if purpose == "images":
        image_model = (brand.get("ai_image_generation_model") or "").strip()
        if image_model:
            return image_model
    field_map = {
        "chat": "openai_model_chat",
        "analysis": "openai_model_analysis",
        "ads": "openai_model_ads",
        "images": "openai_model_images",
    }
    field = field_map.get(purpose, "openai_model_chat")
    model = ((brand.get(field) or "").strip() or (brand.get("openai_model") or "").strip())
    if model:
        return model

    provider = normalize_provider(brand.get("ai_provider") or "openai")
    if provider == "openrouter":
        return OPENROUTER_MODELS_BY_PURPOSE.get(purpose) or OPENROUTER_MODELS_BY_PURPOSE["chat"]
    return OPENAI_MODELS_BY_PURPOSE.get(purpose) or OPENAI_MODELS_BY_PURPOSE["chat"]


def normalize_model_for_provider(provider: str, model: str) -> str:
    provider = normalize_provider(provider)
    model = (model or "").strip()
    if provider == "openrouter" and model and "/" not in model:
        openai_prefixes = ("gpt-", "o1", "o3", "o4", "dall-e")
        if model.startswith(openai_prefixes):
            return f"openai/{model}"
    return model


def _chat_url(cfg: AIProviderConfig) -> str:
    if cfg.provider == "openai":
        return OPENAI_CHAT_URL
    return f"{cfg.base_url.rstrip('/')}/chat/completions"


def _headers(cfg: AIProviderConfig) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    if cfg.provider == "openrouter":
        app_url = _app_setting("app_url", "APP_URL")
        if app_url:
            headers["HTTP-Referer"] = app_url
        headers["X-OpenRouter-Title"] = "GroMore"
    return headers


def chat_completion(
    brand: Optional[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    *,
    purpose: str = "chat",
    model: str = "",
    temperature: float = 0.5,
    response_format: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    cfg = get_provider_config(brand)
    if not cfg.api_key:
        raise ValueError(f"{provider_label(cfg.provider)} API key is not configured.")
    if cfg.provider == "custom" and not cfg.base_url:
        raise ValueError("Custom AI provider base URL is not configured.")

    payload: Dict[str, Any] = {
        "model": normalize_model_for_provider(cfg.provider, model or pick_model(brand, purpose)),
        "messages": messages,
        "temperature": temperature,
    }
    if response_format:
        payload["response_format"] = response_format

    resp = requests.post(_chat_url(cfg), headers=_headers(cfg), json=payload, timeout=timeout)
    if resp.status_code >= 400:
        raise ValueError(f"{provider_label(cfg.provider)} request failed ({resp.status_code}): {resp.text[:300]}")
    return resp.json()


def chat_text(
    brand: Optional[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    *,
    purpose: str = "chat",
    model: str = "",
    temperature: float = 0.5,
    timeout: int = 60,
) -> str:
    data = chat_completion(
        brand,
        messages,
        purpose=purpose,
        model=model,
        temperature=temperature,
        timeout=timeout,
    )
    return ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "") or ""


def chat_json(
    brand: Optional[Dict[str, Any]],
    system: str,
    user_content: str,
    *,
    purpose: str = "chat",
    model: str = "",
    temperature: float = 0.5,
    timeout: int = 60,
) -> Optional[Dict[str, Any]]:
    content = chat_text(
        brand,
        [
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ],
        purpose=purpose,
        model=model,
        temperature=temperature,
        timeout=timeout,
    ).strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3]
    return json.loads(content)


def generate_image_bytes(
    brand: Optional[Dict[str, Any]],
    prompt: str,
    *,
    model: str = "",
    size: str = "1024x1024",
    timeout: int = 120,
) -> bytes:
    cfg = get_provider_config(brand)
    if cfg.provider != "openai":
        raise ValueError("Image generation is currently enabled for OpenAI image models. Text models can use OpenRouter or a custom provider.")
    if not cfg.api_key:
        raise ValueError("OpenAI API key is not configured.")

    payload = {
        "model": model or pick_model(brand, "images") or "dall-e-3",
        "prompt": prompt,
        "size": size,
        "n": 1,
        "response_format": "b64_json",
    }
    resp = requests.post(OPENAI_IMAGES_URL, headers=_headers(cfg), json=payload, timeout=timeout)
    if resp.status_code >= 400 and "response_format" in payload:
        payload.pop("response_format", None)
        resp = requests.post(OPENAI_IMAGES_URL, headers=_headers(cfg), json=payload, timeout=timeout)
    if resp.status_code >= 400:
        raise ValueError(f"OpenAI image request failed ({resp.status_code}): {resp.text[:300]}")

    data = resp.json()
    first = (data.get("data") or [{}])[0]
    if first.get("b64_json"):
        return base64.b64decode(first["b64_json"])
    if first.get("url"):
        img_resp = requests.get(first["url"], timeout=timeout)
        if img_resp.status_code >= 400:
            raise ValueError(f"Generated image download failed ({img_resp.status_code}).")
        return img_resp.content
    raise ValueError("OpenAI image response did not include image data.")
