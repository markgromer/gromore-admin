"""Provider-aware AI helpers.

This module keeps the current OpenAI setup working while allowing
OpenAI-compatible providers such as OpenRouter to be enabled per brand.
"""

import base64
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from flask import current_app


OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_IMAGES_URL = "https://api.openai.com/v1/images/generations"
OPENAI_IMAGE_EDITS_URL = "https://api.openai.com/v1/images/edits"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
XAI_BASE_URL = "https://api.x.ai/v1"
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

OPENAI_MODELS_BY_PURPOSE = {
    "chat": "gpt-4o-mini",
    "analysis": "gpt-4o-mini",
    "ads": "gpt-4o",
    "images": "gpt-image-2",
}

OPENROUTER_MODELS_BY_PURPOSE = {
    "chat": "openai/gpt-4o-mini",
    "analysis": "openai/gpt-4o-mini",
    "ads": "openai/gpt-4o",
    "images": "",
}

PROVIDER_DEFAULT_MODELS = {
    "gemini": {"chat": "gemini-3-flash-preview", "analysis": "gemini-3-flash-preview", "ads": "gemini-3-flash-preview", "images": "gemini-3.1-flash-image-preview"},
    "xai": {"chat": "grok-4.20", "analysis": "grok-4.20", "ads": "grok-4.20", "images": "grok-imagine-image"},
    "bfl": {"images": "flux-2-pro-preview"},
}


@dataclass
class AIProviderConfig:
    provider: str
    api_key: str
    base_url: str
    supports_images: bool


def normalize_provider(provider: str) -> str:
    provider = (provider or "openai").strip().lower()
    aliases = {"grok": "xai", "google": "gemini", "flux": "bfl"}
    provider = aliases.get(provider, provider)
    return provider if provider in {"openai", "openrouter", "gemini", "xai", "bfl", "custom"} else "openai"


def provider_label(provider: str) -> str:
    provider = normalize_provider(provider)
    return {
        "openai": "OpenAI",
        "openrouter": "OpenRouter",
        "gemini": "Google Gemini",
        "xai": "xAI / Grok",
        "bfl": "Black Forest Labs / FLUX",
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
            (brand.get("ai_openrouter_api_key") or "").strip()
            or (brand.get("ai_provider_api_key") or "").strip()
            or _app_setting("openrouter_api_key", "OPENROUTER_API_KEY")
        )
        return AIProviderConfig(provider=provider, api_key=api_key, base_url=OPENROUTER_BASE_URL, supports_images=False)

    if provider == "gemini":
        api_key = (
            (brand.get("ai_gemini_api_key") or "").strip()
            or _app_setting("gemini_api_key", "GEMINI_API_KEY")
            or _app_setting("google_api_key", "GOOGLE_API_KEY")
        )
        return AIProviderConfig(provider=provider, api_key=api_key, base_url=GEMINI_BASE_URL, supports_images=True)

    if provider == "xai":
        api_key = (
            (brand.get("ai_xai_api_key") or "").strip()
            or _app_setting("xai_api_key", "XAI_API_KEY")
        )
        return AIProviderConfig(provider=provider, api_key=api_key, base_url=XAI_BASE_URL, supports_images=True)

    if provider == "bfl":
        api_key = (
            (brand.get("ai_bfl_api_key") or "").strip()
            or _app_setting("bfl_api_key", "BFL_API_KEY")
        )
        return AIProviderConfig(provider=provider, api_key=api_key, base_url="https://api.bfl.ai", supports_images=True)

    if provider == "custom":
        api_key = (
            (brand.get("ai_custom_api_key") or "").strip()
            or (brand.get("ai_provider_api_key") or "").strip()
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


def get_image_provider_config(brand: Optional[Dict[str, Any]]) -> AIProviderConfig:
    brand = brand or {}
    image_provider = normalize_provider(brand.get("ai_image_provider") or brand.get("ai_provider") or "openai")
    if image_provider == "openrouter":
        image_provider = "openai"
    return get_provider_config({**brand, "ai_provider": image_provider})


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
    if provider in PROVIDER_DEFAULT_MODELS:
        defaults = PROVIDER_DEFAULT_MODELS[provider]
        return defaults.get(purpose) or defaults.get("chat") or OPENAI_MODELS_BY_PURPOSE["chat"]
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
    if cfg.provider == "xai":
        return f"{XAI_BASE_URL}/chat/completions"
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


def _gemini_contents(messages: List[Dict[str, Any]]) -> tuple[str, List[Dict[str, Any]]]:
    system_parts: List[str] = []
    contents: List[Dict[str, Any]] = []
    for message in messages:
        role = (message.get("role") or "user").strip().lower()
        text = message.get("content") or ""
        if role == "system":
            system_parts.append(str(text))
            continue
        contents.append({
            "role": "model" if role == "assistant" else "user",
            "parts": [{"text": str(text)}],
        })
    return "\n\n".join(system_parts), contents or [{"role": "user", "parts": [{"text": ""}]}]


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

    if cfg.provider == "gemini":
        model_name = model or pick_model(brand, purpose)
        system_instruction, contents = _gemini_contents(messages)
        payload: Dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"temperature": temperature},
        }
        if system_instruction:
            payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}
        url = f"{GEMINI_BASE_URL}/models/{model_name}:generateContent"
        resp = requests.post(url, params={"key": cfg.api_key}, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            raise ValueError(f"{provider_label(cfg.provider)} request failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        parts = (((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
        text = "".join(part.get("text", "") for part in parts)
        return {"choices": [{"message": {"content": text}}], "raw": data}

    if cfg.provider == "bfl":
        raise ValueError("Black Forest Labs is configured for image generation, not chat completions.")

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
    reference_images: Optional[List[Dict[str, Any]]] = None,
) -> bytes:
    cfg = get_image_provider_config(brand)
    if not cfg.api_key:
        raise ValueError(f"{provider_label(cfg.provider)} API key is not configured.")
    reference_images = [item for item in (reference_images or []) if item.get("bytes")]

    if cfg.provider == "xai":
        if reference_images:
            raise ValueError("Reference images are not supported for xAI image generation yet. Use OpenAI GPT Image or Gemini native image models for reference-based generation.")
        aspect_ratio = {"1024x1024": "1:1", "1792x1024": "16:9", "1536x1024": "16:9", "1024x1792": "9:16", "1024x1536": "9:16"}.get(size, "1:1")
        payload = {
            "model": model or "grok-imagine-image",
            "prompt": prompt,
            "n": 1,
            "aspect_ratio": aspect_ratio,
        }
        resp = requests.post(f"{XAI_BASE_URL}/images/generations", headers=_headers(cfg), json=payload, timeout=timeout)
        if resp.status_code >= 400:
            raise ValueError(f"xAI image request failed ({resp.status_code}): {resp.text[:300]}")
        first = (resp.json().get("data") or [{}])[0]
        if first.get("b64_json"):
            return base64.b64decode(first["b64_json"])
        if first.get("url"):
            img_resp = requests.get(first["url"], timeout=timeout)
            if img_resp.status_code >= 400:
                raise ValueError(f"Generated image download failed ({img_resp.status_code}).")
            return img_resp.content
        raise ValueError("xAI image response did not include image data.")

    if cfg.provider == "gemini":
        aspect_ratio = {"1024x1024": "1:1", "1792x1024": "16:9", "1536x1024": "16:9", "1024x1792": "9:16", "1024x1536": "9:16"}.get(size, "1:1")
        image_model = model or PROVIDER_DEFAULT_MODELS["gemini"]["images"]
        if image_model.startswith("gemini-"):
            ratio_note = f"\nRequested aspect ratio: {aspect_ratio}."
            parts: List[Dict[str, Any]] = [{"text": prompt + ratio_note}]
            for ref in reference_images[:4]:
                mime_type = ref.get("mime_type") or "image/png"
                parts.append({
                    "inlineData": {
                        "mimeType": mime_type,
                        "data": base64.b64encode(ref["bytes"]).decode("ascii"),
                    }
                })
            payload = {"contents": [{"parts": parts}]}
            url = f"{GEMINI_BASE_URL}/models/{image_model}:generateContent"
            resp = requests.post(url, params={"key": cfg.api_key}, json=payload, timeout=timeout)
            if resp.status_code >= 400:
                raise ValueError(f"Gemini image request failed ({resp.status_code}): {resp.text[:300]}")
            data = resp.json()
            parts = (((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
            for part in parts:
                inline_data = part.get("inlineData") or part.get("inline_data") or {}
                b64 = inline_data.get("data")
                if b64:
                    return base64.b64decode(b64)
            raise ValueError("Gemini image response did not include image data.")
        if reference_images:
            raise ValueError("Reference images require Gemini native image models, not Imagen. Select a Gemini image-preview model.")
        url = f"{GEMINI_BASE_URL}/models/{image_model}:predict"
        payload = {
            "instances": [{"prompt": prompt}],
            "parameters": {"sampleCount": 1, "aspectRatio": aspect_ratio},
        }
        resp = requests.post(url, params={"key": cfg.api_key}, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            raise ValueError(f"Gemini image request failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        first = (data.get("predictions") or data.get("generatedImages") or [{}])[0]
        b64 = first.get("bytesBase64Encoded") or first.get("image", {}).get("bytesBase64Encoded") or first.get("imageBytes")
        if b64:
            return base64.b64decode(b64)
        raise ValueError("Gemini image response did not include image data.")

    if cfg.provider == "bfl":
        if reference_images:
            raise ValueError("Reference images are not supported for the configured FLUX endpoint yet. Use OpenAI GPT Image or Gemini native image models for reference-based generation.")
        width, height = {
            "1024x1024": (1024, 1024),
            "1536x1024": (1536, 1024),
            "1792x1024": (1440, 1024),
            "1024x1536": (1024, 1536),
            "1024x1792": (1024, 1440),
        }.get(size, (1024, 1024))
        endpoint = (model or PROVIDER_DEFAULT_MODELS["bfl"]["images"]).strip().strip("/")
        payload = {"prompt": prompt, "width": width, "height": height}
        headers = {"accept": "application/json", "x-key": cfg.api_key, "Content-Type": "application/json"}
        resp = requests.post(f"{cfg.base_url.rstrip('/')}/v1/{endpoint}", headers=headers, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            raise ValueError(f"BFL image request failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        polling_url = data.get("polling_url")
        if not polling_url:
            raise ValueError("BFL image response did not include a polling URL.")

        deadline = time.time() + timeout
        while time.time() < deadline:
            poll = requests.get(polling_url, headers={"accept": "application/json", "x-key": cfg.api_key}, timeout=min(30, timeout))
            if poll.status_code >= 400:
                raise ValueError(f"BFL image polling failed ({poll.status_code}): {poll.text[:300]}")
            poll_data = poll.json()
            status = (poll_data.get("status") or "").strip().lower()
            if status == "ready":
                sample_url = ((poll_data.get("result") or {}).get("sample") or "").strip()
                if not sample_url:
                    raise ValueError("BFL image result did not include an image URL.")
                img_resp = requests.get(sample_url, timeout=timeout)
                if img_resp.status_code >= 400:
                    raise ValueError(f"Generated image download failed ({img_resp.status_code}).")
                return img_resp.content
            if status in {"error", "failed"}:
                raise ValueError(f"BFL image generation failed: {json.dumps(poll_data)[:300]}")
            time.sleep(0.75)
        raise ValueError("BFL image generation timed out before the image was ready.")

    if cfg.provider != "openai":
        raise ValueError(f"Image generation is not supported for {provider_label(cfg.provider)} yet.")

    selected_model = model or pick_model(brand, "images") or "gpt-image-2"
    if reference_images:
        if not selected_model.startswith("gpt-image") and selected_model != "chatgpt-image-latest":
            raise ValueError("Reference images require a GPT Image model. Select GPT Image 1.5, GPT Image 1, or ChatGPT Image Latest.")
        data = {
            "model": selected_model,
            "prompt": prompt,
            "size": size,
            "n": "1",
        }
        files = [
            (
                "image[]",
                (
                    ref.get("filename") or f"reference_{idx + 1}.png",
                    ref["bytes"],
                    ref.get("mime_type") or "image/png",
                ),
            )
            for idx, ref in enumerate(reference_images[:4])
        ]
        edit_headers = {"Authorization": f"Bearer {cfg.api_key}"}
        resp = requests.post(OPENAI_IMAGE_EDITS_URL, headers=edit_headers, data=data, files=files, timeout=timeout)
        if resp.status_code >= 400:
            files = [
                (
                    "image",
                    (
                        ref.get("filename") or f"reference_{idx + 1}.png",
                        ref["bytes"],
                        ref.get("mime_type") or "image/png",
                    ),
                )
                for idx, ref in enumerate(reference_images[:4])
            ]
            resp = requests.post(OPENAI_IMAGE_EDITS_URL, headers=edit_headers, data=data, files=files, timeout=timeout)
        if resp.status_code >= 400:
            raise ValueError(f"OpenAI image edit request failed ({resp.status_code}): {resp.text[:300]}")
        first = (resp.json().get("data") or [{}])[0]
        if first.get("b64_json"):
            return base64.b64decode(first["b64_json"])
        if first.get("url"):
            img_resp = requests.get(first["url"], timeout=timeout)
            if img_resp.status_code >= 400:
                raise ValueError(f"Generated image download failed ({img_resp.status_code}).")
            return img_resp.content
        raise ValueError("OpenAI image edit response did not include image data.")

    payload = {
        "model": selected_model,
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
