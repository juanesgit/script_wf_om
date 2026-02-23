"""
workforce_client.py
───────────────────
Módulo HTTP para interactuar con Oracle Field Service Cloud (OFSC).
Reemplaza el scraping Selenium por requests directos.

Flujo:
  1. selenium_login_and_save_cookies()  → Login con Selenium UNA sola vez, guarda cookies.
  2. build_session()                    → Crea requests.Session con cookies guardadas.
  3. download_csv() / download_all()    → Descarga CSVs vía HTTP GET.
  4. discover_providers()               → (Opcional) Extrae provider_ids del DOM.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
COOKIES_FILE = BASE_DIR / "cookies.json"
USER_AGENT_FILE = BASE_DIR / "user_agent.txt"
PROVIDERS_FILE = BASE_DIR / "providers_om.json"


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _normalize_url(url: str) -> str:
    u = url.strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    if not u.endswith("/"):
        u += "/"
    return u


def build_export_url(base_url: str, provider_id: int, day_str: str) -> str:
    ts = int(time.time() * 1000)
    base = _normalize_url(base_url)
    return (
        f"{base}?m=gridexport&a=download&itype=manage"
        f"&providerId={provider_id}"
        f"&date={day_str}"
        f"&panel=top&view=time"
        f"&dates={day_str}"
        f"&recursively=1"
        f"&_={ts}"
    )


# ─── Chrome / Selenium (solo para login y discovery) ─────────────────────────

def _prepare_chrome_data_dir() -> str:
    if os.name != "nt":
        p = Path("/tmp/chrome-data-om")
    else:
        p = Path(tempfile.gettempdir()) / "chrome-data-om"
    try:
        shutil.rmtree(p, ignore_errors=True)
    except Exception:
        pass
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def _create_chrome(
    headless: bool = True,
    chrome_path: Optional[str] = None,
    chromedriver_path: Optional[str] = None,
):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-geolocation")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-extensions")
    try:
        ud = _prepare_chrome_data_dir()
        options.add_argument(f"--user-data-dir={ud}")
    except Exception:
        pass

    if chrome_path and os.path.exists(chrome_path):
        options.binary_location = chrome_path

    # Intentar primero sin driver explícito (Selenium Manager)
    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except Exception:
        pass

    # Fallback con driver explícito
    service = None
    if chromedriver_path and os.path.exists(chromedriver_path):
        service = Service(chromedriver_path)
    elif os.name != "nt":
        pth = shutil.which("chromedriver") or "chromedriver"
        service = Service(pth)

    driver = webdriver.Chrome(service=service, options=options)
    return driver


def selenium_login_and_save_cookies(
    base_url: str,
    usuario: str,
    clave: str,
    headless: bool = True,
    timeout: int = 100,
    chrome_path: Optional[str] = None,
    chromedriver_path: Optional[str] = None,
) -> None:
    """Inicia sesión en Workforce con Selenium y guarda cookies en disco."""
    driver = _create_chrome(headless=headless, chrome_path=chrome_path, chromedriver_path=chromedriver_path)
    try:
        url = _normalize_url(base_url)
        log.info("[LOGIN] Abriendo %s", url)
        driver.get(url)
        wait = WebDriverWait(driver, timeout, ignored_exceptions=(StaleElementReferenceException,))
        max_logins = 6
        attempt = 0
        success = False

        while attempt < max_logins and not success:
            try:
                log.info("[LOGIN] Intento %d/%d", attempt + 1, max_logins)
                user_el = wait.until(EC.element_to_be_clickable((By.ID, "username")))
                pass_el = wait.until(EC.element_to_be_clickable((By.ID, "password")))
                user_el.clear(); user_el.send_keys(usuario)
                pass_el.clear(); pass_el.send_keys(clave)

                btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Iniciar']")))
                try:
                    driver.execute_script("arguments[0].click();", btn)
                except Exception:
                    btn.click()

                # Manejar sesión vieja
                try:
                    sesion_vieja = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.ID, "del-oldest-session"))
                    )
                except Exception:
                    sesion_vieja = None

                if sesion_vieja:
                    log.info("[LOGIN] Sesión anterior detectada, reemplazando...")
                    try:
                        elem = WebDriverWait(driver, timeout).until(
                            EC.element_to_be_clickable((By.ID, "del-oldest-session"))
                        )
                        driver.execute_script("arguments[0].click();", elem)
                    except Exception:
                        driver.find_element(By.ID, "del-oldest-session").click()

                    pass_el = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.ID, "password"))
                    )
                    pass_el.clear(); pass_el.send_keys(clave)
                    try:
                        signin = WebDriverWait(driver, timeout).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "button#sign-in>div"))
                        )
                        driver.execute_script("arguments[0].click();", signin)
                    except Exception:
                        driver.find_element(By.CSS_SELECTOR, "button#sign-in>div").click()

                # Verificar login exitoso
                try:
                    boton_vista = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.XPATH, "(//button[@title='Vista']//span)[2]"))
                    )
                    if boton_vista:
                        try:
                            driver.execute_script("arguments[0].click();", boton_vista)
                        except Exception:
                            boton_vista.click()
                        success = True
                        log.info("[LOGIN] Sesión iniciada correctamente")
                except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                    raise

            except (NoSuchElementException, TimeoutException, StaleElementReferenceException) as e:
                attempt += 1
                log.warning("[LOGIN] Intento %d/%d fallido: %s", attempt, max_logins, e.__class__.__name__)
                try:
                    driver.refresh()
                except Exception:
                    pass
                time.sleep(2 + random.uniform(0, 1.5))

        if not success:
            raise RuntimeError("No se pudo iniciar sesión en Workforce")

        # Guardar cookies y user-agent
        cookies = driver.get_cookies()
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
        ua = driver.execute_script("return navigator.userAgent;")
        USER_AGENT_FILE.write_text(ua or "", encoding="utf-8")
        log.info("[LOGIN] Cookies y user-agent guardados")
    finally:
        driver.quit()


# ─── Auto-descubrimiento de Provider IDs ─────────────────────────────────────

def discover_providers(
    base_url: str,
    usuario: str,
    clave: str,
    target_names: List[str],
    headless: bool = True,
    timeout: int = 100,
    chrome_path: Optional[str] = None,
    chromedriver_path: Optional[str] = None,
) -> Dict[int, str]:
    """
    Inicia sesión en Workforce y extrae los provider_ids del panel lateral
    buscando los nombres de cubos especificados en target_names.
    
    Retorna dict {provider_id: nombre} y lo guarda en providers_om.json.
    """
    driver = _create_chrome(headless=headless, chrome_path=chrome_path, chromedriver_path=chromedriver_path)
    try:
        url = _normalize_url(base_url)
        log.info("[DISCOVER] Abriendo %s", url)
        driver.get(url)
        wait = WebDriverWait(driver, timeout, ignored_exceptions=(StaleElementReferenceException,))

        # Login
        max_logins = 4
        attempt = 0
        success = False
        while attempt < max_logins and not success:
            try:
                user_el = wait.until(EC.element_to_be_clickable((By.ID, "username")))
                pass_el = wait.until(EC.element_to_be_clickable((By.ID, "password")))
                user_el.clear(); user_el.send_keys(usuario)
                pass_el.clear(); pass_el.send_keys(clave)
                btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Iniciar']")))
                try:
                    driver.execute_script("arguments[0].click();", btn)
                except Exception:
                    btn.click()
                try:
                    sesion_vieja = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.ID, "del-oldest-session"))
                    )
                except Exception:
                    sesion_vieja = None
                if sesion_vieja:
                    try:
                        elem = WebDriverWait(driver, timeout).until(
                            EC.element_to_be_clickable((By.ID, "del-oldest-session"))
                        )
                        driver.execute_script("arguments[0].click();", elem)
                    except Exception:
                        driver.find_element(By.ID, "del-oldest-session").click()
                    pass_el = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.ID, "password"))
                    )
                    pass_el.clear(); pass_el.send_keys(clave)
                    try:
                        signin = WebDriverWait(driver, timeout).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "button#sign-in>div"))
                        )
                        driver.execute_script("arguments[0].click();", signin)
                    except Exception:
                        driver.find_element(By.CSS_SELECTOR, "button#sign-in>div").click()
                boton_vista = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((By.XPATH, "(//button[@title='Vista']//span)[2]"))
                )
                if boton_vista:
                    driver.execute_script("arguments[0].click();", boton_vista)
                    success = True
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                attempt += 1
                try:
                    driver.refresh()
                except Exception:
                    pass
                time.sleep(2)

        if not success:
            raise RuntimeError("No se pudo iniciar sesión para descubrir providers")

        # Esperar a que cargue el panel de recursos
        time.sleep(5)

        # Extraer provider IDs del DOM
        # OFSC usa spans con texto del nombre del cubo; el providerId está en atributos data-* o en la URL
        # Estrategia: buscar todos los elementos <span> en el panel lateral y extraer sus IDs padre
        found: Dict[int, str] = {}

        # Guardar cookies también durante discovery
        cookies = driver.get_cookies()
        COOKIES_FILE.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
        ua = driver.execute_script("return navigator.userAgent;")
        USER_AGENT_FILE.write_text(ua or "", encoding="utf-8")

        for name in target_names:
            try:
                # Buscar el span con el texto exacto del cubo
                spans = driver.find_elements(By.XPATH, f"//span[text()='{name}']")
                for span in spans:
                    # Navegar hacia arriba para encontrar el elemento con data-provider-id o similar
                    try:
                        parent = span.find_element(By.XPATH, "./ancestor::*[@data-providerid]")
                        pid = parent.get_attribute("data-providerid")
                        if pid and pid.isdigit():
                            found[int(pid)] = name
                            log.info("[DISCOVER] %s -> providerId=%s", name, pid)
                            break
                    except NoSuchElementException:
                        pass

                    # Alternativa: buscar en onclick o href
                    try:
                        parent = span.find_element(By.XPATH, "./..")
                        onclick = parent.get_attribute("onclick") or ""
                        href = parent.get_attribute("href") or ""
                        for attr_val in [onclick, href]:
                            match = re.search(r"providerId[=:](\d+)", attr_val)
                            if match:
                                pid_int = int(match.group(1))
                                found[pid_int] = name
                                log.info("[DISCOVER] %s -> providerId=%d (via attr)", name, pid_int)
                                break
                    except Exception:
                        pass

                    # Alternativa: hacer clic y capturar la URL resultante
                    try:
                        driver.execute_script("arguments[0].click();", span)
                        time.sleep(2)
                        current_url = driver.current_url
                        match = re.search(r"providerId=(\d+)", current_url)
                        if match:
                            pid_int = int(match.group(1))
                            found[pid_int] = name
                            log.info("[DISCOVER] %s -> providerId=%d (via URL)", name, pid_int)
                    except Exception:
                        pass

                if name not in found.values():
                    log.warning("[DISCOVER] No se encontró providerId para: %s", name)
            except Exception as e:
                log.warning("[DISCOVER] Error buscando %s: %s", name, e)

        # Guardar resultado
        if found:
            PROVIDERS_FILE.write_text(
                json.dumps({str(k): v for k, v in found.items()}, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            log.info("[DISCOVER] Guardado %d providers en %s", len(found), PROVIDERS_FILE)
        else:
            log.warning("[DISCOVER] No se encontraron providers. Revisa los nombres de cubos.")

        return found
    finally:
        driver.quit()


def load_providers_from_file() -> Dict[int, str]:
    """Carga providers desde providers_om.json si existe."""
    if PROVIDERS_FILE.exists():
        data = json.loads(PROVIDERS_FILE.read_text(encoding="utf-8"))
        return {int(k): v for k, v in data.items()}
    return {}


# ─── Sesión HTTP con cookies ─────────────────────────────────────────────────

def build_session(base_url: str) -> requests.Session:
    """Crea una requests.Session con las cookies guardadas del login Selenium."""
    s = requests.Session()
    if USER_AGENT_FILE.exists():
        s.headers["User-Agent"] = USER_AGENT_FILE.read_text(encoding="utf-8").strip()
    else:
        s.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0 Safari/537.36"
        )
    s.headers["Referer"] = _normalize_url(base_url)
    if COOKIES_FILE.exists():
        try:
            cookies = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
            for c in cookies:
                s.cookies.set(
                    c.get("name"), c.get("value"),
                    domain=c.get("domain"), path=c.get("path", "/"),
                )
        except Exception:
            pass
    return s


def is_csv_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    dispo = (resp.headers.get("Content-Disposition") or "").lower()
    if ("text/csv" in ctype) or ("attachment" in dispo and "csv" in dispo):
        return True
    sample = resp.content[:512].decode(errors="ignore")
    if "<html" in sample.lower() or "<body" in sample.lower():
        return False
    return (("," in sample) or (";" in sample)) and ("\n" in sample)


def safe_get(
    session: requests.Session,
    url: str,
    max_attempts: int = 5,
    timeout: int = 120,
) -> requests.Response:
    """GET con reintentos y backoff exponencial."""
    base_wait = 2.0
    for attempt in range(1, max_attempts + 1):
        r = session.get(url, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("Retry-After")
            if ra and ra.isdigit():
                wait = float(ra)
            else:
                wait = base_wait * (2 ** (attempt - 1)) + random.uniform(0, 1.5)
            log.warning("[HTTP] %d en intento %d, esperando %.1fs", r.status_code, attempt, wait)
            time.sleep(wait)
            continue
        if r.status_code in (401, 403):
            raise PermissionError(f"HTTP {r.status_code} - Sesión expirada o sin permisos")
        r.raise_for_status()
        return r
    raise RuntimeError(f"No se pudo obtener la URL tras {max_attempts} intentos")


def ensure_authenticated(
    session: requests.Session,
    base_url: str,
    test_provider: int,
) -> bool:
    """Verifica si la sesión HTTP sigue autenticada."""
    d = date.today().strftime("%Y-%m-%d")
    url = build_export_url(base_url, test_provider, d)
    try:
        r = safe_get(session, url, max_attempts=2, timeout=30)
    except Exception:
        return False
    return is_csv_response(r)


# ─── Descarga de CSVs ────────────────────────────────────────────────────────

def download_csv(
    session: requests.Session,
    base_url: str,
    provider_id: int,
    day: date,
    out_path: Path,
) -> None:
    """Descarga un CSV de actividades para un provider y día específicos."""
    dstr = day.strftime("%Y-%m-%d")
    url = build_export_url(base_url, provider_id, dstr)
    attempts = 3
    last_text_head = ""
    for i in range(1, attempts + 1):
        r = safe_get(session, url)
        if is_csv_response(r):
            tmp = out_path.with_suffix(out_path.suffix + ".part")
            tmp.write_bytes(r.content)
            tmp.replace(out_path)
            return
        last_text_head = (r.text[:500].lower() if r.text else "")[:120]
        log.warning("[CSV] Intento %d/%d: respuesta no CSV para provider %d, día %s", i, attempts, provider_id, dstr)
        time.sleep(1 + random.uniform(0, 1.5))
    raise RuntimeError(f"Respuesta no CSV para provider {provider_id} día {dstr}: {last_text_head}")


def download_all(
    session: requests.Session,
    base_url: str,
    provider_map: Dict[int, str],
    start_day: date,
    end_day: date,
    out_dir: Path,
    throttle_range: Tuple[float, float] = (0.6, 1.4),
) -> Tuple[int, int]:
    """
    Descarga CSVs para todos los providers en el rango de fechas.
    Retorna (descargados, errores).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    cur = start_day
    downloaded = 0
    errors = 0

    while cur <= end_day:
        for pid, label in provider_map.items():
            fname = f"Actividades-{label}_{cur.strftime('%d_%m_%y')}.csv"
            path = out_dir / fname
            log.info("[GET]  %s %s -> %s", cur.strftime("%Y-%m-%d"), label, fname)
            try:
                download_csv(session, base_url, pid, cur, path)
                size = path.stat().st_size if path.exists() else -1
                log.info("[OK]   %s %s guardado (%d bytes)", cur.strftime("%Y-%m-%d"), label, size)
                downloaded += 1
            except Exception as e:
                log.error("[ERROR] %s %s: %s", cur.strftime("%Y-%m-%d"), label, e)
                errors += 1
            time.sleep(random.uniform(*throttle_range))
        cur += timedelta(days=1)

    log.info("[DONE] descargados=%d, errores=%d", downloaded, errors)
    return downloaded, errors
