"""
script_wf_v2.py
───────────────
Orquestador principal: descarga actividades de Workforce (OFSC) vía HTTP
y las carga a MySQL. Reemplaza el scraping Selenium del script_wf_v1.py.

Uso:
    python script_wf_v2.py                  # Ejecuta con .env
    python script_wf_v2.py --discover       # Descubre provider_ids y sale
    python script_wf_v2.py --days 5         # Override de días a descargar
    python script_wf_v2.py --headless 0     # Login con Chrome visible (debug)
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import shutil
import sys
import time
import warnings
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

import workforce_client as wf

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("automatizacion_v2.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

# ─── Configuración ────────────────────────────────────────────────────────────

load_dotenv()

URL_WORKFORCE = os.getenv("URL_WORKFORCE", "http://amx-res-co.etadirect.com")
USUARIO_WF = os.getenv("USUARIO_WORKFORCE", "")
CLAVE_WF = os.getenv("CLAVE_WORKFORCE", "")
HEADLESS = os.getenv("HEADLESS", "1") == "1"
DAYS = int(os.getenv("DAYS", "2"))
OUT_DIR = Path(os.getenv("DIRECTORIO_DESCARGAS", "./downloads"))

USUARIO_MYSQL = os.getenv("USUARIO_MYSQL", "ccot")
PASS_MYSQL = os.getenv("PASS_MYSQL", "ccot")
HOST_MYSQL = os.getenv("HOST_MYSQL", "10.108.34.32")
PUERTO_MYSQL = os.getenv("PUERTO_MYSQL", "33063")
DB_MYSQL = os.getenv("DB_MYSQL", "ccot")
TABLA_DESTINO = os.getenv("TABLA_DESTINO", "wf_futuro_pruebas_stage")
STORED_PROCEDURE = os.getenv("STORED_PROCEDURE", "actualizar_wf_om")

CHROME_PATH = os.getenv("CHROME_PATH", "")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SERVER_HOST = os.getenv("SERVER_HOST", "10.108.34.33")
SERVER_PORT = os.getenv("SERVER_PORT", "2222")
SERVER_USER = os.getenv("SERVER_USER", "root")
SERVER_PATH = os.getenv("SERVER_PATH", "~/script_wf_om/")

# Nombres de cubos O&M que se descargan (hoy y mañana)
# Solo se usan si no hay PROVIDER_IDS en .env ni providers_om.json (auto-discovery)
TARGET_CUBES = [
    "RECURSOS OCCIDENTE (INTEGRAL) SEG FIJA",
    "REGION OCCIDENTE",
    "R3",
    "RECURSOS OCCIDENTE CLIMA Y FUERZA ALIAD",
    "PYMES OCCIDENTE",
]

# Provider IDs por defecto (fallback si .env no tiene PROVIDER_IDS)
DEFAULT_PROVIDERS = {
    33577: "RECURSOS OCCIDENTE (INTEGRAL) SEG FIJA",
    5: "REGION OCCIDENTE",
    76781: "R3",
    77175: "RECURSOS OCCIDENTE CLIMA Y FUERZA ALIAD",
    4253: "PYMES OCCIDENTE",
}


# ─── Funciones auxiliares ─────────────────────────────────────────────────────

def limpiar_descargas(out_dir: Path) -> None:
    """Elimina todos los archivos de la carpeta de descargas."""
    if out_dir.exists():
        for item in out_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                log.warning("No se pudo eliminar %s: %s", item, e)
        log.info("Carpeta de descargas limpiada: %s", out_dir)
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        log.info("Carpeta de descargas creada: %s", out_dir)


def combinar_csvs(ruta_descargas: Path) -> pd.DataFrame:
    """Combina todos los CSVs descargados en un solo DataFrame."""
    archivos_csv = sorted(ruta_descargas.glob("*.csv"))
    if not archivos_csv:
        raise FileNotFoundError(f"No se encontraron CSVs en {ruta_descargas}")

    patron = re.compile(r"Actividades-(.+?)_\d{2}_\d{2}_\d{2}\.csv")
    todos = []

    for archivo in tqdm(archivos_csv, desc="Combinando CSVs"):
        try:
            df = pd.read_csv(archivo)
        except Exception as e:
            log.warning("Error leyendo %s: %s", archivo.name, e)
            continue

        coincidencia = patron.search(archivo.name)
        df["Origen"] = coincidencia.group(1) if coincidencia else "Origen_Desconocido"
        todos.append(df)

    if not todos:
        raise ValueError("No se pudo leer ningún CSV")

    resultado = pd.concat(todos, ignore_index=True)
    resultado["Fecha"] = pd.to_datetime(resultado["Fecha"], format="%d/%m/%y")
    log.info("CSVs combinados: %d filas, %d columnas", len(resultado), len(resultado.columns))
    return resultado


def cargar_a_mysql(datos: pd.DataFrame) -> None:
    """Trunca la tabla destino, carga datos y ejecuta el stored procedure."""
    conn_str = f"mysql+pymysql://{USUARIO_MYSQL}:{PASS_MYSQL}@{HOST_MYSQL}:{PUERTO_MYSQL}/{DB_MYSQL}"
    ok = False
    while not ok:
        engine = None
        try:
            engine = create_engine(conn_str)
            Session = sessionmaker(bind=engine)
            session = Session()

            # Contar filas existentes
            count = session.execute(text(f"SELECT COUNT(*) FROM {TABLA_DESTINO}")).scalar()
            log.info("Filas existentes en %s: %d", TABLA_DESTINO, count)

            # Truncar tabla
            session.execute(text(f"TRUNCATE TABLE {TABLA_DESTINO}"))
            session.commit()
            log.info("Tabla %s truncada", TABLA_DESTINO)

            # Cargar datos
            datos.to_sql(TABLA_DESTINO, con=engine, if_exists="append", index=False)
            log.info("Datos cargados: %d filas -> %s", len(datos), TABLA_DESTINO)

            # Ejecutar stored procedure
            if STORED_PROCEDURE:
                session.execute(text(f"CALL {STORED_PROCEDURE}(@result_message)"))
                result = session.execute(text("SELECT @result_message")).fetchone()
                session.commit()
                msg = result[0] if result else "sin mensaje"
                log.info("Stored procedure %s ejecutado: %s", STORED_PROCEDURE, msg)

            ok = True
        except Exception as e:
            log.error("Error cargando a MySQL: %s", e)
            log.info("Reintentando en 10 segundos...")
            time.sleep(10)
        finally:
            if engine:
                try:
                    engine.dispose()
                except Exception:
                    pass


def parse_provider_ids_env() -> dict[int, str]:
    """Parsea PROVIDER_IDS del .env. Formato: 'id:NOMBRE,id:NOMBRE,...'"""
    raw = os.getenv("PROVIDER_IDS", "").strip()
    if not raw:
        return {}
    result = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            pid_str, name = pair.split(":", 1)
            try:
                result[int(pid_str.strip())] = name.strip()
            except ValueError:
                pass
    return result


def obtener_providers(headless: bool) -> dict[int, str]:
    """
    Obtiene provider_ids en orden de prioridad:
    1. Variable de entorno PROVIDER_IDS
    2. Archivo providers_om.json (cache de discovery anterior)
    3. Auto-descubrimiento vía Selenium
    """
    # 1. Desde .env
    providers = parse_provider_ids_env()
    if providers:
        log.info("Providers cargados desde .env: %s", providers)
        return providers

    # 2. Desde archivo cache
    providers = wf.load_providers_from_file()
    if providers:
        log.info("Providers cargados desde %s: %s", wf.PROVIDERS_FILE, providers)
        return providers

    # 3. Defaults hardcodeados (provider_ids conocidos)
    if DEFAULT_PROVIDERS:
        log.info("Usando providers por defecto: %s", DEFAULT_PROVIDERS)
        return DEFAULT_PROVIDERS

    # 4. Auto-descubrimiento (último recurso)
    log.info("No hay providers configurados. Iniciando auto-descubrimiento...")
    providers = wf.discover_providers(
        base_url=URL_WORKFORCE,
        usuario=USUARIO_WF,
        clave=CLAVE_WF,
        target_names=TARGET_CUBES,
        headless=headless,
        chrome_path=CHROME_PATH or None,
        chromedriver_path=CHROMEDRIVER_PATH or None,
    )

    if not providers:
        log.error(
            "No se pudieron descubrir los provider_ids automáticamente.\n"
            "Por favor, configúralos manualmente en el .env:\n"
            "  PROVIDER_IDS=123:REGION OCCIDENTE,456:PYMES OCCIDENTE,...\n"
            "O edita providers_om.json directamente."
        )
        sys.exit(1)

    return providers


def ensure_session(providers: dict[int, str], headless: bool) -> wf.requests.Session:
    """Construye sesión HTTP, re-autenticando si es necesario."""
    session = wf.build_session(URL_WORKFORCE)
    test_pid = next(iter(providers))

    if wf.ensure_authenticated(session, URL_WORKFORCE, test_pid):
        log.info("Sesión HTTP válida (cookies vigentes)")
        return session

    log.info("Cookies expiradas o inválidas.")

    # Enviar alerta a Telegram
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        msg = (
            "⚠️ <b>Workforce O&M - Cookies Expiradas</b>\n\n"
            "Las cookies de sesión han expirado.\n"
            "El cron NO descargará datos hasta renovarlas.\n\n"
            "<b>Ejecuta en tu PC local:</b>\n"
            "<code>python script_wf_v2.py --login-manual</code>\n\n"
            "<b>Luego copia al servidor:</b>\n"
            "<code>scp -P 2222 cookies.json user_agent.txt root@10.108.34.33:~/script_wf_om/</code>"
        )
        wf.send_telegram_alert(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)

    log.error("═" * 60)
    log.error("Cookies expiradas. Alerta enviada a Telegram.")
    log.error("")
    log.error("SOLUCIÓN: Ejecuta en tu PC local:")
    log.error("  python script_wf_v2.py --login-manual")
    log.error("  (Abre Chrome para login manual con MFA de Microsoft)")
    log.error("")
    log.error("Luego copia las cookies al servidor:")
    log.error("  scp -P 2222 cookies.json user_agent.txt root@10.108.34.33:~/script_wf_om/")
    log.error("═" * 60)
    sys.exit(1)

    session = wf.build_session(URL_WORKFORCE)

    if not wf.ensure_authenticated(session, URL_WORKFORCE, test_pid):
        raise RuntimeError("No se pudo autenticar la sesión HTTP después del login")

    return session


# ─── Main ─────────────────────────────────────────────────────────────────────

def do_login(headless: bool) -> None:
    """Solo hace login Selenium automático y guarda cookies."""
    log.info("Modo login: iniciando sesión en Workforce...")
    wf.selenium_login_and_save_cookies(
        base_url=URL_WORKFORCE,
        usuario=USUARIO_WF,
        clave=CLAVE_WF,
        headless=headless,
        chrome_path=CHROME_PATH or None,
        chromedriver_path=CHROMEDRIVER_PATH or None,
    )
    log.info("Login exitoso. Archivos generados:")
    log.info("  - %s", wf.COOKIES_FILE)
    log.info("  - %s", wf.USER_AGENT_FILE)
    print("\n✓ Login exitoso. Cookies guardadas.")
    print("\nPara copiar al servidor:")
    print(f"  scp {wf.COOKIES_FILE} {wf.USER_AGENT_FILE} root@SERVIDOR:~/script_wf_om/")


def push_cookies_to_server() -> bool:
    """Envía cookies.json y user_agent.txt al servidor vía SCP."""
    import subprocess
    files = [str(wf.COOKIES_FILE), str(wf.USER_AGENT_FILE)]
    dest = f"{SERVER_USER}@{SERVER_HOST}:{SERVER_PATH}"
    cmd = ["scp", "-P", SERVER_PORT] + files + [dest]
    log.info("Enviando cookies al servidor: %s", " ".join(cmd))
    print(f"\n↑ Enviando cookies a {SERVER_HOST}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            log.info("Cookies enviadas al servidor exitosamente")
            print("✓ Cookies enviadas al servidor exitosamente!")
            if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                wf.send_telegram_alert(
                    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
                    "✅ <b>Workforce O&M - Cookies Renovadas</b>\n\n"
                    "Las cookies se actualizaron correctamente en el servidor.\n"
                    "El cron continuará descargando datos normalmente."
                )
            return True
        else:
            log.error("SCP falló: %s", result.stderr.strip())
            print(f"✗ Error enviando cookies: {result.stderr.strip()}")
            print(f"\nEnvíalas manualmente:")
            print(f"  scp -P {SERVER_PORT} {wf.COOKIES_FILE} {wf.USER_AGENT_FILE} {dest}")
            return False
    except FileNotFoundError:
        log.error("SCP no encontrado. Instala OpenSSH o envía las cookies manualmente.")
        print("\n✗ SCP no encontrado en tu sistema.")
        print(f"  Envíalas manualmente:")
        print(f"  scp -P {SERVER_PORT} {wf.COOKIES_FILE} {wf.USER_AGENT_FILE} {dest}")
        return False
    except subprocess.TimeoutExpired:
        log.error("SCP timeout - verifica conexión al servidor")
        print("\n✗ Timeout conectando al servidor.")
        return False


def do_manual_login(login_timeout: int) -> None:
    """Abre Chrome visible para login manual con MFA. Captura cookies y las envía al servidor."""
    wf.manual_login_and_save_cookies(
        base_url=URL_WORKFORCE,
        timeout=login_timeout,
        chrome_path=CHROME_PATH or None,
        chromedriver_path=CHROMEDRIVER_PATH or None,
    )
    push_cookies_to_server()


def main():
    parser = argparse.ArgumentParser(description="Workforce O&M - Descarga y carga vía HTTP")
    parser.add_argument("--discover", action="store_true", help="Solo descubrir provider_ids y salir")
    parser.add_argument("--login", action="store_true", help="Login automático y guardar cookies")
    parser.add_argument("--login-manual", action="store_true", help="Login manual con MFA de Microsoft")
    parser.add_argument("--login-timeout", type=int, default=300, help="Segundos de espera para login manual (default: 300)")
    parser.add_argument("--days", type=int, default=None, help="Días a descargar (override de .env)")
    parser.add_argument("--headless", type=int, default=None, help="0=Chrome visible, 1=headless")
    args = parser.parse_args()

    headless = HEADLESS if args.headless is None else (args.headless == 1)
    days = args.days if args.days is not None else DAYS

    # Modo login manual: Chrome visible para MFA
    if args.login_manual:
        do_manual_login(args.login_timeout)
        return

    # Modo login automático: solo hacer login y guardar cookies
    if args.login:
        if not USUARIO_WF or not CLAVE_WF:
            log.error("Credenciales de Workforce no configuradas. Revisa el .env")
            sys.exit(1)
        do_login(headless)
        return

    log.info("=" * 60)
    log.info("Workforce O&M v2 - Inicio")
    log.info("URL: %s | Días: %d | Headless: %s", URL_WORKFORCE, days, headless)
    log.info("=" * 60)

    # Obtener providers
    providers = obtener_providers(headless)
    log.info("Providers a descargar: %s", providers)

    if args.discover:
        log.info("Modo discovery: providers encontrados. Saliendo.")
        print("\nProviders encontrados:")
        for pid, name in providers.items():
            print(f"  {pid}: {name}")
        print(f"\nGuardados en: {wf.PROVIDERS_FILE}")
        return

    # Validar credenciales
    if not USUARIO_WF or not CLAVE_WF:
        log.error("Credenciales de Workforce no configuradas. Revisa el .env")
        sys.exit(1)

    # Limpiar descargas
    limpiar_descargas(OUT_DIR)

    # Autenticar
    session = ensure_session(providers, headless)

    # Descargar
    start_day = date.today()
    end_day = start_day + timedelta(days=days - 1)
    log.info("Descargando del %s al %s", start_day, end_day)

    downloaded, errors = wf.download_all(
        session=session,
        base_url=URL_WORKFORCE,
        provider_map=providers,
        start_day=start_day,
        end_day=end_day,
        out_dir=OUT_DIR,
    )

    if downloaded == 0:
        log.error("No se descargó ningún archivo. Abortando carga a MySQL.")
        sys.exit(1)

    log.info("Descarga completada: %d archivos, %d errores", downloaded, errors)

    # Combinar y cargar
    datos = combinar_csvs(OUT_DIR)
    cargar_a_mysql(datos)

    log.info("=" * 60)
    log.info("Proceso completado exitosamente")
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Proceso interrumpido por el usuario")
    except Exception as e:
        log.critical("Error crítico: %s", e, exc_info=True)
        sys.exit(1)
