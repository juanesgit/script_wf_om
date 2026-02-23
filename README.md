# Script Workforce O&M v2

Descarga actividades de Oracle Field Service Cloud (OFSC/Workforce) y las carga a MySQL.

## Mejoras vs v1

| Aspecto | v1 (Selenium) | v2 (HTTP requests) |
|---|---|---|
| **Descarga** | Scraping: clic en botones, esperar archivos | HTTP GET directo con cookies |
| **Velocidad** | ~30s por archivo (UI rendering) | ~2s por archivo |
| **Estabilidad** | Frágil ante cambios de UI | Robusto (URL de exportación estable) |
| **Selenium** | Todo el proceso | Solo login (1 vez, cookies se reusan) |
| **Configuración** | Hardcodeada en el script | `.env` externalizado |
| **Provider IDs** | Nombres de botones en UI | IDs numéricos (auto-descubribles) |

## Instalación

```bash
pip install -r requirements.txt
```

## Configuración

1. Copiar `.env.example` a `.env`:
   ```bash
   cp .env.example .env
   ```

2. Editar `.env` con tus credenciales y configuración.

3. **Provider IDs**: Hay 3 formas de configurarlos:
   - **Auto-descubrimiento**: Dejar `PROVIDER_IDS=` vacío. El script intentará descubrirlos.
   - **Manual en .env**: `PROVIDER_IDS=5:REGION OCCIDENTE,4253:PYMES OCCIDENTE,...`
   - **Archivo JSON**: Editar `providers_om.json` directamente.

## Uso

```bash
# Ejecución normal (descarga + carga MySQL)
python script_wf_v2.py

# Solo descubrir provider_ids
python script_wf_v2.py --discover

# Override de días a descargar
python script_wf_v2.py --days 5

# Login con Chrome visible (debug)
python script_wf_v2.py --headless 0
```

## Estructura

```
script_wf_om/
├── .env.example          # Template de configuración
├── .env                  # Configuración real (no versionado)
├── .gitignore
├── requirements.txt
├── README.md
├── workforce_client.py   # Módulo HTTP: login, cookies, descarga CSV
├── script_wf_v2.py       # Orquestador: descarga + combinar + MySQL + SP
├── script_wf_v1.py       # (Legacy) Script original con Selenium completo
├── cookies.json          # (Auto-generado) Cookies de sesión
├── user_agent.txt        # (Auto-generado) User-Agent del navegador
├── providers_om.json     # (Auto-generado) Cache de provider_ids
└── downloads/            # (Auto-generado) CSVs descargados
```

## Flujo

1. **Autenticación**: Verifica cookies existentes → si expiraron, login Selenium → guarda cookies
2. **Descarga**: HTTP GET por cada `(provider_id, día)` → guarda CSV
3. **Combinación**: Une todos los CSVs en un DataFrame con columna `Origen`
4. **Carga MySQL**: `TRUNCATE` tabla → `INSERT` datos → `CALL actualizar_wf_om()`
