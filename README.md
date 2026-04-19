# 🚢 Line-Up Puertos Argentinos — Dashboard para Agro Trading

Monitor diario del movimiento de buques en puertos argentinos (granos, subproductos, aceites y fertilizantes), con histórico desde 2020 y comparativas contra años anteriores. Pensado como herramienta de análisis para corredores de cereales, traders y analistas de agro.

**Fuente**: [ISA Agents](https://www.isa-agents.com.ar/info/line_up_mndrn.php).
**Stack**: Python (scraping) + Supabase (PostgreSQL en la nube) + Streamlit (dashboard) + GitHub Actions (actualización diaria automática).

---

## 📋 Qué hace

1. **Scrapea** el line-up publicado por ISA para cualquier fecha (2013–2026).
2. **Guarda** cada movimiento en una base de datos Supabase con clave única que deduplica automáticamente.
3. **Backfill** histórico 2020 → hoy (resumable: si lo cortás y lo volvés a correr, salta fechas ya cargadas).
4. **Actualización diaria** corriendo en GitHub Actions a las 10:00 ART.
5. **Dashboard** Streamlit con 4 pestañas:
   - **Hoy**: KPIs, tabla filtrable, barras de productos, torta por puerto, heatmap puerto×producto.
   - **Comparativa histórica**: serie temporal, hoy vs promedio mismo mes últimos 5 años.
   - **Exploración**: filtros libres + export CSV.
   - **En puerto ahora**: buques con ETB ≤ hoy ≤ ETS agrupados por puerto.

---

## 🗂 Estructura del proyecto

```
C:\LineUps_Code\
├── README.md                   # Este archivo
├── requirements.txt            # Dependencias pinneadas
├── .env.example                # Plantilla de credenciales (copiar a .env)
├── .gitignore                  # Protege .env, venv, logs, etc.
├── .github/workflows/
│   └── daily_update.yml        # Action que corre update_today.py cada día
├── .streamlit/
│   └── secrets.toml.example    # Plantilla para Streamlit Cloud
├── logs/                       # Logs del scraper (ignorados por git)
│
├── config.py                   # URL base, listas de productos, mapa meses ES
├── utils.py                    # Parseo de fechas "14-abr", quantity, es_agro
├── scraper.py                  # fetch + parse del HTML
├── db.py                       # Cliente Supabase + upsert + queries paginadas
├── backfill.py                 # Loop 2020 → hoy (CLI)
├── update_today.py             # Hoy + últimos 3 días (para el cron diario)
├── test_end_to_end.py          # Smoke test: scrape + upsert + read
└── dashboard.py                # Streamlit (4 pestañas)
```

---

## 🚀 Setup desde cero

### 1. Prerrequisitos locales

- **Python 3.11 o superior** — https://www.python.org/downloads/ (marcar "Add Python to PATH")
- **Git** — https://git-scm.com/download/win
- Verificá en una terminal nueva:
  ```
  python --version
  git --version
  ```

### 2. Proyecto Supabase

Si estás arrancando **desde cero** (sin proyecto todavía):

1. Crear cuenta en https://supabase.com (gratis).
2. **New Project** → nombre `lineup-argentina`, región `sa-east-1` (São Paulo), contraseña fuerte.
3. Esperá ~2 minutos a que el proyecto esté "Active healthy".
4. En **SQL Editor** pegar y correr:

   ```sql
   create table if not exists public.lineup (
     id             bigserial primary key,
     fecha_consulta date not null,
     port           text not null,
     berth          text,
     vessel         text not null,
     ops            text,
     cat            text,
     cargo          text,
     quantity       integer,
     dest_orig      text,
     area           text,
     shipper        text,
     eta            date,
     etb            date,
     ets            date,
     remarks        text,
     es_agro        boolean default false,
     created_at     timestamptz default now(),
     constraint lineup_unique_row
       unique nulls not distinct
       (fecha_consulta, port, berth, vessel, cargo, quantity, eta, dest_orig, shipper, ops)
   );

   create index if not exists idx_lineup_fecha   on public.lineup (fecha_consulta);
   create index if not exists idx_lineup_cargo   on public.lineup (cargo);
   create index if not exists idx_lineup_port    on public.lineup (port);
   create index if not exists idx_lineup_esagro  on public.lineup (es_agro);
   create index if not exists idx_lineup_shipper on public.lineup (shipper);
   ```

5. En **Settings → API**:
   - Copiá **Project URL** → va a ir a `SUPABASE_URL` del `.env`.
   - Buscá **service_role** (🔒 secret), hacé "Reveal" y copialo → va a `SUPABASE_SERVICE_ROLE_KEY`.
   - ⚠️ El service_role da permisos totales. Nunca commitear ni compartir.

6. **RLS (Row Level Security)**: se deja **deshabilitado** en la tabla `lineup` para que `service_role` la escriba/lea sin fricción. La tabla no se expone públicamente: solo tu código la toca. Si más adelante querés exponerla en una app pública, habilitar RLS y crear un rol `readonly`.

### 3. Instalación local

```bash
# 1. Clonar el repo (o descargar los archivos en C:\LineUps_Code)
cd C:\LineUps_Code

# 2. Crear virtual environment
python -m venv .venv

# 3. Activar el venv (Windows)
.venv\Scripts\activate

# 4. Instalar dependencias
python -m pip install --upgrade pip
pip install -r requirements.txt

# 5. Copiar plantilla de credenciales y completar con los valores reales
copy .env.example .env
# Abrir .env en un editor y pegar SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY

# 6. Verificar que todo conecta
python test_end_to_end.py 2026-04-15
# Debería imprimir "TODO OK" al final.
```

> **Nota sobre Python 3.14**: En Python 3.14 algunas dependencias de Supabase (`pyiceberg`, traído por `storage3`) no tienen wheels precompilados y requieren compilar con Microsoft Visual C++ Build Tools. Por eso en `requirements.txt` fijamos `supabase>=2.15,<2.22` (versión anterior a que se introdujera esa dependencia). Funciona igual en Python 3.11, 3.12, 3.13 y 3.14.

### 4. Carga inicial histórica (backfill)

**Smoke test primero** (7 días, ~15 segundos):

```
python backfill.py --from-date 2026-04-12 --to-date 2026-04-18
```

Si anda OK, lanzá el **backfill completo** 2020 → hoy. Tarda **~1h20min** (2300 días × 2 s de delay por request). Si lo cortás con `Ctrl+C`, la próxima vez salta las fechas ya cargadas:

```
python backfill.py
```

Parámetros opcionales:
- `--from-date 2024-01-01` — arrancar desde otra fecha
- `--to-date 2024-12-31` — terminar en otra fecha
- `--delay 3` — segundos entre requests (default 2)
- `--no-skip` — re-scrapear fechas ya cargadas (fuerza refresh)

### 5. Correr el dashboard

```
streamlit run dashboard.py
```

Abre automáticamente http://localhost:8501. Si la DB está vacía, muestra un banner pidiendo correr el backfill primero.

---

## 🤖 Actualización diaria automática (GitHub Actions)

El scraper corre todos los días a las **10:00 ART (13:00 UTC)** en GitHub Actions — no hace falta que tu máquina esté encendida.

### Setup (una sola vez)

1. **Crear repo en GitHub**:
   - Ir a https://github.com/new
   - Name: `lineup-dashboard`
   - Privacy: **Private** (recomendado)
   - Create repository.

2. **Subir el código local**:
   ```bash
   cd C:\LineUps_Code
   git init
   git add .
   git commit -m "Initial commit: scraper + dashboard line-up"
   git branch -M main
   git remote add origin https://github.com/TU_USUARIO/lineup-dashboard.git
   git push -u origin main
   ```

3. **Configurar secrets en GitHub**:
   - En el repo: **Settings → Secrets and variables → Actions → New repository secret**.
   - Crear dos secrets:
     - Name: `SUPABASE_URL` · Value: el mismo que está en `.env`.
     - Name: `SUPABASE_SERVICE_ROLE_KEY` · Value: el mismo que está en `.env`.

4. **Probar el workflow manualmente**:
   - Tab **Actions** → **Daily line-up update** → **Run workflow** → **Run workflow**.
   - Debería tardar ~1 minuto y quedar en verde.
   - Verificá en Supabase (Table Editor → `lineup`) que las filas de hoy están.

Después de eso, el workflow corre automático todos los días. Podés ver el historial en la tab **Actions**.

---

## 🌐 Deploy del dashboard a Streamlit Community Cloud

Para tener el dashboard accesible desde el celular o compartirlo con colegas:

1. **Ya tenés el repo en GitHub** (paso anterior).
2. Ir a https://share.streamlit.io y loguearte con GitHub.
3. **New app**:
   - Repository: `TU_USUARIO/lineup-dashboard`
   - Branch: `main`
   - Main file path: `dashboard.py`
   - App URL: elegí un subdominio (ej: `lineup-argentina`).
4. **Advanced settings → Secrets**: pegar:
   ```toml
   SUPABASE_URL = "https://gbpfgfeksqmzmsxnxiwg.supabase.co"
   SUPABASE_SERVICE_ROLE_KEY = "tu_service_role_aqui"
   ```
5. **Deploy** → en ~2 minutos tenés la app en `https://lineup-argentina.streamlit.app`.

**Consideración de seguridad**: la app usa `service_role` (permisos totales). Como no hay escritura desde el dashboard (solo SELECTs), el peor caso de un leak sería que alguien consulte la data — que ya planeás compartir. Si querés cerrarlo más: crear una policy de solo-lectura en Supabase y usar `anon key`.

---

## 🛠 Troubleshooting

| Síntoma | Causa probable | Solución |
|---|---|---|
| `python: command not found` | Python no está en PATH | Reinstalá python.org marcando "Add to PATH" |
| `ModuleNotFoundError: No module named 'supabase'` | Olvidaste activar el venv | `.venv\Scripts\activate` |
| `RuntimeError: Faltan credenciales de Supabase` | `.env` falta o está mal | Verificá que el archivo se llame exactamente `.env` y tenga las dos variables |
| `error: Microsoft Visual C++ 14.0 or greater is required` durante `pip install` | Python 3.14 + supabase nuevo pide compilar pyiceberg | Ya está fijado en `requirements.txt` con `supabase<2.22`. Actualizá pip y reintentá. |
| Dashboard dice "La tabla `lineup` está vacía" | Todavía no corriste el backfill | `python backfill.py` |
| Scraper devuelve 0 filas | Fin de semana o feriado argentino | Normal — los puertos no operan sábado/domingo |
| `ValueError: Los headers de la tabla cambiaron` | ISA modificó el HTML | Revisar `config.py:EXPECTED_HEADERS` y actualizar el orden |
| GitHub Action falla con "missing SUPABASE_URL" | Secrets no configurados | Repo → Settings → Secrets → Actions → agregar los dos |
| Streamlit Cloud no arranca | `secrets.toml` mal formado | Verificar que sean comillas dobles y sintaxis TOML válida |

### Logs

Todos los runs del scraper escriben a `logs/scraper.log`. Abrilo para ver detalles de cada request, errores por fecha, y decisiones de parsing.

### Reset de la tabla (último recurso)

Si querés empezar de cero:

```sql
-- En SQL Editor de Supabase
truncate table public.lineup;
```

Después corré `python backfill.py` de nuevo.

---

## 🔍 Notas técnicas

### Clave única de deduplicación

Al principio la constraint era `(fecha_consulta, port, berth, vessel, cargo, quantity, eta)`, pero descubrimos que **un mismo buque carga la misma quantity del mismo producto splitea a varios destinos/shippers** (ej: `ANASTASIA K` carga 10000t MAIZE con ACA: 1 fila a Saudi Arabia + 1 fila a UAE). Por eso la constraint final incluye también `dest_orig`, `shipper` y `ops`, con `NULLS NOT DISTINCT` para que dos NULLs se traten como iguales en el upsert.

### Rollover de año en fechas ETA/ETB/ETS

La fuente devuelve fechas como `"14-abr"` sin año. El parser usa el año de `fecha_consulta` por default, pero con heurística: si la distancia entre el mes parseado y el mes de consulta es > 6 meses, asume que cruzó de año (diciembre→enero o viceversa).

### Paginación de Supabase

Por default, `supabase-py` devuelve **máximo 1000 filas por query**. Toda lectura histórica tiene que paginar con `.range(start, end)` loop hasta no recibir más filas. Está encapsulado en `db._fetch_all()` — no hay que reimplementarlo en cada lugar.

### Respeto al servidor de ISA

- Delay default de **2 segundos** entre requests en el backfill.
- **Un único request en vuelo** a la vez (sin paralelización).
- **User-Agent identificable** (`LineUpDashboard/1.0 (agro trading research; personal use)`).
- Si fallan **5 fechas consecutivas**, el backfill aborta automáticamente (para no martillar).

### Costos

- **Supabase free tier**: 500 MB DB, 2 GB bandwidth/mes, 50K auth users. Con 6 años de line-up argentino estamos en <100 MB. Gratis para siempre para este uso.
- **GitHub Actions**: 2000 minutos/mes gratis para repos privados. El workflow diario consume ~1 min → ~30 min/mes. Holgado.
- **Streamlit Community Cloud**: gratis para apps públicas/privadas con repos públicos. Si el repo es privado, pedir acceso beta a Streamlit.

---

## 📅 Próximos pasos (fuera del MVP)

- Alertas por Telegram/email cuando aparece un buque de X toneladas de Y producto.
- Análisis predictivo (forecast de toneladas mensuales por producto).
- Integración con precios de futuros (CME, MATba) para cruzar oferta exportable con precio.
- Scraping de otras fuentes (NABSA, otros agents) para validar cruces.
