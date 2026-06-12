# 🚢 Line-Up Puertos Argentinos — Dashboard para Agro Trading

Monitor diario del movimiento de buques en puertos argentinos (granos, subproductos, aceites y fertilizantes), con histórico desde 2020, DJVE del MAGyP, farmer selling y un índice de temperatura de mercadería. Pensado para traders FAS, corredores de cereales y analistas de agro.

**Fuentes**: [ISA Agents](https://www.isa-agents.com.ar/info/line_up_mndrn.php) (line-up) · [MAGyP DJVE](https://datos.magyp.gob.ar) (declaraciones de exportación) · [MAGyP Compras](https://datos.magyp.gob.ar/dataset/compras-de-granos) (farmer selling).  
**Stack**: Python (scraping + análisis) + Supabase (PostgreSQL en la nube) + Streamlit (dashboard) + GitHub Actions (actualización diaria automática).

---

## 📋 Qué hace

1. **Scrapea** el line-up publicado por ISA para cualquier fecha (2013–2026).
2. **Descarga** las DJVE (Declaraciones Juradas de Ventas al Exterior) del MAGyP.
3. **Descarga** el avance de compras de granos del MAGyP (farmer selling).
4. **Guarda** todo en Supabase con claves únicas que deduplicen automáticamente.
5. **Backfill** histórico 2020 → hoy (resumable: si lo cortás y lo volvés a correr, salta fechas ya cargadas).
6. **Actualización diaria** a las 10:00 ART corriendo en GitHub Actions.
7. **Dashboard** Streamlit con 6 pestañas:
   - **🔥 MESA**: índice de temperatura por producto (CALIENTE/FIRME/NEUTRO/PESADO). Pre-apertura: ¿qué diferir y qué vender ya?
   - **📊 PANORAMA**: KPIs, tabla filtrable, barras de productos, torta por puerto, heatmap puerto×producto.
   - **🏢 SHIPPERS**: posición por exportador (declarado DJVE vs. comprometido line-up, gap).
   - **🌾 PRODUCTOS**: comparativa histórica — serie temporal, hoy vs. promedio mismo mes últimos 5 años.
   - **⚓ CONGESTION**: buques en puerto ahora, filtros libres, export CSV.
   - **🎯 COMPRADORES FAS**: ranking de urgencia compradora por exportador — gap + ETB próximo.

---

## 🗂 Estructura del proyecto

```
LineUps_Code/
├── README.md                   # Este archivo
├── requirements.txt            # Dependencias pinneadas
├── .env.example                # Plantilla de credenciales (copiar a .env)
├── .gitignore                  # Protege .env, venv, logs, etc.
│
├── .github/workflows/
│   └── daily_update.yml        # Cron 10:00 ART: ISA + DJVE + compras
│
├── .streamlit/
│   └── secrets.toml.example    # Plantilla para Streamlit Cloud
│
├── logs/                       # Logs del scraper (ignorados por git)
│
│── SQL de tablas (correr en Supabase SQL Editor):
│   └── compras.sql             # Tabla compras (farmer selling MAGyP)
│   (lineup y djve se crean con el DDL de la sección Setup)
│
├── Módulos de análisis:
│   ├── config.py               # Constantes globales, paleta de colores, tablas
│   ├── utils.py                # Parseo de fechas "14-abr", quantity, es_agro
│   ├── campanas.py             # Alineación de campañas agrícolas
│   ├── shipper_norm.py         # Normalización de nombres de shippers
│   ├── estacional.py           # Percentiles estacionales por campaña
│   ├── cobertura.py            # Balance declarado DJVE vs. originado line-up
│   ├── compras_fas.py          # Porcentaje de cosecha comercializado (farmer selling)
│   ├── mesa_calor.py           # Índice de temperatura + acción sugerida
│   ├── mesa_diff.py            # Tape de cambios diarios (dirección, banda, buques nuevos)
│   ├── mesa_embarque.py        # Calendario de embarques por mes
│   ├── fas_comprador.py        # Urgencia compradora por exportador (pestaña FAS)
│   ├── fob_djve.py             # FOB implícito desde DJVE
│   ├── estimaciones.py         # Estimaciones de cosecha por fuente
│   └── clima.py                # Estado climático
│
├── Scripts de carga:
│   ├── scraper.py              # Fetch + parse del HTML de ISA
│   ├── db.py                   # Cliente Supabase + upsert + queries paginadas
│   ├── backfill.py             # Loop 2020 → hoy (CLI)
│   ├── update_today.py         # ISA hoy + últimos 3 días (cron diario)
│   ├── update_djve.py          # DJVE del MAGyP (cron diario)
│   └── update_compras.py       # Compras MAGyP / farmer selling (cron diario)
│
├── Verificación:
│   └── verificar_mesa.py       # Pre-flight check: conectividad + datos suficientes
│
├── Tests (sin red, sin DB):
│   ├── test_cobertura.py
│   ├── test_compras_fas.py
│   ├── test_estacional.py
│   ├── test_fas_comprador.py
│   ├── test_mesa_calor.py
│   ├── test_mesa_diff.py
│   ├── test_mesa_embarque.py
│   ├── test_update_compras.py
│   ├── test_dashboard_logic.py
│   ├── test_units.py
│   └── test_end_to_end.py     # Smoke test con red (requiere credenciales)
│
└── dashboard.py                # Streamlit (6 pestañas)
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
4. En **SQL Editor** pegar y correr el DDL de las tablas (sección siguiente).

#### Tabla `lineup` (line-up ISA)

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

#### Tabla `djve` (Declaraciones Juradas de Ventas al Exterior)

```sql
create table if not exists public.djve (
  id               bigserial primary key,
  fecha_registro   date not null,
  codigo_interno   text not null,
  producto         text,
  empresa          text,
  campana          text,
  toneladas        numeric,
  precio_fob       numeric,
  created_at       timestamptz default now(),
  constraint djve_unique_row unique (fecha_registro, codigo_interno, empresa, campana)
);

create index if not exists idx_djve_fecha   on public.djve (fecha_registro);
create index if not exists idx_djve_codigo  on public.djve (codigo_interno);
create index if not exists idx_djve_empresa on public.djve (empresa);
```

#### Tabla `compras` (farmer selling MAGyP)

Pegar y correr el contenido de **`compras.sql`** (incluido en el repo). Esa tabla habilita el componente "farmer selling" del índice de temperatura MESA.

5. En **Settings → API**:
   - Copiá **Project URL** → va a `SUPABASE_URL` del `.env`.
   - Buscá **service_role** (🔒 secret), hacé "Reveal" y copialo → va a `SUPABASE_SERVICE_ROLE_KEY`.
   - ⚠️ El service_role da permisos totales. Nunca commitear ni compartir.

6. **RLS**: la tabla `lineup` y `djve` se dejan con RLS deshabilitado (escritura solo desde CI con service_role). La tabla `compras` tiene RLS habilitado con SELECT público (ver `compras.sql`).

### 3. Instalación local

```bash
# 1. Clonar el repo
cd LineUps_Code

# 2. Crear virtual environment
python -m venv .venv

# 3. Activar (Windows: .venv\Scripts\activate  /  Linux+Mac: source .venv/bin/activate)
source .venv/bin/activate

# 4. Instalar dependencias
pip install -r requirements.txt

# 5. Copiar plantilla de credenciales y completar con los valores reales
cp .env.example .env
# Abrir .env y pegar SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
```

> **Nota sobre Python 3.14**: algunas dependencias de Supabase requieren compilar con MSVC en Windows. Por eso `requirements.txt` fija `supabase>=2.15,<2.22`. Funciona bien en Python 3.11–3.13.

### 4. Carga inicial histórica

**Smoke test primero** (7 días):

```bash
python backfill.py --from-date 2026-04-12 --to-date 2026-04-18
```

**Backfill completo** 2020 → hoy (~1h20min, resumable):

```bash
python backfill.py
python update_djve.py       # DJVE histórico del MAGyP
python update_compras.py    # farmer selling (puede dar 403 desde cloud)
```

### 5. Verificar que los datos están listos para la pestaña MESA

```bash
python verificar_mesa.py
```

Reporta por producto: cuántas campañas tienen historia estacional y si el índice puede calcular (`✓ ÍNDICE`) o degrada (`⚠ SIN HISTORIA`).

### 6. Correr el dashboard

```bash
streamlit run dashboard.py
```

Abre http://localhost:8501. Si la DB está vacía, muestra un banner pidiendo correr el backfill primero.

---

## 🧪 Tests

```bash
# Suite completa (sin red ni DB — ~2 segundos)
python -m unittest test_cobertura test_compras_fas test_estacional \
  test_fas_comprador test_mesa_calor test_mesa_diff test_mesa_embarque \
  test_update_compras test_dashboard_logic test_units -v

# Smoke test con red (requiere .env con credenciales)
python test_end_to_end.py
```

---

## 🤖 Actualización diaria automática (GitHub Actions)

El cron corre todos los días a las **10:00 ART (13:00 UTC)**:

| Paso | Script | Falla si... |
|------|--------|------------|
| Line-up ISA | `update_today.py --dias 3` | ISA bloquea la IP (continue-on-error) |
| DJVE MAGyP | `update_djve.py` | Red o servidor MAGyP caído (bloquea el job) |
| Compras MAGyP | `update_compras.py` | 403 frecuente en IPs de cloud (continue-on-error) |

Si ISA o compras fallan, el job termina como **warning** (no failure) — DJVE siempre se actualiza.

### Setup (una sola vez)

1. **Repo en GitHub** (privado recomendado).
2. **Settings → Secrets and variables → Actions → New repository secret**:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
3. **Probar el workflow manualmente**: tab **Actions** → **Daily line-up + DJVE update** → **Run workflow**.

---

## 🌐 Deploy del dashboard

### Streamlit Community Cloud (gratuito, hibernación posible)

1. Ir a https://share.streamlit.io y loguearse con GitHub.
2. **New app** → repo, branch `main`, main file `dashboard.py`.
3. **Advanced settings → Secrets**:
   ```toml
   SUPABASE_URL = "https://xxx.supabase.co"
   SUPABASE_SERVICE_ROLE_KEY = "tu_service_role_aqui"
   ```
4. Deploy → en ~2 min tenés la app en `https://tu-app.streamlit.app`.

> **Nota de seguridad**: el dashboard solo hace SELECTs — no escribe a la DB. Si querés reducir el blast radius de un eventual leak del key, creá una policy de solo-lectura en Supabase y usá el `anon key` en el dashboard (que ya tiene SELECT público en `compras`).

### Alternativas always-on (sin hibernación)

Para producción sin hibernación: **Render**, **Railway** o **Fly.io** con Docker/Dev Container. El repo incluye la carpeta `.devcontainer` para facilitar la containerización.

---

## 🛠 Troubleshooting

| Síntoma | Causa probable | Solución |
|---|---|---|
| `python: command not found` | Python no está en PATH | Reinstalar python.org marcando "Add to PATH" |
| `ModuleNotFoundError: No module named 'supabase'` | venv no activado | `source .venv/bin/activate` |
| `RuntimeError: Faltan credenciales de Supabase` | `.env` falta o está mal | Verificar nombre exacto `.env` y las dos variables |
| `error: Microsoft Visual C++ 14.0 or greater is required` | Python 3.14 + supabase nuevo | Ya fijado en `requirements.txt <2.22`. Actualizar pip y reintentar. |
| Dashboard dice "La tabla `lineup` está vacía" | No corriste el backfill | `python backfill.py` |
| Pestaña MESA muestra "SIN HISTORIA" en todos los productos | Poco DJVE histórico cargado | `python update_djve.py` + verificar con `python verificar_mesa.py` |
| Update compras da 403 | MAGyP bloquea IPs de cloud | Normal — el índice de MESA degrada al 65% (gap + line-up). Componente farmer selling sin dato. |
| Scraper devuelve 0 filas | Fin de semana o feriado argentino | Normal — puertos no operan sábado/domingo |
| `ValueError: Los headers de la tabla cambiaron` | ISA modificó el HTML | Revisar `config.py:EXPECTED_HEADERS` |
| GitHub Action falla "missing SUPABASE_URL" | Secrets no configurados | Repo → Settings → Secrets → Actions → agregar los dos |

### Logs

Los runs del scraper escriben a `logs/scraper.log`. Abrilo para ver detalles de cada request y errores de parsing.

### Reset de tablas (último recurso)

```sql
-- En SQL Editor de Supabase
truncate table public.lineup;
truncate table public.djve;
truncate table public.compras;
```

Después correr `python backfill.py`, `python update_djve.py`, `python update_compras.py`.

---

## 🔍 Notas técnicas

### Índice de temperatura MESA

Tres componentes con pesos iguales (re-normalizados si falta alguno):
- **Gap de cobertura** (35%): `declarado DJVE − originado line-up`. Valor alto = exportador sin grano.
- **Densidad de line-up** (30%): toneladas en ventana de 30 días. Alto = demanda concentrada.
- **Farmer selling** (35%): `1 − avance_cosecha_comercializada`. Alto = productor no vendió → mercado pesado.

Cada componente se convierte a percentil estacional (últimas 5 campañas, ±15 días del mismo día de campaña). El índice es el promedio ponderado. Banda: CALIENTE (≥80) / FIRME / NEUTRO / PESADO / MUY PESADO (≤20).

### Urgencia compradora FAS

Para cada exportador, el `urgencia_score = (falta_cubrir_tn / 65_000) × (1 + max(0, 1 − dias_ETB / horizonte))`. Combina magnitud de posición corta + cercanía del próximo barco. Rojo = ETB ≤7d, Ámbar ≤15d, Verde >15d.

### Clave única de deduplicación (lineup)

La constraint incluye `dest_orig`, `shipper` y `ops`, con `NULLS NOT DISTINCT`, para manejar el caso de un buque que carga la misma cantidad del mismo producto con splits a distintos destinos/shippers.

### Rollover de año en fechas ETA/ETB/ETS

La fuente devuelve `"14-abr"` sin año. El parser usa el año de `fecha_consulta`, con heurística de rollover si la distancia supera 6 meses.

### Paginación de Supabase

Supabase devuelve máximo 1000 filas por query. Toda lectura histórica pagina con `.range()` loop en `db._fetch_all()`.

### Costos

- **Supabase free tier**: 500 MB DB, 2 GB bandwidth/mes. 6 años de line-up + DJVE ≈ <200 MB. Gratis.
- **GitHub Actions**: ~30 min/mes (1 min/día × 30 días). Dentro del free tier.
- **Streamlit Community Cloud**: gratis.

---

## 📅 Próximos pasos

- Alertas por Telegram/WhatsApp cuando aparece un buque grande nuevo sin cobertura DJVE.
- Integración con precios FAS reales para cruzar urgencia compradora con premium pagado.
- Scraping de otras fuentes (NABSA) para validación cruzada.
- Pronóstico de toneladas mensuales por producto (regresión estacional).
