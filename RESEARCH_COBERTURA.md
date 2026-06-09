# Research — Cobertura exportadora: DECLARADO vs ORIGINADO → SEÑAL FAS

Trading desk Gran Rosario · módulo `cobertura.py` · fecha de corrida: 2026-06-09

---

## 0. Resumen ejecutivo

Cruzamos tres conceptos del negocio exportador argentino para sacar señales sobre
el precio FAS interno:

1. **DECLARADO** (DJVE / Ley 21.453): ventas FOB al exterior ya comprometidas por
   exportador y producto, con ventana de embarque.
2. **ORIGINADO / EMBARCADO** (line-up ISA): buques que el exportador ya tiene
   programados para cargar (grano físico conseguido o en camino).
3. **FALTA CUBRIR** = `DECLARADO − ORIGINADO` = posición corta del exportador.

> Idea central: si un shipper **declaró mucho pero no tiene buques en line-up**,
> está **corto** → tiene que salir a comprar grano al mercado interno para cumplir
> el embarque → **presión alcista sobre el FAS local**. Al revés (más buques que
> lo declarado) está sobre-originado → ya compró de más → **sesgo bajista**.

### Estado de los datos en este entorno

| Fuente | Estado | Detalle |
|---|---|---|
| DJVE (MAGyP) | ❌ **403 Forbidden** | `descargar_djve_acumuladas(2026)` → `(0, 0)`. El MAGyP bloquea la descarga desde este entorno (sin salida a su CDN). |
| Line-up (Supabase) | ❌ **Sin credenciales** | `db.py` ni siquiera importa (`ModuleNotFoundError: dotenv`). Sin Supabase no hay line-up. |

Como **ambas** fuentes están caídas en este entorno, todo el análisis de abajo
corre sobre un **DATASET SINTÉTICO claramente etiquetado como DEMO** (construido a
mano en la sección 4; el snippet de la sección 5 lo reproduce con datos reales).
Los números son inventados para mostrar cómo se leen las señales; **no son datos de
mercado**. El módulo `cobertura.py`
está listo para correr sobre datos reales en cuanto cualquiera de las dos fuentes
esté disponible (las funciones son puras: reciben DataFrames, no tocan red/DB).

---

## 1. Cómo leer el ratio de cobertura

```
ratio_cobertura = originado_tn / declarado_tn       (line-up / DJVE)
falta_cubrir_tn = declarado_tn − originado_tn        (posición corta)
```

| ratio | lectura | implicancia FAS |
|---|---|---|
| **≈ 1.0** | perfectamente cubierto | neutral — el embarque ya tiene grano |
| **< 0.7** | **corto** (falta originar) | **ALCISTA**: el shipper debe comprar interno |
| **> 1.3** | **sobre-originado** | **BAJISTA**: ya compró de más, demanda agotada |
| **0.0** | declaró y NO tiene un solo buque | corto total — máxima presión de compra |
| **inf** | tiene buques sin DJVE | originado sin declarar (transbordo / declaración pendiente) |
| **NaN** | ni declaró ni originó | sin información |

Se calcula a dos niveles:

- **Por producto** (`balance_por_producto`) → la señal de mercado agregada.
- **Por `(shipper_canon, producto)`** (`balance_por_shipper`) → quién está corto y
  en qué. Útil para anticipar **qué exportador** va a salir a comprar.

El horizonte default es **60 días**: se cuentan las DJVE cuya **ventana de embarque
se solapa** con `[fecha_ref, fecha_ref+60]` y los buques con **ETB** en ese rango.

---

## 2. Qué significa para el FAS que los exportadores estén cortos

El FAS (Free Alongside Ship) es el precio que el exportador paga al productor en
puerto. La mecánica:

1. El exportador **declara** una venta FOB (DJVE) → queda obligado a embarcar en
   una ventana.
2. Para embarcar necesita el grano físico. Si todavía no lo compró (no aparece en
   line-up), tiene una **posición corta** que vencer.
3. A medida que se acerca la ventana de embarque sin buque programado, **sale a
   comprar al productor** → **sube el FAS** (compite por mercadería escasa).

Por eso el `falta_cubrir_tn` agregado por producto es un **indicador adelantado de
demanda interna**: mide cuánto grano TIENEN que comprar los exportadores en las
próximas semanas, sí o sí, para no incumplir DJVE.

- `falta_cubrir` grande y positivo + ventana cercana = **bid agresivo inminente**.
- `falta_cubrir` negativo = ya compraron de más = pueden **bajar el bid**.

La **intensidad (1–5)** escala el faltante en múltiplos de Panamax (~60k tn):
`<60k → 1, <180k → 2, <360k → 3, <720k → 4, ≥720k → 5`.

---

## 3. Carga del line-up como confirmación / contradicción

`carga_lineup` mide la **congestión logística** (buques con ETB futuro):

- **nº de buques** y **toneladas totales** en cola.
- **toneladas por puerto** (dónde se acumula).
- **semana pico**: qué semana concentra más toneladas de embarque.

Cómo se combina con la señal de cobertura:

| Cobertura | Line-up | Lectura combinada |
|---|---|---|
| Corto (ratio bajo) | line-up **vacío/liviano** | **Confirma alcista**: falta originar y no hay buques en camino → compra interna asegurada. |
| Corto (ratio bajo) | line-up **muy cargado** ya | **Matiza**: hay buques en camino, parte del faltante puede cerrarse pronto → alcista más débil. |
| Cubierto / sobre-orig. | line-up **muy cargado** | **CONGESTIÓN**: sobreoferta física en puerto → demoras, sobreestadías, sesgo bajista por exceso logístico. |

El umbral de congestión está calibrado a Gran Rosario: **≥360.000 tn (≈6 Panamax)
cargando la misma semana** dispara la señal `CONGESTION`.

---

## 4. Tabla de señales actuales — **DATOS SINTÉTICOS (DEMO)**

> ⚠️ **Los números de esta sección son SINTÉTICOS** (DJVE 403 + line-up sin DB en
> este entorno). Sirven solo para demostrar la lectura. Fecha de referencia del
> demo: **2026-06-01**, horizonte 60 días.

### 4.1 Balance por producto (DEMO)

| Producto | Declarado (tn) | Originado (tn) | Falta cubrir (tn) | Ratio | Lectura |
|---|---:|---:|---:|---:|---|
| Maíz | 740.000 | 290.000 | **+450.000** | 0.39 | corto fuerte |
| Harina soja (SBM) | 600.000 | 455.000 | +145.000 | 0.76 | casi cubierto |
| Soja (SBS) | 420.000 | 0 | **+420.000** | 0.00 | corto total |
| Trigo | 150.000 | 110.000 | +40.000 | 0.73 | casi cubierto |
| Aceite soja (SBO) | 90.000 | 120.000 | −30.000 | 1.33 | sobre-originado |
| Sorgo | 70.000 | 30.000 | +40.000 | 0.43 | corto |

### 4.2 Carga del line-up (DEMO)

- Buques en cola: **17** · Toneladas esperando: **1.005.000 tn**
- Por puerto: San Lorenzo 455k · Rosario 170k · San Martín 120k · Timbues 120k ·
  Bahía Blanca 110k · Necochea 30k
- **Semana pico: 2026-06-08 con 515.000 tn** → supera umbral → **CONGESTIÓN**

### 4.3 Señales generadas (DEMO)

| Señal | Producto | Intensidad | Racional (resumen) |
|---|---|:---:|---|
| **ALCISTA FAS** | Maíz | 4 | Declaran 740k, originan 290k (cob. 39%). Faltan 450k → compra interna fuerte. |
| **ALCISTA FAS** | Soja | 4 | Declaran 420k, **0 buques** (cob. 0%). Faltan 420k → corto total. |
| **CONGESTIÓN** | Todos | 3 | 515k tn concentradas la semana del 08-jun → demoras/sobreestadías. |
| **BAJISTA** | Aceite soja | 1 | Line-up 120k vs declarado 90k (cob. 133%) → ya compraron de más. |
| **ALCISTA FAS** | Sorgo | 1 | Declaran 70k, originan 30k (cob. 43%) → faltan 40k. |

> Nota: Harina soja (ratio 0.76) y Trigo (0.73) quedan **sin señal** porque están
> por encima del umbral 0.70 — zona neutral, cobertura razonable.

### 4.4 Quién está corto — balance por shipper (DEMO)

Acá se ve el valor del **cruce por shipper canonicalizado**: la DJVE trae nombres
legales ("CARGILL S.A.C.I.", "OLEAGINOSA MORENO HNOS S.A.") y el line-up trae
canónicos ("CARGILL", "VITERRA-BUNGE"). `canonicalizar_djve` los une.

| Shipper | Producto | Declarado | Originado | Falta cubrir | Ratio |
|---|---|---:|---:|---:|---:|
| CARGILL | Maíz | 480.000 | 120.000 | **+360.000** | 0.25 |
| COFCO | Soja | 300.000 | 0 | **+300.000** | 0.00 |
| VITERRA-BUNGE | Harina soja | 600.000 | 455.000 | +145.000 | 0.76 |
| CARGILL | Soja | 120.000 | 0 | +120.000 | 0.00 |
| LDC | Maíz | 260.000 | 170.000 | +90.000 | 0.65 |
| ACA | Sorgo | 70.000 | 0 | +70.000 | 0.00 |
| ADM | Trigo | 150.000 | 110.000 | +40.000 | 0.73 |
| AGD | Aceite soja | 90.000 | 120.000 | −30.000 | 1.33 |

Lectura de trading: **CARGILL** y **COFCO** son los que más van a tener que salir a
comprar (maíz y soja respectivamente). Son los nombres a vigilar en el bid de
puerto. **AGD** está largo de aceite — no necesita comprar.

---

## 5. Qué revelaría el cruce con datos reales

Cuando DJVE y line-up estén disponibles, el flujo es idéntico al del demo:

```python
import fob_djve, db, cobertura
from datetime import date

djve = fob_djve.descargar_djve_acumuladas(2026)            # DECLARADO
lu   = db.query_exports_prioritarios(date(2026,1,1), date(2026,12,31))  # ORIGINADO

hoy = date.today()
bp     = cobertura.balance_por_producto(djve, lu, hoy, horizonte_dias=60)
bs     = cobertura.balance_por_shipper(djve, lu, hoy, horizonte_dias=60)
carga  = cobertura.carga_lineup(lu, hoy)
senales = cobertura.senales_trading(bp, carga)
```

Lo que el cruce real revelaría y que ninguna fuente sola muestra:

- **Demanda interna adelantada por producto**: cuántas toneladas TIENEN que comprar
  los exportadores en los próximos 60 días (suma de `falta_cubrir_tn` positivos).
- **Quién** va a estar en el bid (balance por shipper) y **en qué puerto** se va a
  ver la presión.
- **Timing**: la semana pico del line-up dice cuándo se descomprime / se congestiona.
- **Divergencias**: producto declarado fuerte sin line-up (alcista) vs. producto con
  line-up cargado y poca DJVE (originación especulativa o transbordo).

### Si solo está DJVE (line-up caído)

`balance_por_producto` sigue funcionando: el line-up entra vacío, así que **todo lo
declarado aparece como corto** (ratio 0, falta = declarado). Eso por sí solo es una
cota superior de la compra interna pendiente. Además:

- `concentracion_por_shipper(djve, hoy, 60)` → ranking de exportadores por toneladas
  declaradas y su **share** (quién concentra el riesgo de cobertura).
- Las ventanas de embarque (`fecha_inicio/fin_embarque`) dan el **ritmo temporal** de
  la demanda futura, aunque no se pueda restar el originado todavía.

---

## 6. Módulo y tests

- **`cobertura.py`** — funciones puras (sin red/DB; import sin efectos secundarios):
  `canonicalizar_djve`, `balance_por_producto`, `balance_por_shipper`,
  `carga_lineup`, `senales_trading`, más helpers `concentracion_por_shipper` y
  `contexto_campana`. Reutiliza `shipper_norm`, `config.PRODUCTO_DISPLAY` y
  `campanas` — no duplica regex ni lógica de campaña.
- **`test_cobertura.py`** — 19 tests unitarios, sin red ni DB. Cubren: cruce por
  shipper canonicalizado (CARGILL SACI ↔ CARGILL, OLEAGINOSA MORENO ↔ VITERRA-BUNGE),
  `falta_cubrir_tn` en los tres casos (corto/cubierto/sobre-originado),
  `senales_trading` (ALCISTA/BAJISTA/CONGESTIÓN), y bordes (vacíos, producto sin
  DJVE, producto sin line-up). **Resultado: 19/19 OK.**

```
$ python -m unittest test_cobertura
Ran 19 tests in 0.35s — OK
```
