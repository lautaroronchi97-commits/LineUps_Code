# Diseño Visual — Pestaña "MESA · Calor de Mercadería"
## Dashboard Line-Up · SV MATESUR · Especificación de UI

> **Documento complementario de `ESPECIFICACION_MESA_CALOR.md`** (que define el
> QUÉ funcional: fórmulas, datos, módulos). Este documento define el CÓMO
> visual: layout, color, tipografía, componentes y mockups. El implementador
> debe leer ambos. Aquí NO hay código de producción — los snippets son
> referencia ilustrativa.

---

## ÍNDICE

- **Parte 1 — Relevamiento del sistema visual vigente**
  - 1.1 Paleta exacta
  - 1.2 Tipografía
  - 1.3 Patrones de componentes ya usados
  - 1.4 Limitaciones reales de Streamlit y workarounds probados en este repo
- **Parte 2 — Propuesta visual de la pestaña MESA**
  - 2.0 Convención de color de la pestaña (decisión central)
  - 2.1 Encabezado de pestaña
  - 2.2 Sección 1 · Qué cambió desde ayer (el "tape")
  - 2.3 Sección 2 · Semáforo por producto (cards de calor)
  - 2.4 Sección 3 · Matriz producto × mes de embarque
  - 2.5 Sección 4 · Zonas portuarias
  - 2.6 Sección 5 · Top exportadores cortos
  - 2.7 Sección 6 · Nota metodológica
  - 2.8 Layout completo de la pantalla (wireframe integrador)
  - 2.9 Mejoras estéticas rápidas para las pestañas existentes
- **Parte 3 — PROMPT PARA IMPLEMENTACIÓN**

---
---

# PARTE 1 — RELEVAMIENTO DEL SISTEMA VISUAL VIGENTE

## 1.1 Paleta exacta

Fuente de verdad: `BLOOMBERG_PALETTE` en `/home/user/LineUps_Code/config.py`
(líneas ~131-145), replicada en `.streamlit/config.toml`.

| Clave | Hex | Uso actual |
|---|---|---|
| `bg_primary` | `#08080f` | Fondo de la app (casi negro, tinte azul) |
| `bg_card` | `#0e0e1a` | Cards, sidebar, paper de plotly, headers de tabla |
| `bg_hover` | `#16162a` | Hover de tabs |
| `accent` | `#e06010` | Naranja terminal: valores de st.metric, tab activa, botones, barras principales, h2 |
| `accent_blue` | `#6655ee` | Azul eléctrico: series secundarias, MM7d, DJVE, año anterior |
| `positive` | `#00cc66` | Verde (semántica P&L positiva; poco usado) |
| `negative` | `#ff3333` | Rojo: campaña actual en PRODUCTOS, "pendiente/panza", riesgo clima ALTO |
| `warning` | `#e8a800` | Ámbar: línea "Hoy" (vlines), mediana histórica, riesgo MEDIO |
| `text_primary` | `#c8c8d4` | Texto principal |
| `text_muted` | `#50505f` | Labels, captions, líneas históricas grises |
| `grid` | `#14141f` | Grillas plotly, bordes internos de tabla |
| `border` | `#1e1e2e` | Bordes de cards/containers |
| `top_stripe` | `#b83000` | Franja superior fija de 3px (cobre/óxido) y borde inferior de h1 |

Colores fijos por shipper (`SHIPPER_COLORS`, config.py ~115-128): VITERRA-BUNGE
`#FF9900` amber, CARGILL `#00D4FF` cyan, COFCO `#FF3333` rojo, LDC `#CC66FF`
violeta, ADM `#FF66CC` rosa, OLAM `#FFCC33` mostaza, OTROS `#808080`.
**Regla del repo: cada shipper conserva SIEMPRE su color en todos los gráficos.**

Colores fuera de paleta detectados (inconsistencias, ver 2.9): `_SEÑAL_COLOR`
en dashboard.py (~2331) usa `#FF4444 / #FFB300 / #4CAF50` (verde Material, no
es el `positive` de la paleta); filiales PY/UY hardcodean `#FF3333 / #33AAFF`.

## 1.2 Tipografía

- **Familia única**: `JetBrains Mono` importada de Google Fonts en el CSS
  global (dashboard.py ~148), con fallback `Consolas, Menlo, monospace`,
  forzada con `!important` sobre toda la app. Plotly usa
  `Consolas, Menlo, monospace` 12px vía `PLOTLY_TEMPLATE`.
- **Escala vigente** (definida en el CSS global, dashboard.py ~170-340):
  - h1: 15px / 700 / uppercase / letter-spacing 0.15em / borde inferior cobre.
  - h2: 12px / 600 / uppercase / color accent.
  - h3: 11px / 400 / uppercase / color muted.
  - Valor de st.metric: 22px / 700 / color accent. Label: 9px uppercase muted.
  - Tablas: celdas 11px, headers 9px uppercase muted.
  - Captions: 10px uppercase muted.
- Conclusión: **todo es uppercase + monospace + tracking amplio**. El diseño
  de MESA debe respetar esa gramática; lo único que puede romper la escala
  es el número héroe de las cards de calor (ver 2.3).

## 1.3 Patrones de componentes ya usados

| Patrón | Dónde | Detalle |
|---|---|---|
| CSS global inyectado | dashboard.py 144-354 | Un solo `st.markdown(<style>, unsafe_allow_html=True)` al inicio. Estiliza metrics, tabs, dataframes, expanders, botones, alerts, plotly containers. Border-radius 2px en todo. |
| KPI row | Todas las pestañas | `st.columns(5)` + `st.metric` con deltas formateados a mano (`fmt_tons`, `pct_change`). |
| Card HTML custom | CONGESTION (clima, ~2292) | `st.markdown` con div inline-styled: borde 1px coloreado por estado, `min-height` para alinear, centrado, `bg_card`. **Es el precedente directo de las cards de MESA.** |
| Heatmap plotly | PANORAMA (~1243) | `px.imshow` con colorscale de 3 paradas `bg_card → accent_blue → accent`. |
| Banda histórica | PANORAMA y PRODUCTOS | `go.Scatter fill="toself"` con rgba al 10-15%. |
| Línea "Hoy" | Varios charts | `add_vline` dash dot color `warning`. |
| Señales con emoji | SHIPPERS, FAS, clima | Etiquetas tipo `🔥 HOT / 🟢 ALTO / 🔴 MUY BAJO` dentro de dataframes. |
| Panel de alertas | `_render_senales_hoy` (~821) | `st.warning/success/info` apilados — el componente MENOS terminal del dashboard (cajas redondeadas nativas de Streamlit). |
| Tabla terminal | Todas | `st.dataframe(hide_index=True)` + CSS global; botón `⬇ Descargar CSV` debajo. |
| Fragments + caches | Todas | Cada pestaña es `_render_*_tab()` con `@st.fragment`; datos con `@st.cache_data(ttl=900/3600)` derivando del master cache. |
| Formato de números | `fmt_tons` (~796) | `1.23M / 46K / —`. Usarlo SIEMPRE; no inventar otro formato de toneladas. |

## 1.4 Limitaciones reales de Streamlit y workarounds

Lo que este repo ya probó que funciona, y lo que NO se puede hacer:

| Se quiere | ¿Se puede? | Workaround concreto |
|---|---|---|
| Cards con número grande coloreado por estado | `st.metric` NO (el CSS global pinta TODOS los valores en accent; no hay color por instancia) | **Card HTML custom** vía `st.markdown(unsafe_allow_html=True)`, como las cards de clima. Definir clases CSS una sola vez (`.mesa-card`, `.mesa-hero`, etc.) en un bloque `<style>` propio de la pestaña para no repetir estilos inline en cada card. |
| Sparkline DENTRO de una card HTML | Plotly NO se puede embeber en HTML de `st.markdown` | **SVG inline generado en Python** (un `<svg><polyline points="..."/></svg>` de ~140×28px). Es un string, viaja dentro del mismo markdown de la card. Alternativa degradada: bloques unicode `▂▃▅▆█` (8 niveles), cero dependencias. |
| Heatmap con bandas discretas + texto por celda | Sí | `go.Heatmap` con `colorscale` de paradas duplicadas (escala escalonada) o `z` ya discretizado a 0-4, `text`/`texttemplate` para el valor y `customdata` para n_buques en hover. `px.imshow` también sirve pero da menos control del texto. |
| Tooltips ricos en HTML custom | No (solo atributo `title=` nativo del browser) | Poner la info crítica VISIBLE (deltas, percentiles); el hover rico queda para los componentes plotly. |
| Control fino de grilla / alturas iguales | No hay CSS grid entre columns | `st.columns` + `min-height` fija en las cards (patrón clima). Gap de columns no es configurable por columna. |
| Colorear celdas de `st.dataframe` | Parcial | `pandas Styler` (`df.style.map(...)`) con background-color por celda — funciona con `st.dataframe`. NO se puede meter HTML dentro de celdas. `column_config.ProgressColumn` sirve para barras dentro de la tabla. |
| Texto enriquecido en `st.dataframe` | No (sin HTML en celdas) | Emojis y glifos unicode (↗ → ↘, 🔥, 🧊) sí renderizan en celdas; usarlos como encoding. |
| Layout horizontal tipo ticker/tape | No hay componente nativo | Lista HTML custom de líneas monospace (sección 2.2). |

---
---

# PARTE 2 — PROPUESTA VISUAL PESTAÑA MESA

## 2.0 Convención de color de la pestaña (decisión central)

**Problema:** en una mesa, rojo=baja/peligro y verde=sube/ok. Pero acá
"CALIENTE" es *oportunidad para el vendedor* (diferir, exigir premio) y
"PESADO" también es accionable (comprar barato). Mapear calor a verde/rojo
P&L sería ambiguo: ¿rojo es malo para quién?

**Decisión: escala TÉRMICA, no escala P&L.** El índice mide *temperatura de
la demanda física*, y la spec ya habla en ese idioma (🔥/🧊). Rojo = calor,
cian = frío. El verde `positive` **no se usa para estados en toda la pestaña
MESA** (queda reservado a deltas numéricos puntuales si hiciera falta, pero
la recomendación es no usarlo: ninguna banda ni dirección es "buena" o "mala"
en sí misma — son condiciones de mercado).

### Mapeo de las 5 bandas (constante sugerida `MESA_HEAT_COLORS`)

| Banda | Rango | Hex texto/borde | Fondo del chip (12% alpha) | Origen en paleta |
|---|---|---|---|---|
| 🔥 CALIENTE | ≥ 80 | `#ff3333` | `rgba(255,51,51,0.12)` | `negative`, resignificado como *rojo térmico* |
| FIRME | 60-80 | `#e06010` | `rgba(224,96,16,0.12)` | `accent` |
| NEUTRO | 40-60 | `#8a8a99` | `rgba(138,138,153,0.10)` | gris derivado entre `text_muted` y `text_primary` (el `text_muted` puro es ilegible como texto de chip) |
| PESADO | 20-40 | `#6655ee` | `rgba(102,85,238,0.12)` | `accent_blue` |
| 🧊 MUY PESADO | < 20 | `#00d4ff` | `rgba(0,212,255,0.12)` | cian hielo (mismo hex que CARGILL; no hay colisión porque nunca conviven en el mismo componente) |

La rampa rojo→naranja→gris→violeta→cian es perceptualmente ordenada
(caliente→frío), funciona sobre `#08080f`, y reutiliza 3 de los 4 acentos
existentes. Banda `None` ("SIN HISTORIA SUFICIENTE"): texto `text_muted`
sobre fondo `bg_card`, sin color.

### Mapeo de las 3 direcciones

Misma lógica térmica — la flecha dice *hacia dónde va la temperatura*:

| Dirección | Glifo | Color | Lectura |
|---|---|---|---|
| ABRIÉNDOSE | `↗` | `#ff3333` | el gap crece → se calienta |
| ESTABLE | `→` | `#8a8a99` | sin cambio material |
| CERRÁNDOSE | `↘` | `#00d4ff` | se están cubriendo → se enfría |

**Una sola leyenda para toda la pestaña: "rojo = calor / demanda urgente ·
cian = frío / cubiertos". Cero ambigüedad P&L.** La ACCIÓN sugerida (texto
"DIFERIR", "VENDER YA"...) no lleva color semántico propio: va en
`text_primary` 700 con glifo `►` en `accent` — la acción es un llamado de
atención, no una valencia.

## 2.1 Encabezado de pestaña

Una línea de contexto arriba de todo (h2 + caption estándar), que ancla las
comparaciones y declara la convención:

```
MESA · CALOR DE MERCADERÍA                        (h2, color accent)
SNAPSHOT 2026-06-12 · VS HÁBIL ANTERIOR 2026-06-11 · ROJO=CALOR · CIAN=FRÍO   (caption 10px muted)
```

Si el snapshot de ayer hábil no existe (feriado largo, hueco de datos), la
caption lo dice explícitamente: `SIN SNAPSHOT PREVIO — DELTAS NO DISPONIBLES`.

## 2.2 Sección 1 · "Qué cambió desde ayer" — el tape

**Componente elegido: lista HTML custom estilo wire/tape de noticias**, NO
`st.info/warning/success` (las cajas nativas son el elemento menos terminal
del dashboard actual; ver 2.9). Cada evento = una línea monospace con un tag
de tipo a la izquierda y borde izquierdo de 3px coloreado.

**Por qué:** es la sección que el trader lee primero; tiene que parecer un
wire de Bloomberg (densidad máxima, una línea por evento, escaneable), no una
pila de toasts. La spec (sección 8) ya define el formato de oración legible.

**Layout:** ancho completo, un solo contenedor `st.markdown`. Máximo ~8
líneas visibles; si hay más, las menos importantes (buques/DJVE) colapsan en
un `st.expander("VER TODOS LOS CAMBIOS (N)")`. Orden: cambios de DIRECCIÓN
primero (el evento más importante según la spec), luego cambios de BANDA,
luego gaps, luego buques/DJVE.

**Color del borde + tag por tipo de evento:**

| Tag | Evento | Borde |
|---|---|---|
| `[DIR ]` | Cambio de dirección | el color de la dirección NUEVA (↗ rojo / ↘ cian / → gris) |
| `[BANDA]` | Cambio de banda | color de la banda NUEVA |
| `[GAP ]` | Movimiento de gap ≥ umbral | `accent` `#e06010` |
| `[BUQUE]` | Buque nuevo ≥ 30k tn | `accent_blue` `#6655ee` |
| `[DJVE]` | DJVE nueva ≥ 20k tn | `accent_blue` `#6655ee` |

**Tipografía:** tag 9px uppercase 700 en el color del borde; producto en
10px 700 `text_primary`; resto de la oración 11px `text_primary`; cifras con
`fmt_tons`. Sin estado que reportar → una sola línea gris:
`SIN CAMBIOS MATERIALES VS 2026-06-11`.

**Mockup:**

```
QUÉ CAMBIÓ DESDE AYER (HÁBIL 11-JUN)
┌──────────────────────────────────────────────────────────────────────────┐
│▌[DIR ] MAÍZ      pasó de ABRIÉNDOSE ↗ a ESTABLE → (Δgap 10d: +12K tn)    │ ← borde gris
│▌[BANDA] TRIGO    NEUTRO → PESADO · calor 41 → 36                         │ ← borde violeta
│▌[GAP ] SOJA(cr)  gap 30d +54K tn (de 310K a 364K) — abriéndose           │ ← borde naranja
│▌[BUQUE] MAÍZ     COFCO nominó 2 buques (66K tn) en Timbúes, ETB 8-11d    │ ← borde azul
│▌[DJVE] TRIGO     LDC registró 28K tn, embarque jul-ago                   │ ← borde azul
└──────────────────────────────────────────────────────────────────────────┘
  ▸ VER TODOS LOS CAMBIOS (9)                                    (expander)
```

## 2.3 Sección 2 · Semáforo por producto — las cards de calor

**Componente elegido: cards HTML custom** (patrón clima de CONGESTION,
elevado). `st.metric` queda descartado: no permite color por estado, ni
chip de banda, ni sparkline, ni línea de acción.

**Layout:** `st.columns(4)` — MAÍZ · TRIGO · SOJA (CRUSH) · SBS. Las tres
primeras idénticas; la de **SBS es informativa**: misma estructura pero
`opacity:0.65`, sin línea de ACCIÓN, y un sub-label `INFORMATIVO · POROTO
EXPORT` (la spec dice que no se mezcla con el crush). `min-height: 235px`
para alinear las cuatro.

**Anatomía y jerarquía (de héroe a accesorio):**

1. **HÉROE: el número de calor.** 44px / 700 / color de banda. Es el único
   elemento de toda la app que supera los 22px — está bien: es LA cifra de
   la mañana.
2. **Chip de banda** al lado del héroe: `🔥 CALIENTE` 10px uppercase, texto
   en color de banda sobre fondo rgba 12%, borde 1px del mismo color,
   border-radius 2px.
3. **Dirección**: `↗ ABRIÉNDOSE` 12px 700 en color de dirección + en la
   misma línea, 10px muted, el dato que la sostiene: `Δgap 10d +48K tn`.
4. **Delta vs ayer**: `Δ +6 vs ayer` 10px; color = rojo térmico si sube,
   cian si baja, gris si |Δ| < 1. (Térmico, no P&L — consistente con 2.0.)
5. **Sparkline 30d** del índice: SVG inline ~140×28px, trazo 1.5px color
   `accent_blue` (color de "serie secundaria" en todo el dashboard), último
   punto marcado con un círculo de 2px en el color de banda. Arriba a la
   derecha de la card. Fallback honesto si se complica: `▂▂▃▅▅▆█` unicode.
6. **Separador** 1px `border` y la línea de **ACCIÓN**: glifo `►` en
   `accent` + texto 12px 700 `text_primary`, subtítulo 10px muted con la
   explicación de la matriz ("el premio va a mejorar").
7. **Pie de card — los 3 componentes del índice**: una línea 9px muted:
   `GAP p92 · LINEUP p81 · FARMER p33` (percentiles de C1/C2/C3). Da
   trazabilidad sin abrir la metodología. Si el producto está
   `SIN HISTORIA SUFICIENTE`: héroe reemplazado por `—` gris y esa leyenda.

**Mockup (una card; la fila son 4 de estas):**

```
┌───────────────────────────────┐  ┌───────────────────────────┐
│ MAÍZ                ∿∿∿∿∿╱╲╱● │  │ producto 11px upper muted │
│                               │  │ sparkline 30d arriba-der  │
│  ▟▛▙▟   ┌────────────┐        │  │                           │
│  █ █ 84 │🔥 CALIENTE │        │  │ héroe 44px rojo térmico   │
│  ▜▙▟▛   └────────────┘        │  │ chip banda                │
│                               │  │                           │
│  ↗ ABRIÉNDOSE · Δgap +48K/10d │  │ dirección 12px rojo       │
│  Δ +6 vs ayer                 │  │ delta índice 10px         │
│ ───────────────────────────── │  │ separador 1px #1e1e2e     │
│ ► DIFERIR                     │  │ acción 12px 700 blanco,   │
│   el premio va a mejorar      │  │   ► en naranja accent     │
│                               │  │                           │
│ GAP p92 · LINEUP p81 · FARMER │  │ componentes 9px muted     │
│ p33                           │  │                           │
└───────────────────────────────┘  └───────────────────────────┘

Fila completa:
┌─ MAÍZ ──────┐ ┌─ TRIGO ─────┐ ┌─ SOJA (CRUSH) ┐ ┌─ SBS ░░░░░░░─┐
│  84 CALIENTE│ │  36 PESADO  │ │  61 FIRME     │ │  52 NEUTRO   │
│  ↗ DIFERIR  │ │  ↘ COMPRAR  │ │  → VENDER SEL.│ │  INFORMATIVO │
└─────────────┘ └─────────────┘ └───────────────┘ └──(op. 0.65)──┘
```

**Microdetalle opcional que eleva:** borde superior de la card de 2px en el
color de banda (eco de la franja cobre global). Borde general se mantiene
1px `border` neutro para no convertir la fila en un arcoíris.

## 2.4 Sección 3 · Matriz producto × mes de embarque

**Componente elegido: `go.Heatmap` plotly** con escala discreta de 5 bandas.
Descartada la tabla estilizada con Styler: el heatmap ya es el patrón del
repo (PANORAMA), da hover rico (que acá importa: gap_m, declarado, originado,
n_buques) y el bloque de color continuo es más escaneable que celdas de tabla.

**Especificación:**

- Ancho completo. Altura compacta: `~46px por fila + 60` (3-4 filas ⇒
  ~200-240px). Es un vistazo, no un gráfico para estudiar.
- Eje Y: MAÍZ, SOJA (CRUSH), TRIGO (orden fijo, el de las cards). Eje X: los
  próximos 6 meses (`JUN JUL AGO SEP OCT NOV`), año solo si cruza
  (`ENE 27`).
- `z` = percentil 0-100 pero **colorscale escalonada** con las paradas
  duplicadas en 0.2/0.4/0.6/0.8 usando los 5 hex de banda con alpha de fondo
  (los fondos rgba 12-18% de 2.0, no los hex plenos — celda llena de
  `#ff3333` puro gritaría más que el héroe de la card). Celda sin dato
  (`None`): `bg_card` con texto `—`.
- **Texto por celda** (`texttemplate`): percentil grande + n_buques chico:
  `85` y debajo `(12 bq)` — 12px y 9px, color `text_primary`. n_buques es el
  indicador de densidad del dato que pide la spec (sección 6): un mes lejano
  con `(1 bq)` se lee con desconfianza, y eso es deliberado.
- Hover: `MAÍZ · SEP 26 — calor p85 · gap 410K tn (DJVE 612K − lineup 202K) · 12 buques`.
- Sin colorbar continua (mentiría: las bandas son discretas). En su lugar,
  una **leyenda manual de chips** en una caption HTML debajo:
  `■ ≥80 CALIENTE  ■ 60-80 FIRME  ■ 40-60 NEUTRO  ■ 20-40 PESADO  ■ <20 MUY PESADO`.
- Caption al pie 9px muted: `MESES LEJANOS: LINE-UP INCOMPLETO POR NATURALEZA — LEER n_buques`.

**Mockup:**

```
PRESIÓN POR MES DE EMBARQUE · POSICIONES A3
┌──────────────────────────────────────────────────────────────────────┐
│              JUN      JUL      AGO      SEP      OCT      NOV       │
│            ┌────────┬────────┬────────┬────────┬────────┬────────┐  │
│ MAÍZ       │██ 85   │██ 78   │▒▒ 64   │░░ 51   │░░ 44   │·· 38   │  │
│            │ (14 bq)│ (11 bq)│ (6 bq) │ (3 bq) │ (2 bq) │ (1 bq) │  │
│            ├────────┼────────┼────────┼────────┼────────┼────────┤  │
│ SOJA(CRUSH)│▒▒ 66   │░░ 52   │░░ 47   │·· 39   │·· 33   │   —    │  │
│            │ (9 bq) │ (7 bq) │ (4 bq) │ (2 bq) │ (1 bq) │ (0 bq) │  │
│            ├────────┼────────┼────────┼────────┼────────┼────────┤  │
│ TRIGO      │·· 31   │·· 28   │░░ 42   │▒▒ 61   │██ 81   │▒▒ 70   │  │
│            │ (5 bq) │ (4 bq) │ (3 bq) │ (5 bq) │ (6 bq) │ (2 bq) │  │
│            └────────┴────────┴────────┴────────┴────────┴────────┘  │
│  ■≥80 CALIENTE ■60-80 FIRME ■40-60 NEUTRO ■20-40 PESADO ■<20 M.PESADO│
│  MESES LEJANOS: LINE-UP INCOMPLETO — LEER N_BUQUES                   │
└──────────────────────────────────────────────────────────────────────┘
(██=rojo 18% · ▒▒=naranja 15% · ░░=gris 10% · ··=violeta 12%)
```

Lectura A3 instantánea: "maíz julio caliente, trigo se calienta en oct".

## 2.5 Sección 4 · Zonas portuarias

**Componente elegido: 3 cards HTML custom en `st.columns(3)`** (UP-RIVER
ROSARIO · BAHÍA BLANCA · QUEQUÉN), cada una con un **bullet bar de
percentil** + lista de buques próximos. Descartado un heatmap zona×producto:
con 3 zonas × 3 productos sería un postage stamp; las cards permiten meter
los buques ≤7d, que es el dato operativo.

**Anatomía por card:**

1. Header: nombre de zona 11px uppercase 700 `text_primary` + flecha de
   dirección zonal (mismo encoding 2.0) a la derecha.
2. **Por producto (3 micro-filas)**: label del producto 10px (ancho fijo) +
   **bullet bar HTML** — un track de 6px `bg_hover` con fill hasta el
   percentil en el color de banda y el valor `p88` 10px a la derecha.
   Es el "número en contexto histórico" en 12px de alto; un gauge plotly
   gastaría 150px por zona para decir lo mismo.
3. Separador + **PRÓXIMOS ≤7D**: hasta 4 líneas 10px:
   `ETB-2d  COFCO      MAÍZ   66K` — con el nombre del shipper coloreado con
   su `SHIPPER_COLORS` (la única aparición de colores de shipper en MESA;
   refuerza la identidad visual del resto del dashboard). Si hay más:
   `+3 buques más · 94K`.
4. `min-height` común (~220px).

**Nota estructural obligatoria** (spec sección 7): caption única bajo las 3
cards, 9px muted:
`DJVE ES NACIONAL (SIN PUERTO): ESTA VISTA ES SOLO LADO LINE-UP — NO HAY GAP DE COBERTURA ZONAL.`

**Mockup:**

```
ZONAS PORTUARIAS · TONELAJE 30D VS HISTORIA ESTACIONAL
┌─ UP-RIVER ROSARIO ──── ↗ ─┐ ┌─ BAHÍA BLANCA ──────── → ─┐ ┌─ QUEQUÉN ─────────── ↘ ─┐
│                            │ │                            │ │                          │
│ MAÍZ    ████████░░ p88 🔥 │ │ MAÍZ    █████░░░░░ p54    │ │ MAÍZ    ███░░░░░░░ p31  │
│ SOJA(c) ██████░░░░ p64    │ │ TRIGO   ███████░░░ p71    │ │ TRIGO   ██░░░░░░░░ p22  │
│ TRIGO   ███░░░░░░░ p35    │ │ SOJA(c) ██░░░░░░░░ p18 🧊 │ │ SOJA(c)      —  s/hist  │
│ ────────────────────────── │ │ ────────────────────────── │ │ ──────────────────────── │
│ PRÓXIMOS ≤7D               │ │ PRÓXIMOS ≤7D               │ │ PRÓXIMOS ≤7D             │
│ ETB-1d COFCO   MAÍZ   66K  │ │ ETB-3d CARGILL TRIGO  41K  │ │ ETB-6d ACA    TRIGO 28K  │
│ ETB-2d LDC     MAÍZ   33K  │ │ ETB-5d VIT-BGE MAÍZ   35K  │ │                          │
│ ETB-4d VIT-BGE SOJA   45K  │ │                            │ │                          │
│ +3 buques más · 94K        │ │                            │ │                          │
└────────────────────────────┘ └────────────────────────────┘ └──────────────────────────┘
 DJVE ES NACIONAL (SIN PUERTO): VISTA SOLO LADO LINE-UP — NO HAY GAP ZONAL.
```

## 2.6 Sección 5 · Top exportadores cortos

**Componente elegido: `st.dataframe` compacto con `pandas Styler` +
`column_config.ProgressColumn`.** Acá NO inventar cards: es una tabla de
ranking, el formato tabla terminal del repo es exactamente correcto, y la
pestaña COMPRADORES FAS ya tiene el detalle — esto es un teaser de 6-8 filas.

**Especificación:**

- Fuente: `fas_comprador.tabla_urgencia()` (reuso, spec sección 9.5). Top
  6-8 por score 7d, solo filas con `falta_cubrir > 0`.
- Columnas: `EXPORTADOR · PROD · FALTA 7D · FALTA 30D · ETB(D) · DIR · SCORE`.
  - `FALTA 7D`: `column_config.ProgressColumn` (barra horizontal nativa
    dentro de la celda, formateada en K tn) — densidad tipo TradingView
    screener sin HTML.
  - `DIR`: glifo `↗ → ↘` por (exportador, producto) — el cruce que detecta
    la ventana "a fijar": **exportador corto + ↗ = candidato a tomar a
    fijar**. Si `↗`, el Styler pinta esa celda con `rgba(255,51,51,0.12)`.
  - `ETB(D)`: Styler con texto rojo térmico si ≤3d, ámbar `warning` si ≤7d.
- Altura fija ~280px, `hide_index=True`, sin download button (está en FAS).
- Caption 9px: `DETALLE COMPLETO EN PESTAÑA COMPRADORES FAS`. Y la lectura
  clave explícita: `↗ + CORTO = POSIBLE TOMA A FIJAR (VENTANA CARRY A)`.

**Mockup:**

```
TOP EXPORTADORES CORTOS · 7D
┌──────────────────────────────────────────────────────────────────────┐
│ EXPORTADOR    PROD     FALTA 7D        FALTA 30D  ETB(D)  DIR  SCORE │
│ COFCO         MAÍZ     ▓▓▓▓▓▓▓▓ 132K     298K       2     ↗    4.1  │
│ LDC           MAÍZA    ▓▓▓▓▓ 87K         140K       4     →    2.6  │
│ VITERRA-BUNGE SOJA(c)  ▓▓▓▓ 66K          181K       3     ↗    2.4  │
│ ADM           TRIGO    ▓▓ 41K             95K       6     ↘    1.3  │
│ MOLINOS       SOJA(c)  ▓▓ 38K             52K       7     →    1.1  │
│ CARGILL       MAÍZ     ▓ 22K              80K       5     →    0.8  │
└──────────────────────────────────────────────────────────────────────┘
 ↗ + CORTO = POSIBLE TOMA A FIJAR (VENTANA CARRY A) · DETALLE EN COMPRADORES FAS
```

## 2.7 Sección 6 · Nota metodológica

**Componente: `st.expander` colapsado** (patrón ya estilizado por el CSS
global), título `ⓘ METODOLOGÍA · PESOS VIGENTES · LIMITACIONES`. Contenido
en markdown plano, tres bloques:

1. **Fórmula** en code block + tabla de 2 columnas con la parametría VIGENTE
   leída de las constantes del módulo (no hardcodear los defaults en el
   texto: si la mesa recalibra un lunes, la UI lo refleja — requisito de la
   spec sección 11.3).
2. **Bandas y convención de color** (la tabla de 2.0 resumida).
3. **Limitaciones** (las 5 de la spec sección 13, copiadas tal cual, en
   lista corta).

Sin mockup: es texto. Única regla visual: nada de h2/h3 dentro del expander
(el CSS global los pinta naranja y compite); usar `**bold**` y tablas.

## 2.8 Layout completo de la pantalla (wireframe integrador)

Orden F-pattern, lo accionable arriba, 2 minutos de lectura:

```
╔════════════════════════════════════════════════════════════════════════╗
║ MESA · CALOR DE MERCADERÍA                                              ║
║ SNAPSHOT 12-JUN · VS 11-JUN · ROJO=CALOR · CIAN=FRÍO                    ║
╠════════════════════════════════════════════════════════════════════════╣
║ 1│ QUÉ CAMBIÓ DESDE AYER          (tape, ancho completo, ≤8 líneas)     ║  ~15s
╟─────────────────────────────────────────────────────────────────────────╢
║ 2│ ┌MAÍZ────┐ ┌TRIGO───┐ ┌SOJA(CR)┐ ┌SBS─░░░─┐   (4 cards iguales)     ║  ~40s
║  │ │84 🔥 ↗ │ │36 PE ↘ │ │61 FI → │ │52 NE   │                         ║
║  │ │DIFERIR │ │COMPRAR │ │VEND.SEL│ │info    │                         ║
║  │ └────────┘ └────────┘ └────────┘ └────────┘                         ║
╟─────────────────────────────────────────────────────────────────────────╢
║ 3│ MATRIZ PRODUCTO × MES   (heatmap plotly, ancho completo, ~230px)    ║  ~25s
╟─────────────────────────────────────────────────────────────────────────╢
║ 4│ ┌UP-RIVER──┐ ┌B.BLANCA──┐ ┌QUEQUÉN──┐   (3 cards zona)              ║  ~20s
╟─────────────────────────────────────────────────────────────────────────╢
║ 5│ TOP EXPORTADORES CORTOS  (dataframe 6-8 filas, ancho completo)      ║  ~15s
╟─────────────────────────────────────────────────────────────────────────╢
║ 6│ ▸ ⓘ METODOLOGÍA · PESOS VIGENTES · LIMITACIONES   (expander)        ║   0s
╚════════════════════════════════════════════════════════════════════════╝
```

`st.divider()` entre secciones (ya estilizado fino por el CSS global). La
pestaña va PRIMERA en `st.tabs` con label `🔥 MESA` (consistente con los
emojis de las otras tabs).

## 2.9 Mejoras estéticas rápidas para las pestañas existentes

Inconsistencias gruesas detectadas en el relevamiento (todas chicas, alto
retorno):

1. **SEÑALES HOY rompe el look terminal** (`_render_senales_hoy`, ~899-916):
   usa `st.success/warning/info` nativos (cajas verdes/amarillas/azules
   redondeadas de Streamlit). Migrarlo al MISMO componente tape de la
   sección 2.2 — un solo componente de "eventos" para toda la app, y la
   transición visual hacia MESA queda gratis.
2. **Verde Material fuera de paleta** (`_SEÑAL_COLOR`, ~2331): `#4CAF50` no
   es el `positive #00cc66`; `#FF4444/#FFB300` tampoco son `negative/warning`.
   Reemplazar por las claves de `BLOOMBERG_PALETTE`.
3. **Radio y fuente inconsistentes en cards de clima** (~2292-2320):
   `border-radius:4px` vs los 2px de TODO el resto, y
   `font-family:Consolas` inline cuando el global es JetBrains Mono.
   Unificar a 2px y quitar el font-family inline (hereda el global).
4. **Colores PY/UY hardcodeados** (~1381): `PY #FF3333` colisiona con el
   rojo de COFCO en la misma pestaña SHIPPERS. Cambiar PY a `warning
   #e8a800` (o mover el par a `config.py` como constante).

---
---

# PARTE 3 — PROMPT PARA IMPLEMENTACIÓN

> Copiar/pegar desde acá hasta el final en una sesión nueva.

---

Sos un desarrollador senior Python/Streamlit. Vas a implementar la pestaña
**"MESA — Calor de Mercadería"** en el dashboard del repo
`/home/user/LineUps_Code`. Hay DOS documentos fuente de verdad que tenés que
leer COMPLETOS antes de escribir una línea:

1. `/home/user/LineUps_Code/ESPECIFICACION_MESA_CALOR.md` — especificación
   FUNCIONAL: fórmulas del índice de calor, momentum, matriz nivel×dirección,
   vista por mes de embarque, zonas, diff diario, parametría, etapas. Las
   decisiones de la sección 2 están CERRADAS: no las reabras.
2. `/home/user/LineUps_Code/DISENO_VISUAL_MESA.md` (este documento) —
   especificación VISUAL: convención de color térmica (sección 2.0),
   anatomía de cada componente, mockups ASCII (secciones 2.1-2.8). Los
   mockups son normativos: el implementador no decide layout.

## Convenciones del repo (obligatorias)

- **Módulos puros sin red ni DB** para toda la lógica (como `cobertura.py`,
  `fas_comprador.py`): reciben DataFrames, devuelven DataFrames/dicts.
  Parametría en constantes de módulo editables. Docstrings en castellano.
- **Tests `unittest` sin red** por cada módulo nuevo, con datos sintéticos.
  Suite completa verde antes de cada commit: `python -m unittest discover`
  (hoy 205 tests).
- **UI solo en `dashboard.py`**: `_render_mesa_tab(fecha_ref)` decorada con
  `@st.fragment`; datos vía `@st.cache_data(ttl=900)` para cálculos del día
  y `ttl=3600` para percentiles históricos. Reusar `cached_master_exports`,
  `cached_djve`, `fmt_tons`, `aplicar_tema`, `BLOOMBERG_PALETTE` y la
  agrupación zona de CONGESTION. NO reimplementar nada listado en la sección
  3.2 de la especificación funcional.
- Todo el texto de UI y comentarios en castellano. Commit por etapa.

## Orden de implementación

Primero la LÓGICA (etapas 1-4 de la sección 12 de la especificación
funcional: `estacional.py`, `mesa_calor.py`, matriz de embarque, diff
diario — cada una con tests). Después la UI visual, en este orden:

1. **Constantes visuales** en `config.py`: `MESA_HEAT_COLORS` (5 bandas) y
   `MESA_DIR_COLORS` (3 direcciones) con los hex EXACTOS de la sección 2.0
   del documento visual.
2. **CSS de la pestaña**: un único bloque `<style>` con las clases
   `.mesa-card`, `.mesa-hero`, `.mesa-chip`, `.mesa-tape`, `.mesa-bullet`,
   etc., inyectado una sola vez dentro de `_render_mesa_tab` (mismo patrón
   que el CSS global de dashboard.py líneas 144-354). Nada de estilos inline
   repetidos por card salvo los valores dinámicos (colores de banda).
3. **Encabezado + Sección 1 (tape)** — sección 2.2 del doc visual.
4. **Sección 2 (4 cards de calor)** — sección 2.3, incluyendo el sparkline
   SVG inline (función helper pura `sparkline_svg(valores, color_linea,
   color_punto) -> str`, testeable en unittest comprobando que devuelve un
   `<svg>` bien formado).
5. **Sección 3 (heatmap producto×mes)** — sección 2.4: `go.Heatmap`,
   colorscale escalonada con los fondos rgba de banda, texto percentil +
   n_buques, sin colorbar, leyenda manual de chips.
6. **Sección 4 (3 cards de zona con bullet bars)** — sección 2.5 + caption
   de la limitación DJVE nacional.
7. **Sección 5 (tabla top cortos)** — sección 2.6: `st.dataframe` +
   `ProgressColumn` + Styler.
8. **Sección 6 (expander metodología)** — sección 2.7, leyendo la parametría
   vigente de las constantes del módulo (no hardcodear los valores en el
   texto).
9. **Reordenar tabs**: MESA primera, label `🔥 MESA`.
10. (Opcional, commit separado) Mejoras 2.9 a pestañas existentes: migrar
    SEÑALES HOY al componente tape, corregir `_SEÑAL_COLOR`, radio/fuente de
    cards de clima, color PY.

## Criterios de aceptación visuales

Verificación: correr `streamlit run dashboard.py` y comparar contra los
mockups ASCII del documento visual, sección por sección.

- [ ] MESA es la PRIMERA pestaña y se abre por defecto.
- [ ] Convención térmica respetada: en TODA la pestaña, rojo `#ff3333` solo
      significa calor/abriéndose y cian `#00d4ff` solo frío/cerrándose. El
      verde `#00cc66` NO aparece en la pestaña MESA.
- [ ] Las 4 cards de producto tienen la misma altura; el número de calor es
      44px en el color de su banda; el chip de banda tiene fondo rgba 12% +
      borde 1px del mismo color; SBS se ve al 65% de opacidad, sin acción.
- [ ] Cada card muestra: héroe, chip, dirección con flecha y Δgap, delta del
      índice vs ayer, sparkline 30d, línea de acción con `►` naranja, y pie
      con percentiles de C1/C2/C3.
- [ ] El tape de "qué cambió" usa HTML custom (NO `st.info/success/warning`),
      una línea por evento, tag + borde izquierdo 3px coloreado por tipo,
      cambios de dirección primero. Sin cambios → línea única gris.
- [ ] El heatmap producto×mes muestra bandas DISCRETAS (5 colores, sin
      colorbar continua), percentil + `(n bq)` por celda, celdas sin dato en
      `bg_card` con `—`, y la caption de meses lejanos.
- [ ] Las 3 cards de zona muestran bullet bars de percentil por producto,
      flecha de dirección zonal, lista de buques ≤7d con el shipper en su
      color de `SHIPPER_COLORS`, y la caption "DJVE es nacional...".
- [ ] La tabla de cortos: ≤8 filas, ProgressColumn en FALTA 7D, columna DIR
      con `↗ → ↘` (fondo rojo 12% si ↗), ETB(d) rojo si ≤3d, caption de
      ventana a fijar.
- [ ] Expander de metodología colapsado por defecto, con pesos VIGENTES
      leídos de constantes y las 5 limitaciones de la sección 13.
- [ ] Si falta historia/snapshot previo: la UI dice `SIN HISTORIA SUFICIENTE`
      / `DELTAS NO DISPONIBLES` en gris — nunca inventa un número ni rompe.
- [ ] Cero warnings de Streamlit en consola; la pestaña re-renderiza sola
      (fragment) al cambiar `fecha_ref` sin recalcular las otras pestañas.
- [ ] `python -m unittest discover` verde (los 205 existentes + los nuevos).
- [ ] Tipografía: nada fuera de JetBrains Mono heredada; ningún elemento
      nuevo supera los 44px del héroe; labels uppercase con tracking como el
      resto de la app; border-radius 2px en todo lo nuevo.

---

*Documento de diseño visual generado el 12-jun-2026. Relevamiento sobre
`dashboard.py` (2528 líneas), `config.py` y `.streamlit/config.toml`.*
