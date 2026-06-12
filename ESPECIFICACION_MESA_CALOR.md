# Especificación Funcional y Técnica
# Pestaña "MESA — Calor de Mercadería"
## Dashboard Line-Up · Mesa de trading SV MATESUR

> **Documento de diseño APROBADO por el líder de la mesa.** Contiene el contexto
> de negocio, las decisiones tomadas, las fórmulas en pseudocódigo y las etapas
> de implementación. Está pensado para que una sesión nueva de Claude (u otro
> modelo) lo implemente sin necesitar la conversación que lo originó.
>
> **Regla de estilo:** seguir las convenciones existentes del repo — módulos
> puros sin red ni DB (como `cobertura.py`, `fas_comprador.py`), docstrings en
> castellano, tests `unittest` sin red, parametría editable en constantes de
> módulo, UI en `dashboard.py` con `@st.fragment` y caches `@st.cache_data`.

---

## ÍNDICE

1. Contexto de negocio y objetivo
2. Decisiones de diseño tomadas (cerradas, no reabrir)
3. Fuentes de datos disponibles y módulos existentes a reutilizar
4. El ÍNDICE DE CALOR por producto
5. Momentum: la matriz Nivel × Dirección
6. Vista por mes de embarque (alineada a posiciones A3)
7. Vista por zona portuaria
8. Sección "Qué cambió desde ayer"
9. Pantalla MESA (orden y componentes UI)
10. Parametría (resumen de valores editables)
11. Plan de validación (sin backtest de memoria)
12. Etapas de implementación
13. Limitaciones conocidas y honestidad metodológica

---

## 1. CONTEXTO DE NEGOCIO Y OBJETIVO

El usuario lidera la mesa de trading de SV MATESUR (corredora + correacopio).
La mesa **difiere compras y ventas para capturar carry**: compra grano a
productores y lo vende a exportadores/fábricas, gestionando el timing de cada
pata. Opera maíz, trigo y soja. Referencia completa del negocio en el archivo
de contexto de la mesa (carry variantes A/B, a fijar vs a precio, paridad
matba, zonas de TC).

**La pregunta que esta pestaña responde cada mañana, pre-apertura:**
*¿Qué producto está CALIENTE (la exportación/industria necesita mercadería y
se puede sobrepagar) y cuál está PESADO (cubiertos, no va a haber interés)?*

**Decisiones de mesa que alimenta:**
1. **Timing de venta**: vender ya vs diferir especulando con mayor premio.
2. **Dirección**: a qué exportador apuntar (el más corto paga mejor).
3. **Prioridad de producto**: dónde concentrar el esfuerzo comercial.
4. **Detección de ventana "a fijar"**: un exportador corto con gap creciendo
   probablemente esté tomando a fijar → habilita la variante A de carry.

**Explícitamente FUERA de alcance (decisión del usuario):** precios. La mesa
ya tiene su Excel real-time (Refinitiv, A3, sintéticos). Este sistema es la
**pata de cantidad** (necesidades físicas de mercadería); el precio lo cruza
la mesa por su lado.

---

## 2. DECISIONES DE DISEÑO TOMADAS (cerradas, no reabrir)

| # | Decisión | Valor elegido |
|---|----------|---------------|
| 1 | Frecuencia de lectura | Diaria, pre-apertura. La logística naval no cambia abruptamente: lo valioso de cada día es el DELTA vs ayer |
| 2 | Precios | NO se integran. Solo necesidades de mercadería |
| 3 | Eje temporal | DOBLE: horizontes en días (7/15/30, ya construido en COMPRADORES FAS) + presión por mes de embarque alineada a posiciones A3 |
| 4 | Soja | Se lee la demanda de CRUSH: line-up SBM+SBO convertido a equivalente poroto. La soja que origina la mesa es calidad FÁBRICA → su comprador natural es la industria |
| 5 | Benchmark de "caliente" | PERCENTIL HISTÓRICO ESTACIONAL: el valor actual vs la misma época de las últimas 3-5 campañas (snapshots en Supabase). NO umbrales absolutos |
| 6 | Universo de exportadores | TODOS los del line-up (un COFCO corto presiona el bid de todos aunque la mesa no opere con él) |
| 7 | Lado oferta | El ritmo de farmer selling (compras MAGyP vs mismas semanas de campañas previas) SE INTEGRA al índice de calor |
| 8 | Momentum | El índice trae NIVEL + DIRECCIÓN (gap abriéndose / estable / cerrándose). Es la pieza que decide vender-ya vs diferir |
| 9 | Geografía | Desagregado por zona portuaria (up-river Rosario / Bahía Blanca / Quequén), con la limitación de que DJVE es nacional (sección 13) |
| 10 | Entregable | Nueva pestaña "MESA" en el dashboard, primera posición, pensada para leerse en 2 minutos antes de la rueda |
| 11 | Materialidad del delta diario | Solo cambios relevantes, umbrales editables (sección 8) |
| 12 | Pesos del índice | DEMANDA DOMINANTE: ~65% demanda (gap cobertura + line-up) / ~35% oferta (farmer selling). Editables |
| 13 | Validación | Sin backtest contra memoria de mesa (el usuario no la va a proveer). Plan alternativo en sección 11 |

---

## 3. FUENTES DE DATOS Y MÓDULOS EXISTENTES

### 3.1 Datos en Supabase / fuentes
| Fuente | Contenido | Historia |
|--------|-----------|----------|
| Line-up ISA (snapshots diarios) | vessel, cargo, quantity, ETB, shipper_canon, puerto/terminal, ops | ~5 años |
| DJVE (tabla `djve`) | nro, razon_social, producto, codigo_interno, toneladas, fecha_registro, ventana de embarque (inicio/fin) | Reconstruible "as-of" filtrando `fecha_registro <= fecha` |
| Compras MAGyP (CKAN) | compras acumuladas por grano, sector (exportación/industria), campaña, semana | Varias campañas |
| Estimaciones MAGyP | producción por producto/campaña | Por campaña |

### 3.2 Módulos existentes a REUTILIZAR (no reimplementar)
| Módulo | Funciones clave |
|--------|-----------------|
| `cobertura.py` | `balance_por_producto()`, `balance_por_shipper()`, `_filtrar_djve_por_ventana()`, `_filtrar_lineup_por_ventana()`, `canonicalizar_djve()` |
| `compras_fas.py` | `descargar_compras()`, `compras_acumuladas_campana()`, `posicion_exportadora()` |
| `fas_comprador.py` | `urgencia_por_shipper()`, `tabla_urgencia()` — la pestaña COMPRADORES FAS existente queda como vista de detalle |
| `campanas.py` | ventanas de campaña por producto |
| `shipper_norm.py` | canonicalización de exportadores |
| `config.py` | `PRODUCTO_DISPLAY`, `CODIGOS_PRIORITARIOS`, paleta |
| `db.py` | `query_exports_prioritarios()`, `query_djve()` |
| `dashboard.py` | `cached_master_exports()`, `cached_djve()`, agrupación de zonas de la pestaña CONGESTION |

---

## 4. EL ÍNDICE DE CALOR POR PRODUCTO

### 4.1 Fórmula
```
CALOR(producto, fecha) =
      w_gap    × Pctl_estacional( gap_cobertura_30d )        # default 0.35
    + w_lineup × Pctl_estacional( tonelaje_lineup_30d )      # default 0.30
    + w_farmer × ( 100 − Pctl_estacional( avance_ventas ) )  # default 0.35

Resultado: 0-100
```
Demanda (w_gap + w_lineup = 65%) domina sobre oferta (w_farmer = 35%).
**Los tres pesos son editables** (constantes de módulo).

### 4.2 Componentes

**C1 — Gap de cobertura (30 días):**
```
gap = declarado_DJVE_ventana_activa − originado_lineup
```
Usa `cobertura.balance_por_producto()` con horizonte 30d. Para la historia,
reconstruir as-of: DJVE con `fecha_registro <= fecha_snapshot` y ventana de
embarque solapando la ventana de 30 días desde la fecha del snapshot; line-up
del snapshot de esa fecha.

**C2 — Densidad del line-up (30 días):**
```
tonelaje = SUMA(quantity) de buques con ETB en [fecha, fecha+30]
```
Directo de los snapshots históricos.

**C3 — Avance de farmer selling:**
```
avance = compras_acumuladas_campaña(producto, semana_actual)
         ÷ producción_estimada(campaña)
```
Percentil vs el avance en la MISMA semana de campaña de las campañas
anteriores. Se invierte: productor retenido (avance bajo) = más calor.
Para soja usar el sector que corresponda a la demanda de crush (industria).

### 4.3 Percentil estacional (motor común)
```
FUNCION pctl_estacional(metrica, producto, fecha, ventana_dias=15, campanas=5):
    historia = []
    PARA cada campaña previa c (hasta 5):
        fecha_equivalente = misma semana de campaña en c
        valores = metrica(producto, d) para d en
                  [fecha_equivalente − 15, fecha_equivalente + 15]
        historia.extend(valores)
    RETURN percentil de metrica(producto, fecha) dentro de historia  # 0-100
```
- Alinear por **semana de campaña** (no semana calendario) usando `campanas.py`.
- Si hay menos de 2 campañas de historia para un producto → devolver None y
  la UI muestra "SIN HISTORIA SUFICIENTE" (no inventar percentil).

### 4.4 Soja: equivalente poroto desde el crush
```
poroto_eq = tonelaje_SBM / 0.745  +  tonelaje_SBO / 0.19
```
Factores de rendimiento industrial editables (default: 1 tn soja → ~0.745 tn
harina, ~0.19 tn aceite). El producto "SOJA (crush)" usa poroto_eq como C2 y
las DJVE de SBM+SBO convertidas igual como C1. SBS (poroto export) se muestra
como línea separada informativa, sin mezclarse.

### 4.5 Bandas y etiquetas
| Calor | Etiqueta |
|-------|----------|
| ≥ 80 | 🔥 CALIENTE |
| 60-80 | FIRME |
| 40-60 | NEUTRO |
| 20-40 | PESADO |
| < 20 | 🧊 MUY PESADO |

---

## 5. MOMENTUM: LA MATRIZ NIVEL × DIRECCIÓN

### 5.1 Dirección del gap
```
delta_gap = gap_cobertura(hoy) − gap_cobertura(hoy − K días)   # K default 10

ABRIÉNDOSE  si delta_gap ≥ +UMBRAL     # default 32.500 tn (media panamax)
CERRÁNDOSE  si delta_gap ≤ −UMBRAL
ESTABLE     en el resto
```
Se calcula por producto y también por (exportador, producto) para el ranking.

### 5.2 La matriz de acción (corazón de la pestaña)
| | Gap ABRIÉNDOSE ↗ | ESTABLE → | CERRÁNDOSE ↘ |
|---|---|---|---|
| 🔥 CALIENTE | **DIFERIR** — el premio va a mejorar | Vender selectivo al más corto | **VENDER YA** — se están cubriendo, el premio se desinfla |
| NEUTRO | Atención: calentándose | Sin señal | Sin apuro |
| 🧊 PESADO | Posible giro — vigilar | No esperar nada del bid | Comprar barato al productor presionado |

La celda de cada producto se muestra con su acción sugerida. **Lectura
adicional**: exportador corto + gap creciendo = candidato a estar tomando
a fijar → ventana para la variante A de carry de la mesa.

---

## 6. VISTA POR MES DE EMBARQUE (posiciones A3)

Para cada producto y cada mes calendario m de los próximos 6:
```
declarado_m = DJVE cuya ventana de embarque solapa el mes m
originado_m = line-up con ETB dentro del mes m
gap_m       = declarado_m − originado_m
pctl_m      = percentil de gap_m vs el MISMO mes calendario de años previos
```
Render: matriz producto × mes con la banda de calor por celda:
```
            JUN     JUL     AGO     SEP     OCT     NOV
MAÍZ        🔥 85   🔥 78   FIRME   NEUTRO  ...
SOJA(crush) FIRME   NEUTRO  ...
TRIGO       PESADO  PESADO  ...
```
Esto habla el idioma de las posiciones A3 de la mesa ("maíz julio caliente")
y se cruza directo contra los spreads que ya miran en su Excel.

Nota: los meses lejanos tienen line-up incompleto por naturaleza (los buques
se nominan con semanas de anticipación, no meses). El percentil estacional lo
corrige en parte (la historia también estaba incompleta a esa distancia);
mostrar igualmente n_buques por celda como indicador de densidad del dato.

---

## 7. VISTA POR ZONA PORTUARIA

Zonas: **Up-river Rosario / Bahía Blanca / Quequén** (reutilizar la
agrupación puerto→zona existente de la pestaña CONGESTION).

Por zona y producto:
- Tonelaje del line-up próximos 30d vs su historia estacional (percentil).
- Buques próximos (ETB ≤ 7d) con shipper y tonelaje.
- Dirección (mismo cálculo de momentum, sobre el tonelaje zonal).

**Restricción estructural (no resolver, documentar en la UI):** la DJVE es
nacional — no declara puerto de embarque. El gap de cobertura por exportador
NO se puede zonificar. La vista zonal se construye solo del lado line-up.
Mostrar nota al pie en la sección.

---

## 8. SECCIÓN "QUÉ CAMBIÓ DESDE AYER"

Comparar snapshot de hoy vs día hábil anterior. Listar SOLO:

| Evento | Umbral default (editable) |
|--------|---------------------------|
| Buque nuevo en line-up | ≥ 30.000 tn |
| DJVE nueva (por exportador-producto, `fecha_registro` = ayer) | ≥ 20.000 tn |
| Movimiento del gap de cobertura de un producto | ≥ 32.500 tn |
| Cambio de banda del índice (ej. NEUTRO → FIRME) | siempre |
| **Cambio de dirección en la matriz** (ej. abriéndose → cerrándose) | siempre — es el evento más importante |

Formato: lista corta de oraciones legibles, estilo nota de mesa:
*"MAÍZ: COFCO nominó 2 buques (66k tn) en Timbúes, ETB 8-11d. Gap del
producto pasó de abriéndose a estable."*

---

## 9. PANTALLA MESA (orden de lectura, 2 minutos)

1. **Qué cambió desde ayer** (sección 8) — arriba de todo.
2. **Semáforo por producto**: card por producto con CALOR (número + banda),
   DIRECCIÓN (flecha), y ACCIÓN sugerida de la matriz. Productos: MAÍZ,
   TRIGO, SOJA (crush), y SBS informativo.
3. **Matriz producto × mes de embarque** (sección 6).
4. **Zonas** (sección 7).
5. **Top exportadores cortos** (reusar `fas_comprador.tabla_urgencia()`),
   con link mental a la pestaña COMPRADORES FAS para el detalle.
6. Nota metodológica colapsada (expander): fórmula, pesos vigentes,
   limitaciones de la sección 13.

Implementación UI: `_render_mesa_tab(fecha_ref)` con `@st.fragment`, caches
`@st.cache_data(ttl=900)` para cálculos del día y `ttl=3600` para percentiles
históricos (costosos). La pestaña MESA va PRIMERA en `st.tabs([...])`.

---

## 10. PARAMETRÍA (todo editable, constantes de módulo)

| Parámetro | Default |
|-----------|---------|
| `W_GAP`, `W_LINEUP`, `W_FARMER` | 0.35 / 0.30 / 0.35 |
| `HORIZONTE_CALOR_DIAS` | 30 |
| `CAMPANAS_HISTORIA` | 5 (mínimo 2 para emitir percentil) |
| `VENTANA_ESTACIONAL_DIAS` | ±15 |
| `K_MOMENTUM_DIAS` | 10 |
| `UMBRAL_DIRECCION_TN` | 32.500 (media panamax) |
| `UMBRAL_BUQUE_NUEVO_TN` | 30.000 |
| `UMBRAL_DJVE_NUEVA_TN` | 20.000 |
| `RINDE_HARINA`, `RINDE_ACEITE` | 0.745 / 0.19 |
| Bandas de calor | 80 / 60 / 40 / 20 |

---

## 11. PLAN DE VALIDACIÓN (sin backtest de memoria)

El usuario NO va a proveer episodios históricos de sobreprecio. Plan
alternativo, en orden:

1. **Cordura estadística**: computar el índice sobre toda la historia
   disponible y verificar que (a) la distribución de bandas sea razonable
   (~20% del tiempo en cada banda, por construcción de percentiles), (b) el
   índice no salte más de una banda día a día sin evento que lo explique,
   (c) no haya NaN/SIN HISTORIA en los productos core.
2. **Cruce contra eventos públicos documentados** en el propio repo:
   `RESEARCH_TRADING.md` y `PRESION_FAS.md` registran episodios verificables
   (paro aceitero con 750k tn/día frenadas, baja de retenciones jun-2026,
   récord de DJVE de maíz, soja retenida 32% comercializada). Verificar que
   el índice reaccione en la dirección correcta en esas fechas.
3. **Calibración en uso**: la mesa tiene reunión operativa los lunes. Cada
   lunes, contrastar las señales de la semana contra lo visto en la rueda y
   ajustar pesos/umbrales desde parametría. Dejar en la UI los pesos vigentes
   visibles (expander de metodología) para que el ajuste sea consciente.

---

## 12. ETAPAS DE IMPLEMENTACIÓN

1. **`estacional.py`** — motor de percentiles estacionales (módulo puro):
   alineación por semana de campaña, ventana ±15d, mínimo de historia.
   Tests con series sintéticas.
2. **`mesa_calor.py`** — índice de calor + momentum + matriz nivel×dirección
   + equivalente poroto (módulo puro, importa `cobertura`, `compras_fas`,
   `estacional`). Tests.
3. **`mesa_embarque.py`** (o dentro de `mesa_calor.py` si queda chico) —
   matriz producto × mes de embarque. Tests.
4. **Diff diario** — "qué cambió desde ayer" comparando snapshots y DJVE por
   `fecha_registro`. Tests.
5. **Pestaña MESA en `dashboard.py`** — caches + `_render_mesa_tab()` +
   reordenar tabs (MESA primera).
6. **Validación** — script de cordura estadística (etapa 11.1) + verificación
   manual contra eventos (11.2).

Cada etapa: tests unitarios sin red, suite completa verde antes de avanzar
(`python -m unittest discover` — hoy 205 tests), commit por etapa.

---

## 13. LIMITACIONES CONOCIDAS (mostrar en la nota metodológica de la UI)

1. **DJVE anticipadas**: se puede declarar mucho antes de comprar (estrategia
   fiscal, ej. soja esperando baja de retenciones). Un gap enorme puede ser
   especulación impositiva, no urgencia física. Mitigación parcial: el
   momentum y la densidad real del line-up filtran; el lector debe saberlo.
2. **DJVE sin puerto**: la vista zonal es solo line-up (sección 7).
3. **Compras MAGyP semanales y con rezago**: el componente farmer selling se
   mueve más lento que el line-up. No es bug, es la frecuencia del dato.
4. **Sin precios por diseño**: el índice dice dónde hay presión física; si esa
   presión ya está pagada en el premio lo dice el Excel de la mesa, no este
   sistema.
5. **Meses lejanos con line-up incompleto** (sección 6): los buques se
   nominan con semanas de anticipación; la celda de un mes a 5 meses vista
   describe DJVE más que line-up.

---

*Documento generado a partir de la sesión de planificación con el líder de la
mesa (jun-2026). Diseño aprobado. Implementación pendiente.*
