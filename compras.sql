-- Tabla `compras`: comercializacion de granos del MAGyP.
-- Compras de la exportacion e industria al productor, publicadas semanalmente
-- por campana / grano / sector (SIO-Granos). Alimenta el componente
-- "farmer selling" del indice de calor de la pestana MESA.
--
-- Se carga con update_compras.py (corre en el workflow daily_update.yml).
-- Crear esta tabla UNA VEZ en Supabase: SQL Editor -> pegar -> Run.

create table if not exists public.compras (
    id                  bigserial primary key,
    fecha               date            not null,   -- semana de la observacion
    grano_raw           text,                       -- nombre crudo MAGyP
    codigo_interno      text            not null,   -- ej MAIZE, WHEAT, SBS
    campana             text            not null,   -- ej "2025/26"
    sector              text            not null,   -- EXPORTACION / INDUSTRIA
    toneladas           double precision default 0,
    toneladas_a_fijar   double precision,
    precio_promedio_usd double precision,
    porcentaje_cosecha  double precision,           -- % cosecha comercializado
    actualizado_en      timestamptz     default now(),

    -- Una observacion semanal por (campana, grano, sector, fecha).
    -- Coincide con UPSERT_CONFLICT_COMPRAS en config.py.
    unique (campana, codigo_interno, sector, fecha)
);

-- Indice para las lecturas del dashboard (filtra por codigo + campana).
create index if not exists compras_codigo_campana_idx
    on public.compras (codigo_interno, campana);

-- RLS: lectura publica para el dashboard (anon key).
alter table public.compras enable row level security;

drop policy if exists "compras lectura publica" on public.compras;
create policy "compras lectura publica"
    on public.compras for select
    using (true);

-- ESCRITURA PUBLICA EN compras (panel de carga web sin contrasena).
--
-- ADVERTENCIA DE SEGURIDAD: estas dos politicas permiten que CUALQUIERA con la
-- anon key (o sea, cualquiera que abra la web publica) inserte y actualice filas
-- en `compras`. Es el costo de tener el boton de carga en la web sin login.
-- El dano esta ACOTADO a esta tabla: las politicas NO tocan `lineup` ni `djve`,
-- y la service_role sigue siendo la unica con poder total. Si en el futuro
-- agregas login o volves a carga solo-local, borra estas dos politicas:
--   drop policy "compras escritura publica insert" on public.compras;
--   drop policy "compras escritura publica update" on public.compras;
drop policy if exists "compras escritura publica insert" on public.compras;
create policy "compras escritura publica insert"
    on public.compras for insert
    with check (true);

-- UPDATE necesario para que el UPSERT (insert ... on conflict do update) pueda
-- sobreescribir una observacion semanal ya cargada (idempotencia).
drop policy if exists "compras escritura publica update" on public.compras;
create policy "compras escritura publica update"
    on public.compras for update
    using (true)
    with check (true);
