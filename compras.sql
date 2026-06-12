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

-- RLS: el dashboard usa la anon key (solo lectura). Habilitar lectura publica
-- y dejar la escritura solo para la service_role (que ignora RLS).
alter table public.compras enable row level security;

drop policy if exists "compras lectura publica" on public.compras;
create policy "compras lectura publica"
    on public.compras for select
    using (true);
