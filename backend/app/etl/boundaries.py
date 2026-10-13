"""Load administrative boundaries from the registrar's GeoPackages.

The GeoPackages also carry pre-computed enrollment columns. Those are deliberately NOT
read: the municipal layer's counts were produced by a join on municipality name alone
and are inflated wherever a name repeats across provinces (docs/DATA-AUDIT.md). Geometry
is the only thing taken from these files; every count is recomputed from the CSV.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from psycopg import Connection

from ..gpkg import read_features

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BoundarySource:
    """Where one administrative level's geometry comes from."""

    level: str
    path: Path
    layer: str | None
    name_field: str
    province_field: str | None = None
    region_field: str | None = None


# Filenames as the registrar's Drive folder ships them.
DEFAULT_SOURCES = {
    "country": ("All Enrollees International (Country-level).gpkg", "NAME_LONG", None, None),
    "province": ("All Enrollees Per Program (Provincial-level).gpkg", "adm2_name", None, None),
    "municipality": (
        "All Enrollees Per Program and Total (Municipal-level).gpkg",
        "Muncipality",  # the source file's spelling
        "Province",
        "Region",
    ),
}


def discover_sources(data_dir: Path) -> list[BoundarySource]:
    sources: list[BoundarySource] = []
    for level, (filename, name_f, prov_f, region_f) in DEFAULT_SOURCES.items():
        path = data_dir / filename
        if not path.exists():
            log.warning("skipping %s: %s not found", level, filename)
            continue
        sources.append(BoundarySource(level, path, None, name_f, prov_f, region_f))
    return sources


UPSERT = """
INSERT INTO boundary (level, name, province, region, geom)
VALUES (%(level)s::boundary_level, %(name)s, %(province)s, %(region)s,
        ST_Multi(ST_SimplifyPreserveTopology(
            ST_MakeValid(ST_GeomFromWKB(%(wkb)s, 4326)), %(tolerance)s
        )))
ON CONFLICT (level, name, COALESCE(province, ''))
DO UPDATE SET geom = EXCLUDED.geom,
              region = EXCLUDED.region
"""


def load_boundaries(
    conn: Connection,
    sources: list[BoundarySource],
    tolerance: float = 0.005,
    *,
    replace: bool = False,
) -> dict[str, int]:
    """Insert compact boundary geometry and return feature counts per level.

    GeoPackages contain more detail than this dashboard can display.  Simplifying each
    feature *before* it reaches PostgreSQL avoids storing the 680 MB source layer and
    its projected/simplified copies, which exceeds Neon free-project storage.

    ``replace`` is used by the first-time browser importer after a failed or partial
    map load.  It clears stale geometry, vacuums reusable index pages, then reloads and
    reconnects the already-imported enrollment records below.
    """
    if replace:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM boundary")
            # These indexes belonged to the former three-copy geometry design.  They
            # may contain large, now-unneeded entries from a failed initial import.
            cur.execute("DROP INDEX IF EXISTS boundary_geom_simple_idx")
            cur.execute("DROP INDEX IF EXISTS boundary_geom_3857_idx")
        conn.commit()
        # VACUUM must run outside a transaction.  It makes the pages released by the
        # deletion reusable without requiring the extra disk space of VACUUM FULL.
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("VACUUM (ANALYZE) boundary")
        finally:
            conn.autocommit = False

    counts: dict[str, int] = {}
    for source in sources:
        loaded = skipped = 0
        with conn.cursor() as cur:
            for attrs, wkb in read_features(source.path, source.layer):
                name = attrs.get(source.name_field)
                if not name or wkb is None:
                    skipped += 1
                    continue
                cur.execute(
                    UPSERT,
                    {
                        "level": source.level,
                        "name": str(name).strip(),
                        "province": (
                            str(attrs.get(source.province_field) or "").strip() or None
                            if source.province_field
                            else None
                        ),
                        "region": (
                            " ".join(str(attrs.get(source.region_field) or "").split()) or None
                            if source.region_field
                            else None
                        ),
                        "wkb": wkb,
                        "tolerance": tolerance,
                    },
                )
                loaded += 1
        conn.commit()
        counts[source.level] = loaded
        log.info("%s: %d features loaded, %d skipped", source.level, loaded, skipped)

    with conn.cursor() as cur:
        # Parent links: a municipality belongs to the province of the same name.
        cur.execute(
            "UPDATE boundary m SET parent_id = p.id "
            "FROM boundary p "
            "WHERE m.level = 'municipality' AND p.level = 'province' "
            "  AND p.name = m.province AND m.parent_id IS DISTINCT FROM p.id"
        )
        cur.execute("ANALYZE boundary")
    conn.commit()
    return counts
