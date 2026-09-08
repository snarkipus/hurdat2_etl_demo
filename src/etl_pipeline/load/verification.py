"""Small acceptance checks against committed loader output."""

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_pipeline.exceptions import ETLError


def verify_persisted_dataset(
    engine: Engine, expected_storms: int, expected_observations: int
) -> None:
    """Check via a fresh connection after commit; the caller still owns the engine."""
    check = "connection"
    try:
        with engine.connect() as connection:
            check = "persisted counts query"
            storms, observations = connection.execute(
                text("""
                    SELECT (SELECT count(*) FROM storms),
                           (SELECT count(*) FROM observations)
                """)
            ).one()
            if (storms, observations) != (expected_storms, expected_observations):
                raise ETLError(
                    "Persisted count mismatch: "
                    f"storms expected={expected_storms}, actual={storms}; "
                    f"observations expected={expected_observations}, "
                    f"actual={observations}"
                )

            # Installation belongs to the existing loader setup, not this check.
            check = "Spatial extension load"
            connection.execute(text("LOAD spatial"))
            check = "point geometry/coordinate query"
            invalid = connection.execute(
                text("""
                    WITH geometries AS (
                        SELECT ST_GeomFromText(geom) AS g FROM observations
                        UNION ALL
                        SELECT ST_GeomFromText('POINT(0 0)')
                        WHERE NOT EXISTS (SELECT 1 FROM observations)
                    )
                    SELECT count(*) FILTER (WHERE NOT coalesce(
                        CASE WHEN ST_GeometryType(g) = 'POINT' AND NOT ST_IsEmpty(g)
                        THEN isfinite(ST_X(g)) AND isfinite(ST_Y(g))
                             AND ST_X(g) BETWEEN -180 AND 180
                             AND ST_Y(g) BETWEEN -90 AND 90
                        ELSE false END, false))
                    FROM geometries
                """)
            ).scalar_one()
            if invalid:
                raise ETLError(
                    f"Persisted point geometry/coordinate check failed: {invalid} "
                    f"of {observations} observations invalid; require nonempty points "
                    "with finite longitude [-180, 180] and latitude [-90, 90]"
                )
    except ETLError:
        raise
    except Exception as error:
        raise ETLError(
            f"Persisted verification {check} failed ({type(error).__name__}); "
            f"expected storms={expected_storms}, observations={expected_observations}"
        ) from error
