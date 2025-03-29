"""SQLAlchemy models for the HURDAT2 database schema."""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for SQLAlchemy declarative models."""

    pass


class Storm(Base):
    """
    SQLAlchemy model representing storm metadata in the 'storms' table.
    """

    __tablename__ = "storms"

    __table_args__ = (
        UniqueConstraint("basin", "cyclone_number", "year", name="uix_storm"),
        CheckConstraint("basin IN ('AL', 'EP', 'CP')", name="check_basin"),
        CheckConstraint("cyclone_number > 0", name="check_cyclone_number"),
        CheckConstraint("year >= 1851", name="check_year"),
    )

    storm_id: Mapped[str] = mapped_column(
        String, primary_key=True, doc="Unique storm identifier (e.g., 'AL012023')"
    )
    basin: Mapped[str] = mapped_column(
        String, nullable=False, doc="Basin code (e.g., 'AL' for Atlantic)"
    )
    cyclone_number: Mapped[int] = mapped_column(
        Integer, nullable=False, doc="ATCF cyclone number"
    )
    year: Mapped[int] = mapped_column(Integer, nullable=False, doc="Year of the storm")
    name: Mapped[str] = mapped_column(
        String, nullable=False, doc="Storm name or 'UNNAMED'"
    )

    # Relationship to Observation table
    observations: Mapped[list["Observation"]] = relationship(
        "Observation", back_populates="storm", doc="List of observations for this storm"
    )


class Observation(Base):
    """
    SQLAlchemy model representing individual storm observations in the 'observations'
    table.
    """

    __tablename__ = "observations"

    __table_args__ = (
        CheckConstraint(
            "status IN ('TD', 'TS', 'HU', 'EX', 'SD', 'SS', 'LO', 'WV', 'DB')",
            name="check_status",
        ),
        CheckConstraint(
            "max_wind IS NULL OR max_wind >= 0 OR max_wind IN (-999, -99)",
            name="check_max_wind",
        ),
        CheckConstraint(
            "min_pressure IS NULL OR min_pressure >= 0 OR min_pressure IN (-999, -99)",
            name="check_min_pressure",
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=False, doc="Unique observation ID"
    )
    storm_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("storms.storm_id"),
        nullable=False,
        doc="Foreign key referencing the associated storm",
    )
    date: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, doc="Timestamp of the observation (UTC)"
    )
    record_identifier: Mapped[str | None] = mapped_column(
        String, doc="Special record identifier (e.g., 'L' for landfall)"
    )
    status: Mapped[str] = mapped_column(
        String, nullable=False, doc="Storm status code (e.g., 'HU' for hurricane)"
    )
    geom: Mapped[str] = mapped_column(
        String,
        nullable=False,
        doc="Location geometry in WKT format (e.g., 'POINT(-80.0 25.0)')",
    )
    max_wind: Mapped[int | None] = mapped_column(
        Integer, doc="Maximum sustained wind speed (knots)"
    )
    max_wind_mph: Mapped[float | None] = mapped_column(
        Float, nullable=True, doc="Maximum sustained wind speed (mph)"
    )
    min_pressure: Mapped[int | None] = mapped_column(
        Integer, doc="Minimum central pressure (millibars)"
    )
    # Wind radii fields (34, 50, 64 knots) in nautical miles
    ne34: Mapped[int | None] = mapped_column(Integer, doc="NE 34kt wind radius (nm)")
    se34: Mapped[int | None] = mapped_column(Integer, doc="SE 34kt wind radius (nm)")
    sw34: Mapped[int | None] = mapped_column(Integer, doc="SW 34kt wind radius (nm)")
    nw34: Mapped[int | None] = mapped_column(Integer, doc="NW 34kt wind radius (nm)")
    ne50: Mapped[int | None] = mapped_column(Integer, doc="NE 50kt wind radius (nm)")
    se50: Mapped[int | None] = mapped_column(Integer, doc="SE 50kt wind radius (nm)")
    sw50: Mapped[int | None] = mapped_column(Integer, doc="SW 50kt wind radius (nm)")
    nw50: Mapped[int | None] = mapped_column(Integer, doc="NW 50kt wind radius (nm)")
    ne64: Mapped[int | None] = mapped_column(Integer, doc="NE 64kt wind radius (nm)")
    se64: Mapped[int | None] = mapped_column(Integer, doc="SE 64kt wind radius (nm)")
    sw64: Mapped[int | None] = mapped_column(Integer, doc="SW 64kt wind radius (nm)")
    nw64: Mapped[int | None] = mapped_column(Integer, doc="NW 64kt wind radius (nm)")
    max_wind_radius: Mapped[int | None] = mapped_column(
        Integer, doc="Radius of maximum winds (nm)"
    )

    # Relationship to Storm table
    storm: Mapped["Storm"] = relationship(
        "Storm",
        back_populates="observations",
        doc="The storm this observation belongs to",
    )
