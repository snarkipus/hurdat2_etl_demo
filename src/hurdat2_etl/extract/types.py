"""
Type definitions for HURDAT2 data extraction.
Located in: src/hurdat2_etl/extract/types.py

This module contains enums and custom types used in parsing HURDAT2 format data.
The StormStatus enum maps the official HURDAT2 storm status codes to enum values.

Reference: https://www.nhc.noaa.gov/data/hurdat/hurdat2-format-nov2019.pdf
"""

from enum import Enum


class StormStatus(str, Enum):
    """
    Hurricane status indicators from HURDAT2 format specification.
    String enum ensures values match exactly with source data.
    """

    TROPICAL_STORM = "TS"  # Tropical Storm
    HURRICANE = "HU"  # Hurricane
    EXTRATROPICAL = "EX"  # Extratropical cyclone
    LOW = "LO"  # Low pressure system
    WAVE = "WV"  # Tropical Wave
    DISTURBANCE = "DB"  # Disturbance
    SUBTROPICAL_STORM = "SS"  # Subtropical Storm
    SUBTROPICAL_DEPRESSION = "SD"  # Subtropical Depression
    TROPICAL_DEPRESSION = "TD"  # Tropical Depression
    TROPICAL_WAVE = "WV"  # Duplicate of WAVE - per HURDAT2 spec
    UNKNOWN = "XX"  # Unknown/Missing status
