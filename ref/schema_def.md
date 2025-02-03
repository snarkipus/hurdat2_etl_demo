## The revised Atlantic hurricane database (HURDAT2) - Chris Landsea – April 2022

The National Hurricane Center (NHC) conducts a post-storm analysis of each tropical cyclone in its area of responsibility to determine the official assessment of the cyclone's history. This analysis makes use of all available observations, including those that may not have been available in real time. In addition, NHC conducts ongoing reviews of any retrospective tropical cyclone analyses brought to its attention, and on a regular basis updates the historical record to reflect changes introduced via the Best Track Change Committee (Landsea et al. 2004a, 2004b, 2008, 2012, Hagen et al. 2012, Kieper et al. 2016, and Delgado et al. 2018). NHC has traditionally disseminated the tropical cyclone historical database in a format known as HURDAT (short for HURricane DATabase – Jarvinen et al. 1984). This report updates the original HURDAT documentation to reflect significant changes since 2012 to both the format and content for the tropical cyclones and subtropical cyclones of the Atlantic basin (i.e., North Atlantic Ocean, Gulf of Mexico, and Caribbean Sea). (Note for April 2022: Radius of Maximum Wind added into HURDAT2 for the first time beginning with the 2021 hurricane season.)

The original HURDAT format substantially limited the type of best track information that could be conveyed. The format of this new version - HURDAT2 (HURricane DATa 2nd generation) - is based upon the “best tracks” available from the b-decks in the Automated Tropical Cyclone Forecast (ATCF – Sampson and Schrader 2000) system database and is described below. Reasons for the revised version include:
    1) inclusion of non-synoptic (other than 00, 06, 12, and 18Z) best track times (mainly to indicate landfalls and intensity maxima);
    2) inclusion of non-developing tropical depressions; and
    3) inclusion of best track wind radii.

An example of the new HURDAT2 format for Hurricane Ida from 2021 follows:

```csv
AL092021, IDA, 40,
20210826, 1200, , TD, 16.5N, 78.9W, 30, 1006, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 60
20210826, 1800, , TS, 17.4N, 79.5W, 35, 1006, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50
20210827, 0000, , TS, 18.3N, 80.2W, 40, 1004, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50
20210827, 0600, , TS, 19.4N, 80.9W, 45, 1002, 70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 40
20210827, 1200, , TS, 20.4N, 81.7W, 55, 996, 80, 60, 0, 60, 30, 0, 0, 0, 0, 0, 0, 0, 30
20210827, 1800, L, HU, 21.5N, 82.6W, 70, 987, 80, 60, 40, 60, 40, 30, 0, 20, 20, 0, 0, 0, 20
20210827, 2325, L, HU, 22.4N, 83.2W, 70, 988, 80, 60, 40, 60, 40, 30, 0, 20, 20, 0, 0, 0, 20
20210828, 0000, , HU, 22.6N, 83.5W, 70, 989, 100, 60, 40, 70, 50, 30, 0, 30, 20, 0, 0, 0, 20
20210828, 0600, , HU, 23.5N, 84.7W, 70, 987, 100, 60, 40, 70, 50, 30, 0, 30, 20, 0, 0, 0, 20
20210828, 1200, , HU, 24.4N, 85.7W, 70, 986, 110, 80, 60, 100, 50, 40, 20, 30, 25, 20, 0, 0, 20
20210828, 1800, , HU, 25.6N, 86.6W, 80, 976, 110, 100, 70, 100, 50, 40, 20, 40, 25, 20, 10, 20, 20
20210829, 0000, , HU, 26.7N, 87.6W, 90, 967, 120, 100, 80, 110, 70, 60, 40, 60, 35, 30, 20, 30, 20
20210829, 0600, , HU, 27.6N, 88.7W, 115, 950, 120, 100, 80, 110, 70, 60, 40, 60, 35, 30, 20, 30, 15
20210829, 1200, , HU, 28.5N, 89.6W, 130, 929, 130, 110, 80, 110, 70, 60, 40, 60, 45, 35, 20, 30, 10
20210829, 1655, L, HU, 29.1N, 90.2W, 130, 931, 130, 110, 80, 110, 70, 60, 40, 60, 45, 35, 20, 30, 10
20210829, 1800, , HU, 29.2N, 90.4W, 125, 932, 130, 120, 80, 80, 70, 60, 40, 40, 45, 35, 20, 25, 10
20210830, 0000, , HU, 29.9N, 90.6W, 105, 944, 80, 120, 80, 70, 50, 60, 40, 40, 30, 30, 20, 20, 10
20210830, 0600, , HU, 30.6N, 90.8W, 65, 978, 80, 130, 80, 40, 50, 50, 0, 0, 30, 30, 0, 0, 30
20210830, 1200, , TS, 31.5N, 90.9W, 40, 992, 50, 160, 60, 30, 0, 0, 0, 0, 0, 0, 0, 0, 40
20210830, 1800, , TS, 32.2N, 90.5W, 35, 996, 0, 160, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80
20210831, 0000, , TD, 33.0N, 90.0W, 30, 996, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 250
20210831, 0600, , TD, 33.8N, 89.4W, 25, 996, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 210
20210831, 1200, , TD, 34.4N, 88.4W, 25, 996, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 250
20210831, 1800, , TD, 35.1N, 87.1W, 20, 999, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 250
20210901, 0000, , TD, 35.8N, 85.5W, 20, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 270
20210901, 0600, , TD, 36.7N, 83.6W, 20, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 270
20210901, 1200, , EX, 37.7N, 81.5W, 25, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 300
20210901, 1800, , EX, 39.0N, 78.5W, 30, 999, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 210
20210902, 0000, , EX, 39.8N, 75.6W, 35, 997, 0, 150, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 150
20210902, 0600, , EX, 40.6N, 72.8W, 40, 997, 0, 150, 150, 0, 0, 0, 0, 0, 0, 0, 0, 0, 150
20210902, 1200, , EX, 41.4N, 69.7W, 40, 997, 180, 150, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 150
20210902, 1800, , EX, 43.3N, 67.2W, 40, 996, 0, 150, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 150
20210903, 0000, , EX, 45.4N, 64.7W, 40, 995, 150, 150, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 150
20210903, 0600, , EX, 46.6N, 63.6W, 45, 992, 150, 150, 60, 150, 0, 0, 0, 0, 0, 0, 0, 0, 120
20210903, 1200, , EX, 47.5N, 62.7W, 45, 991, 120, 150, 90, 90, 0, 0, 0, 0, 0, 0, 0, 0, 90
20210903, 1800, , EX, 48.6N, 62.4W, 45, 992, 250, 150, 120, 120, 0, 0, 0, 0, 0, 0, 0, 0, 90
20210904, 0000, , EX, 48.8N, 63.1W, 45, 992, 180, 90, 90, 120, 0, 0, 0, 0, 0, 0, 0, 0, 90
20210904, 0600, , EX, 48.7N, 63.9W, 40, 992, 120, 0, 0, 120, 0, 0, 0, 0, 0, 0, 0, 0, 90
20210904, 1200, , EX, 47.6N, 63.9W, 35, 996, 120, 0, 0, 120, 0, 0, 0, 0, 0, 0, 0, 0, 90
20210904, 1800, , EX, 46.6N, 63.5W, 30, 999, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90
```

There are two types of lines of data in the new format: the header line and the data lines. The format is comma delimited to maximize its ease in use. The header line has the following format:

```csv
AL092021, IDA, 40,
1234567890123456789012345768901234567
```

    AL (Spaces 1 and 2) – Basin – Atlantic
    09 (Spaces 3 and 4) – ATCF cyclone number for that year
    2021 (Spaces 5-8, before first comma) – Year
    IDA (Spaces 19-28, before second comma) – Name, if available, or else “UNNAMED”
    40 (Spaces 34-36) – Number of best track entries – rows – to follow

Notes:
1) Cyclone number: In HURDAT2, the order cyclones appear in the file is determined by the date/time of the first tropical or subtropical cyclone record in the best track. This sequence may or may not correspond to the ATCF cyclone number. For example, the 2011 unnamed tropical storm AL20 which formed on 1 September, is sequenced here between AL12 (Katia – formed on 29 Aug) and AL13 (Lee – formed on 2 September). This mismatch between ATCF cyclone number and the HURDAT2 sequencing can occur if post-storm analysis alters the relative genesis times between two cyclones. In addition, in 2011 it became practice to assign operationally unnamed cyclones ATCF numbers from the end of the list, rather than insert them in sequence and alter the ATCF numbers of cyclones previously assigned.

2) Name: Tropical cyclones were not formally named before 1950 and are thus referred to as “UNNAMED” in the database. Systems that were added into the database after the season (such as AL20 in 2011) also are considered “UNNAMED”. Non-developing tropical depressions formally were given names (actually numbers, such as “TEN”) that were included into the ATCF b-decks starting in 2003. Non-developing tropical depressions before this year are also referred to as “UNNAMED”.

The remaining rows of data in the new format are the data lines. These have the following format:
```csv
20210829, 1655, L, HU, 29.1N, 90.2W, 130, 931, 130, 110, 80, 110, 70, 60, 40, 60, 45, 35, 20, 30, 10
12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345
```
    2021 (Spaces 1-4) – Year
    08 (Spaces 5-6) – Month
    29 (Spaces 7-8, before 1st comma) – Day
    16 (Spaces 11-12) – Hours in UTC (Universal Time Coordinate)
    55 (Spaces 13-14, before 2nd comma) – Minutes
    L (Space 17, before 3rd comma) – Record identifier (see notes below)
    C – Closest approach to a coast, not followed by a landfall
    G – Genesis
    I – An intensity peak in terms of both pressure and wind
    L – Landfall (center of system crossing a coastline)
    P – Minimum in central pressure
    R – Provides additional detail on the intensity of the cyclone when rapid changes are underway
    S – Change of status of the system
    T – Provides additional detail on the track (position) of the cyclone
    W – Maximum sustained wind speed
    HU (Spaces 20-21, before 4th comma) – Status of system. Options are:
    TD – Tropical cyclone of tropical depression intensity (< 34 knots)
    TS – Tropical cyclone of tropical storm intensity (34-63 knots)
    HU – Tropical cyclone of hurricane intensity (> 64 knots)
    EX – Extratropical cyclone (of any intensity)
    SD – Subtropical cyclone of subtropical depression intensity (< 34 knots)
    SS – Subtropical cyclone of subtropical storm intensity (> 34 knots)
    LO – A low that is neither a tropical cyclone, a subtropical cyclone, nor an extratropical cyclone (of any intensity)
    WV – Tropical Wave (of any intensity)
    DB – Disturbance (of any intensity)
    29.1 (Spaces 24-27) – Latitude
    N (Space 28, before 5th comma) – Hemisphere – North or South
    90.2 (Spaces 31-35) – Longitude
    W (Space 36, before 6th comma) – Hemisphere – West or East
    130 (Spaces 39-41, before 7th comma) – Maximum sustained wind (in knots)
    931 (Spaces 44-47, before 8th comma) – Minimum Pressure (in millibars)
    130 (Spaces 50-53, before 9th comma) – 34 kt wind radii maximum extent in northeastern quadrant (in nautical miles)
    110 (Spaces 56-59, before 10th comma) – 34 kt wind radii maximum extent in southeastern quadrant (in nautical miles)
    70 (Spaces 62-65, before 11th comma) – 34 kt wind radii maximum extent in southwestern quadrant (in nautical miles)
    60 (Spaces 68-71, before 12th comma) – 34 kt wind radii maximum extent in northwestern quadrant (in nautical miles)
    40 (Spaces 74-77, before 13th comma) – 50 kt wind radii maximum extent in northeastern quadrant (in nautical miles)
    60 (Spaces 80-83, before 14th comma) – 50 kt wind radii maximum extent in southeastern quadrant (in nautical miles)
    80 (Spaces 86-89, before 15th comma) – 50 kt wind radii maximum extent in southwestern quadrant (in nautical miles)
    30 (Spaces 92-95, before 16th comma) – 50 kt wind radii maximum extent in northwestern quadrant (in nautical miles)
    45 (Spaces 98-101, before 17th comma) – 64 kt wind radii maximum extent in northeastern quadrant (in nautical miles)
    25 (Spaces 104-107, before 18th comma) – 64 kt wind radii maximum extent in southeastern quadrant (in nautical miles)
    35 (Spaces 110-113, before 19th comma) – 64 kt wind radii maximum extent in southwestern quadrant (in nautical miles)
    20 (Spaces 116-119, before 20th comma) – 64 kt wind radii maximum extent in northwestern quadrant (in nautical miles)
    15 (Spaces 122-125) – Radius of Maximum Wind (in nautical miles)

Notes:

1) Record identifier: This code is used to identify records that correspond to landfalls or to indicate the reason for inclusion of a record not at the standard synoptic times (0000, 0600, 1200, and 1800 UTC). For the years 1851-1970 and 1991 onward, all continental United States landfalls are marked, while international landfalls are only marked from 1951 to 1970 and 1991 onward. The landfall identifier (L) is the only identifier that will appear with a standard synoptic time record. The remaining identifiers (see table above) are only used with asynoptic records to indicate the reason for their inclusion. Inclusion of asynoptic data is at the discretion of the Hurricane Specialist who performed the post-storm analysis; standards for inclusion or non-inclusion have varied over time. Identification of asynoptic peaks in intensity (either wind or pressure) may represent either system’s lifetime peak or a secondary peak.

2) Time: Nearly all HURDAT2 records correspond to the synoptic times of 0000, 0600, 1200, and 1800. Recording best track data to the nearest minute became available within the b-decks beginning in 1991 and some tropical cyclones since that year have the landfall best track to the nearest minute.

3) Status: Tropical cyclones with an ending tropical depression status (the dissipating stage) were first used in the best track beginning in 1871, primarily for systems weakening over land. Tropical cyclones with beginning tropical depression (the formation stage) were first included in the best track beginning in 1882. Subtropical depression and subtropical storm status were first used beginning in 1968 at the advent of routine satellite imagery for the Atlantic basin. The low status – first used in 1987 - is for cyclones that are not tropical cyclone or subtropical cyclones, nor extratropical cyclones. These typically are assigned at the beginning of a system’s lifecycle and/or at the end of a system’s lifecycle. The tropical wave status – first used in 1981 - is almost exclusively for cyclones that degenerate into an open trough for a time, but then redevelop later in time into a tropical cyclone (for example, AL10-DENNIS in 1981 between 13 and 15 August). The disturbance status is similar to tropical wave and was first used in 1980. It should be noted that for tropical wave and disturbance status the location given is the approximate position of the lower tropospheric vorticity center, as the surface center no longer exists for these stages.

4) Maximum sustained surface wind: This is defined as the maximum 1-min average wind associated with the tropical cyclone at an elevation of 10 m with an unobstructed exposure. Values are given to the nearest 10 kt for the years 1851 through 1885 and to the nearest 5 kt from 1886 onward. A value is assigned for every cyclone at every best track time. Note that the non-developing tropical depressions of 1967 did not have intensities assigned to them in the b-decks. These are indicated as “-99” currently, but will be revised and assigned an intensity when the Atlantic hurricane database reanalysis project (Hagen et al. 2012) reaches that hurricane season.

5) Central Pressure: These values are given to the nearest millibar. Originally, central pressure best track values were only included if there was a specific observation that could be used explicitly. Missing central pressure values are noted as “-999”. Beginning in 1979, central pressures have been analyzed and included for every best track entry, even if there was not a specific in-situ measurement available.

6) Wind Radii – These values have been best tracked since 2004 and are thus available here from that year forward with a resolution to the nearest 5 nm. Best tracks of the wind radii have not been done before 2004 and are listed as “-999” to denote missing data. Note that occasionally when there is a non-synoptic time best track entry included for either landfall or peak intensity, that the wind radii best tracks were not provided. These instances are also denoted with a “-999” in the database.

7) Radius of Maximum Wind: These values have been best tracked only starting in 2021. Before 2021, the missing data are denoted as “-999”. Uncertainty in the RMW values – expressed as estimated absolute error in nautical miles – have been provided by a survey of the NHC Hurricane Specialists in 2022:

    | Storm Classification | Uncertainty (knots) |
    |---------------------|-------------------:|
    | Tropical Storm/Subtropical Storm - Satellite/no scatterometer within 6 hr | 27 |
    | Tropical Storm/Subtropical Storm - Satellite/with scatterometer within 6 hr | 17 |
    | Tropical Storm/Subtropical Storm - Aircraft and satellite | 13 |
    | Tropical Storm/Subtropical Storm - U.S. landfall | 13 |
    | Category 1 or 2 Hurricane - Satellite/no scatterometer within 6 hr | 16 |
    | Category 1 or 2 Hurricane - Satellite/with scatterometer within 6 hr | 12 |
    | Category 1 or 2 Hurricane - Aircraft and satellite | 9 |
    | Category 1 or 2 Hurricane - U.S. landfall | 8 |
    | Category 3, 4, or 5 Hurricane - Satellite/no scatterometer within 6 hr | 11 |
    | Category 3, 4, or 5 Hurricane - Satellite/with scatterometer within 6 hr | 9 |
    | Category 3, 4, or 5 Hurricane - Aircraft and satellite | 5 |
    | Category 3, 4, or 5 Hurricane - U.S. landfall | 5 |

General Notes:

The database goes back to 1851, but it is far from being complete and accurate for the entire century and a half. Uncertainty estimates of the best track parameters available for are available for various era in Landsea et al. (2012), Hagen et al. (2012), Torn and Snyder (2012), and Landsea and Franklin (2013). Moreover, as one goes back further in time in addition to larger uncertainties, biases become more pronounced as well with tropical cyclone frequencies being underreported and the tropical cyclone intensities being underanalyzed. That is, some storms were missed and many intensities are too low in the pre-aircraft reconnaissance era (1944 for the western half of the basin) and in the pre-satellite era (late-1960s for the entire basin). Even in the last decade or two, new technologies affect the best tracks in a non-trivial way because of our generally improving ability to observe the frequency, intensity, and size of tropical cyclones. See Vecchi and Knutson (2008), Landsea et al. (2010), Vecchi and Knutson (2012), Uhlhorn and Nolan (2012), Vecchi et al. (2021) on methods that have been determined to address some of the undersampling issues that arise in monitoring these mesoscale, oceanic phenomenon.

The only aspect of the original HURDAT database that is not contained in the new HURDAT2 is the state-by-state categorization of the Saffir Simpson Hurricane Wind Scale for continental U.S. hurricanes. This information is not a best track quantity and thus will not be included here. However, such U.S. Saffir Simpson Hurricane Wind Scale impact records will continue to be maintained, but within a separate database.

## References:

Delgado, S., C. W. Landsea, and H. Willoughby, 2018: Reanalysis of the 1954-63 Atlantic hurricane seasons. J. Climate, 31, 4177-4192. https://www.aoml.noaa.gov/hrd/Landsea/delgado-et-al-jclimate-2018.pdf

Hagen, A. B., D. Strahan-Sakoskie, and C. Luckett, 2012: A reanalysis of the 1944-53 Atlantic hurricane seasons - The first decade of aircraft reconnaissance. J. Climate, 25, 4441-4460. http://www.aoml.noaa.gov/hrd/Landsea/1944-1953_Published_Paper.pdf

Jarvinen, B. R., C. J. Neumann, and M. A. S. Davis, 1984: A tropical cyclone data tape for the North Atlantic Basin, 1886-1983: Contents, limitations, and uses. NOAA Technical Memorandum NWS NHC 22, Coral Gables, Florida, 21 pp. http://www.nhc.noaa.gov/pdf/NWS-NHC-1988-22.pdf

Kieper, M. E., C. W. Landsea, and J. L. Beven, II, 2016: A reanalysis of Hurricane Camille. Bull. Amer. Meteor. Soc., 97, 367-384. https://www.aoml.noaa.gov/hrd/Landsea/kieper-landsea-beven-bams-2016.pdf
