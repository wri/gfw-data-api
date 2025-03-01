GADM_41_IDS_MISSING_REVISION = (
    "IDN.35.4",
    "IDN.35.8",
    "IDN.35.9",
    "IDN.35.13",
    "IDN.35.14",
)


def extract_level_id(adm_level: int, id_string: str):
    """Given a desired admin level and a string containing at least that level
    of id, return the id of just that level."""

    # Exception because of bad formatting of GHA gids in gadm_administrative_boundaries/v4.1
    if id_string.startswith("GHA") and not id_string.startswith("GHA."):
        id_string = "GHA." + id_string[3:]
    # Exception because bad ids IDN.35.4, IDN.35.8, IDN.35.9, IDN.35.13, IDN.35.14
    # (they are missing final '_1') in gadm_administrative_boundaries/v4.1
    if id_string.startswith("IDN") and "_" not in id_string:
        id_string += "_1"

    return (id_string.rsplit("_")[0]).split(".")[adm_level]


def fix_id_pattern(adm_level: int, id_pattern_string: str, provider: str, version: str):
    """Given an admin level and a GADM id pattern suitable for a SQL LIKE
    clause, return an id pattern adjusted for observed errors in GADM
    records."""
    new_pattern: str = id_pattern_string

    if provider == "gadm" and version == "4.1":
        if adm_level in (1, 2) and id_pattern_string.startswith("GHA."):
            new_pattern = new_pattern.replace("GHA.", "GHA")
        elif id_pattern_string.rstrip(r"\__") in GADM_41_IDS_MISSING_REVISION:
            new_pattern = new_pattern.rstrip(r"\__")

    return new_pattern
