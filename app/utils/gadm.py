def extract_level_gid(gid_level: int, gid_str: str):
    """Given a desired admin level and a string containing at least that level
    of id, return the id of just that level."""

    # Exception because of bad formatting of GHA gids in gadm_administrative_boundaries/v4.1
    if gid_str.startswith("GHA") and not gid_str.startswith("GHA."):
        gid_str = "GHA." + gid_str[3:]
    # Exception because bad ids IDN.35.4, IDN.35.8, IDN.35.9, IDN.35.13, IDN.35.14
    # (they are missing final '_1') in gadm_administrative_boundaries/v4.1
    if gid_str.startswith("IDN") and not gid_str.endswith("_1"):
        gid_str += "_1"

    return (gid_str.rsplit("_")[0]).split(".")[gid_level]
