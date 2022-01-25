def from_vsi_path(file_name: str) -> str:
    """Convert GDAL /vsi path to s3:// or gs:// path.

    Taken from pixetl
    """

    protocols = {"vsis3": "s3", "vsigs": "gs"}

    parts = file_name.split("/")
    try:
        vsi = f"{protocols[parts[1]]}://{'/'.join(parts[2:])}"
    except KeyError:
        raise ValueError(f"Unknown protocol: {parts[1]}")
    return vsi
