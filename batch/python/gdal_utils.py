import os
import subprocess
from typing import Dict, List, Optional, Tuple

from errors import GDALError, SubprocessKilledError


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


def run_gdal_subcommand(cmd: List[str], env: Optional[Dict] = None) -> Tuple[str, str]:
    """Run GDAL as sub command and catch common errors."""

    gdal_env = os.environ.copy()
    if env:
        gdal_env.update(**env)

    print(f"RUN subcommand {cmd}, using env {gdal_env}")
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=gdal_env
    )

    o_byte, e_byte = p.communicate()

    # somehow return type when running `gdalbuildvrt` is str but otherwise bytes
    try:
        o = o_byte.decode("utf-8")
        e = e_byte.decode("utf-8")
    except AttributeError:
        o = str(o_byte)
        e = str(e_byte)

    if p.returncode != 0:
        print(f"Exit code {p.returncode} for command {cmd}")
        print(f"Standard output: {o}")
        print(f"Standard error: {e}")
        if p.returncode == -9:
            raise SubprocessKilledError()
        else:
            raise GDALError(e)

    return o, e
