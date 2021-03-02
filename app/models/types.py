from typing import Any, Callable, Coroutine, Dict, List, Tuple, Union
from uuid import UUID

from pydantic import StrictInt

from app.models.enum.pixetl import NonNumericFloat
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import Job

NoDataType = Union[StrictInt, NonNumericFloat]
SymbologyFuncType = Callable[
    [str, str, str, RasterTileSetSourceCreationOptions, int, int, Dict[Any, Any]],
    Coroutine[Any, Any, Tuple[List[Job], str]],
]
Pipeline = Callable[[str, str, UUID, Dict[str, Any]], Coroutine[Any, Any, ChangeLog]]
