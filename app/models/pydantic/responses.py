from typing import Any, Type, FrozenSet, Dict

from pydantic import BaseModel
from pydantic.utils import lenient_issubclass

from .base import StrictBaseModel


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


def partial(*fields):
    """ Make the object "partial": i.e. mark all fields as "skippable"

    In Pydantic terms, this means that they're not nullable, but not required either.

    Example:

        @partial
        class User(pd.BaseModel):
            id: int

        # `id` can be skipped, but cannot be `None`
        User()
        User(id=1)

    Example:

        @partial('id')
        class User(pd.BaseModel):
            id: int
            login: str

        # `id` can be skipped, but not `login`
        User(login='johnwick')
        User(login='johnwick', id=1)
    """
    # Call pattern: @partial class Model(pd.BaseModel):
    if len(fields) == 1 and lenient_issubclass(fields[0], BaseModel):
        the_model = fields[0]
        field_names = ()
    # Call pattern: @partial('field_name') class Model(pd.BaseModel):
    else:
        the_model = None
        field_names = fields

    # Decorator
    def decorator(model: Type[BaseModel] = the_model, field_names: FrozenSet[str] = frozenset(field_names)):
        # Iter fields, set `required=False`
        for field in model.__fields__.values():
            # All fields, or specific named fields
            if not field_names or field.name in field_names:
                field.required = False

        # Exclude unset
        # Otherwise non-nullable fields would have `{'field': None}` which is unacceptable
        dict_orig = model.dict

        def dict_excludes_unset(*args, exclude_unset: bool = None, **kwargs):
            exclude_unset = True
            return dict_orig(*args, **kwargs, exclude_unset=exclude_unset)
        model.dict = dict_excludes_unset

        # Done
        return model

    return decorator


@partial('links', 'meta')
class PaginatedResponse(Response):
    links: Dict
    meta: Dict
