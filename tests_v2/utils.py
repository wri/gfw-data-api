import uuid
from typing import Callable, Dict, Optional, Tuple


async def is_admin_mocked() -> bool:
    return True


async def is_service_account_mocked() -> bool:
    return True


async def get_user_id_mocked() -> str:
    return "userid_123"


async def get_api_key_mocked() -> Tuple[Optional[str], Optional[str]]:
    return str(uuid.uuid4()), "localhost"


def generate_uuid(*args, **kwargs) -> uuid.UUID:
    return uuid.uuid4()


def void_function(*args, **kwargs) -> None:
    return


def false_function(*args, **kwargs) -> bool:
    return False


def int_function_closure(value: int) -> Callable:
    def int_function(*args, **kwargs) -> int:
        return value

    return int_function


def dict_function_closure(value: Dict) -> Callable:
    def dict_function(*args, **kwargs) -> Dict:
        return value

    return dict_function
