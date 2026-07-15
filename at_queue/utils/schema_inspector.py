import inspect
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

import jsonref
from pydantic import ConfigDict
from pydantic import create_model
from pydantic.json_schema import model_json_schema


def function_to_json_schema(func: Callable, as_method=False):
    """
    Convert a function signature to JSON Schema using Pydantic v2.
    """
    signature = inspect.signature(func)
    parameters = list(signature.parameters.items())

    # Skip first parameter (self/cls) for methods
    if as_method and parameters:
        parameters = parameters[1:]

    # Create fields dictionary for Pydantic model
    fields = {}
    has_kwargs = False

    for param_name, param in parameters:
        # Handle *args and **kwargs
        if param.kind == param.VAR_POSITIONAL:
            # *args - we'll treat as array, but in JSON Schema this is complex
            continue
        elif param.kind == param.VAR_KEYWORD:
            # **kwargs - allow additional properties
            has_kwargs = True
            continue

        # Get annotation or use Any if not specified
        annotation = param.annotation if param.annotation != inspect.Parameter.empty else Any

        # Create field definition
        if param.default == inspect.Parameter.empty:
            # Required field
            fields[param_name] = (annotation, ...)
        else:
            # Optional field with default
            fields[param_name] = (annotation, param.default)

    # Create model config
    config_dict = {}
    if has_kwargs:
        config_dict["extra"] = "allow"
    else:
        config_dict["extra"] = "forbid"

    # Create dynamic Pydantic model
    model_name = f"{func.__name__}_Params"
    model = create_model(
        model_name, __config__=ConfigDict(**config_dict), **fields)

    # Generate JSON Schema
    schema = model_json_schema(model)

    # Extract the main schema part (remove definitions/title if needed)
    # Pydantic returns full compliant JSON Schema
    schema = jsonref.replace_refs(schema)
    return schema


def return_type_to_json_schema(func: Callable) -> Optional[Dict[str, Any]]:
    """
    Convert function return type annotation to JSON Schema using Pydantic v2.
    """
    signature = inspect.signature(func)
    return_annotation = signature.return_annotation

    if return_annotation == inspect.Signature.empty:
        return None

    # Create a simple model with the return type as its field
    if return_annotation is None or return_annotation is type(None):
        return_type = type(None)
    else:
        return_type = return_annotation

    # Create a model with single field having the return type
    model = create_model("ReturnModel", result=(return_type, ...))

    # Generate JSON Schema and extract the result field schema
    schema = model_json_schema(model)

    # Extract the main schema part (remove definitions/title if needed)
    # Pydantic returns full compliant JSON Schema
    schema = jsonref.replace_refs(schema)

    return schema["properties"]["result"]


def method_to_json_schema(method: Callable):
    """
    Convert a method signature to JSON Schema using Pydantic v2.
    """
    return function_to_json_schema(method, as_method=True)


def create_args_model(method_orig, auth_token=None):
    signature = inspect.signature(method_orig)
    fields = {}
    has_kwargs = False

    for param_name, param in signature.parameters.items():
        if param_name in ["self", "cls"]:
            continue

        # Обрабатываем *args
        if param.kind == param.VAR_POSITIONAL:
            # Для *args создаем поле с типом списка
            # annotation = list if param.annotation == inspect.Parameter.empty else param.annotation
            # fields[param_name] = (annotation, [])
            continue

        # Обрабатываем **kwargs
        if param.kind == param.VAR_KEYWORD:
            has_kwargs = True
            # Для **kwargs создаем поле с типом dict
            # annotation = dict if param.annotation == inspect.Parameter.empty else param.annotation
            # fields[param_name] = (annotation, {})
            continue

        # Обычные параметры
        annotation = param.annotation if param.annotation != inspect.Parameter.empty else Any
        default = param.default if param.default != inspect.Parameter.empty else ...
        fields[param_name] = (annotation, default)

    if auth_token is not None and "auth_token" not in fields:
        fields["auth_token"] = (str, ...)

    # Для **kwargs разрешаем дополнительные поля
    config = ConfigDict(extra="allow" if has_kwargs else "forbid")
    model = create_model(f"{method_orig.__name__}Args",
                         __config__=config, **fields)
    return model
